package services

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type Blobs struct {
	root       string
	locker     sync.Map
	tempLocker sync.Map
	temp       string
}

func NewBlobs(workdir string) (*Blobs, error) {
	workdir, err := filepath.Abs(workdir)
	if err != nil {
		return nil, err
	}
	returns := &Blobs{
		root:       workdir,
		locker:     sync.Map{},
		tempLocker: sync.Map{},
		temp:       filepath.Join(workdir, "cache"),
	}
	if err := os.MkdirAll(returns.root, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(returns.temp, 0o755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	return returns, nil
}

func (b *Blobs) Update(pointer string, reader io.Reader) (string, error) {
	if reader == nil {
		return "", fmt.Errorf("reader is nil")
	}
	key := uuid.New().String()
	for {
		if _, loaded := b.locker.LoadOrStore(key, fmt.Sprintf("%s-%s", key, time.Now())); !loaded {
			break
		}
		key = uuid.New().String()
	}
	defer b.tempLocker.Delete(key)
	tmplFile := filepath.Join(b.temp, key)
	f, err := os.Create(tmplFile)
	if err != nil {
		return "", err
	}
	md5Hash := md5.New()
	sha1Hash := sha1.New()
	sha256Hash := sha256.New()
	sha512Hash := sha512.New()
	_, err = io.Copy(io.MultiWriter(f, md5Hash, sha1Hash, sha256Hash, sha512Hash), reader)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmplFile)
		return "", err
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmplFile)
		return "", err
	}
	md5sum := fmt.Sprintf("%x", md5Hash.Sum(nil))
	md5Hash.Reset()
	sha1sum := fmt.Sprintf("%x", sha1Hash.Sum(nil))
	sha1Hash.Reset()
	sha256sum := fmt.Sprintf("%x", sha256Hash.Sum(nil))
	sha256Hash.Reset()
	sha512sum := fmt.Sprintf("%x", sha512Hash.Sum(nil))
	sha512Hash.Reset()
	dest := filepath.Join(b.root, sha256sum[:4], sha256sum[4:])
	lockerAny, _ := b.locker.LoadOrStore(sha256sum, &sync.RWMutex{})
	locker := lockerAny.(*sync.RWMutex)
	locker.Lock()
	defer locker.Unlock()
	if err = os.MkdirAll(filepath.Dir(dest), 0o755); err != nil && !os.IsExist(err) {
		return "", err
	}

	if stat, err := os.Stat(dest); err != nil && !os.IsNotExist(err) {
		return "", errors.Errorf("路径 %s 解析错误 %v", dest, err)
	} else if stat != nil && stat.IsDir() {
		return "", errors.Errorf("路径 %s 不应该是目录", dest)
	} else if stat != nil && !stat.IsDir() {
		_ = os.RemoveAll(tmplFile)
		if err = b.updateMeta(sha256sum, func(meta *blobMeta) error {
			if pointer != "" {
				meta.Linker[pointer] = time.Now()
			}
			return nil
		}); err != nil {
			return "", err
		}
		return sha256sum, nil
	}
	if err = os.Rename(tmplFile, dest); err != nil && !os.IsExist(err) {
		_ = os.RemoveAll(tmplFile)
		_ = os.RemoveAll(dest)
		return "", err
	} else if os.IsExist(err) {
		_ = os.RemoveAll(tmplFile)
	}
	if err = b.updateMeta(sha256sum, func(meta *blobMeta) error {
		meta.MD5 = md5sum
		meta.SHA1 = sha1sum
		meta.SHA256 = sha256sum
		meta.SHA512 = sha512sum
		if pointer != "" {
			meta.Linker[pointer] = time.Now()
		}
		return nil
	}); err != nil {
		return "", err
	}
	return sha256sum, nil
}

func (b *Blobs) AddPointer(token, point string) error {
	lockerAny, _ := b.locker.LoadOrStore(token, &sync.RWMutex{})
	locker := lockerAny.(*sync.RWMutex)
	locker.Lock()
	defer locker.Unlock()
	_, err := os.Stat(filepath.Join(b.root, token[:4], token[4:]))
	if err != nil {
		return err
	}
	return b.updateMeta(token, func(meta *blobMeta) error {
		meta.Linker[point] = time.Now()
		return nil
	})
}

func (b *Blobs) DelPointer(token, point string) error {
	lockerAny, _ := b.locker.LoadOrStore(token, &sync.RWMutex{})
	locker := lockerAny.(*sync.RWMutex)
	locker.Lock()
	defer locker.Unlock()
	_, err := os.Stat(filepath.Join(b.root, token[:4], token[4:]))
	if err != nil {
		return err
	}
	return b.updateMeta(token, func(meta *blobMeta) error {
		delete(meta.Linker, point)
		return nil
	})
}

func (b *Blobs) updateMeta(token string, metaFunc func(meta *blobMeta) error) error {
	metaFile := filepath.Join(b.root, token[:4], token[4:]) + ".meta.json"
	meta, err := parseFileMeta(metaFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := metaFunc(meta); err != nil {
		return err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(metaFile, data, 0o600)
}

type blobMeta struct {
	Linker map[string]time.Time `json:"points"`

	MD5    string `json:"md5,omitempty"`
	SHA1   string `json:"sha1,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	SHA512 string `json:"sha512,omitempty"`
}

type BlobReader struct {
	*os.File
	readLock func()
}

func (b BlobReader) Close() error {
	defer b.readLock()
	return b.File.Close()
}

func (b *Blobs) Get(key string) (io.ReadSeekCloser, error) {
	lockerAny, _ := b.locker.LoadOrStore(key, &sync.RWMutex{})
	locker := lockerAny.(*sync.RWMutex)
	locker.RLock()
	open, err := os.Open(filepath.Join(b.root, key[:4], key[4:]))
	if err != nil {
		locker.RUnlock()
		return nil, err
	}
	return BlobReader{
		File: open,
		readLock: func() {
			locker.RUnlock()
		},
	}, err
}

func (b *Blobs) Delete(key string) error {
	lockerAny, _ := b.locker.LoadOrStore(key, &sync.RWMutex{})
	locker := lockerAny.(*sync.RWMutex)
	locker.Lock()
	defer locker.Unlock()
	filePath := filepath.Join(b.root, key[:4], key[4:])
	meta, err := parseFileMeta(filePath + ".meta.json")
	if err != nil {
		return err
	}
	if len(meta.Linker) > 0 {
		return os.ErrExist
	}
	err = os.Remove(filePath)
	if err != nil {
		return err
	}
	return os.Remove(filePath + ".meta.json")
}

func parseFileMeta(path string) (*blobMeta, error) {
	meta := &blobMeta{
		Linker: make(map[string]time.Time),
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return meta, err
	}

	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

func (b *Blobs) Gc() error {
	zap.L().Debug("执行 blob gc")
	emptyKey := make([]string, 0)
	if err := filepath.Walk(b.root, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".meta.json") && info.Mode().IsRegular() {
			meta, err := parseFileMeta(path)
			if err != nil {
				return err
			}
			if len(meta.Linker) == 0 {
				emptyKey = append(emptyKey, meta.SHA256)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, s := range emptyKey {
		zap.L().Debug("删除文件块", zap.String("path", s))
		if err := b.Delete(s); err != nil && os.IsExist(err) {
			zap.L().Debug("删除时文件新增索引", zap.String("path", s))
		} else if err != nil {
			return errors.Wrapf(err, "删除 %s 出现问题", emptyKey)
		}
	}
	return nil
}
