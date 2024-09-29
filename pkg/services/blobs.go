package services

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

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

func (b *Blobs) Update(reader io.Reader) (string, error) {
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
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, hash), reader)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmplFile)
		return "", err
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmplFile)
		return "", err
	}

	sha := fmt.Sprintf("%x", hash.Sum(nil))
	hash.Reset()
	dest := filepath.Join(b.root, sha[:4], sha[4:])
	lockerAny, _ := b.locker.LoadOrStore(sha, &sync.RWMutex{})
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
		return sha, nil
	}
	if err = os.Rename(tmplFile, dest); err != nil {
		_ = os.RemoveAll(tmplFile)
		_ = os.RemoveAll(dest)
	}
	return sha, nil
}

type blobReader struct {
	*os.File
	readLock func()
}

func (b blobReader) Close() error {
	defer b.readLock()
	return b.File.Close()
}

func (b *Blobs) Get(key string) (io.ReadCloser, error) {
	lockerAny, _ := b.locker.LoadOrStore(key, &sync.RWMutex{})
	locker := lockerAny.(*sync.RWMutex)
	locker.RLock()
	open, err := os.Open(filepath.Join(b.root, key[:4], key[4:]))
	if err != nil {
		locker.RUnlock()
		return nil, err
	}
	return blobReader{
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
	return os.Remove(filepath.Join(b.root, key[:4], key[4:]))
}
