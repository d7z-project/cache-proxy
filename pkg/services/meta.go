package services

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileMeta struct {
	localDir string
	cache    sync.Map
	gcLocker sync.RWMutex
}

func NewFileMeta(root string) (*FileMeta, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &FileMeta{
		localDir: root,
		cache:    sync.Map{},
		gcLocker: sync.RWMutex{},
	}, nil
}

// Gc 清理数据, 将更新时间超过参数的内容进行清理并返回 meta
func (m *FileMeta) Gc(ttl time.Duration) (map[string]map[string]string, error) {
	gcBegin := time.Now()
	// TODO: 优化锁结构，降低重锁导致的停机时间
	m.gcLocker.Lock()
	defer m.gcLocker.Unlock()
	dir, err := os.ReadDir(m.localDir)
	if err != nil {
		return nil, err
	}
	result := make(map[string]map[string]string)
	for _, entry := range dir {
		info, _ := entry.Info()
		modTime := info.ModTime()
		item := map[string]string{}
		if value, ok := m.cache.Load(entry.Name()); ok {
			modTime = value.(*metaCache).update
			item = value.(*metaCache).data
		}
		if gcBegin.Sub(modTime) <= ttl {
			continue
		}
		localPath := filepath.Join(m.localDir, entry.Name())
		data, err := os.ReadFile(localPath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &item); err != nil {
			return nil, err
		}
		decodeString, err := base64.URLEncoding.DecodeString(entry.Name())
		if err != nil {
			return nil, err
		}
		result[string(decodeString)] = item
		if err = os.Remove(localPath); err != nil {
			return nil, err
		}
		m.cache.Delete(entry.Name())
	}
	return result, nil
}

type metaCache struct {
	locker sync.RWMutex
	data   map[string]string
	update time.Time
}

func newMetaCache() *metaCache {
	return &metaCache{
		data:   map[string]string{},
		locker: sync.RWMutex{},
		update: time.Now(),
	}
}

func (m *FileMeta) getContent(filePath string, create bool) (*metaCache, error) {
	newMeta := newMetaCache()
	newMeta.locker.Lock()
	defer newMeta.locker.Unlock()
	actual, find := m.cache.LoadOrStore(filePath, newMeta)
	cache := actual.(*metaCache)
	localPath := filepath.Join(m.localDir, filePath)
	if !find {
		data, err := os.ReadFile(localPath)
		if !create && os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		if err = os.MkdirAll(filepath.Dir(filePath), 0700); err != nil && !os.IsExist(err) {
			return nil, err
		}
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err != nil {
			file, err := os.Create(filePath)
			if err != nil {
				return nil, err
			}
			if _, err = file.Write([]byte("{}")); err != nil {
				_ = file.Close()
				_ = os.Remove(filePath)
				return nil, err
			}
			_ = file.Close()
		} else {
			if err = json.Unmarshal(data, &cache.data); err != nil {
				return nil, err
			}
		}
		if stat, err := os.Stat(localPath); err != nil && !os.IsNotExist(err) {
			return nil, err
		} else if err != nil {
			cache.update = time.Now()
		} else {
			cache.update = stat.ModTime()
		}
	}
	return cache, nil
}

func (m *FileMeta) Get(filePath string, key string) (string, error) {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()
	content, err := m.getContent(filePath, false)
	if err != nil {
		return "", err
	}
	content.locker.RLock()
	defer content.locker.RUnlock()
	return content.data[key], nil
}

func (m *FileMeta) Put(filePath string, key string, value string, safe bool) error {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()
	content, err := m.getContent(filePath, true)
	if err != nil {
		return err
	}
	content.locker.Lock()
	defer content.locker.Unlock()
	content.data[key] = value
	content.update = time.Now()
	if safe {
		data, err := json.Marshal(&content.data)
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(m.localDir, filePath), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}
