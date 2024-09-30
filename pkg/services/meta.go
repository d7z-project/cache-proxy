package services

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
func (m *FileMeta) Gc(filter func(string) time.Duration) (map[string]map[string]string, error) {
	gcBegin := time.Now()
	files := make(map[string]time.Duration)
	_ = filepath.Walk(m.localDir, func(path string, info os.FileInfo, err error) error {
		subFile := strings.Trim(strings.TrimPrefix(path, m.localDir), string(filepath.Separator))
		duration := filter(subFile)
		if duration >= 0 && info.Mode().Type().IsRegular() && gcBegin.Sub(info.ModTime()) > duration {
			files[subFile] = duration
		}
		return nil
	})
	m.gcLocker.Lock()
	defer m.gcLocker.Unlock()
	result := make(map[string]map[string]string)
	for file, ttl := range files {
		if err := func() error {
			content, err := m.getContent(file, false)
			if err != nil {
				return err
			}
			content.locker.Lock()
			defer content.locker.Unlock()
			if gcBegin.Sub(content.update) <= ttl {
				return nil
			}
			// 任务结束缓存已经被移除，此时 map 已线程安全
			result[file] = content.data
			if err = os.Remove(filepath.Join(m.localDir, file)); err != nil {
				return err
			}
			m.cache.Delete(file)
			return nil
		}(); err == nil {
			return result, err
		}
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

func (m *FileMeta) getContent(pathKey string, create bool) (*metaCache, error) {
	newMeta := newMetaCache()
	newMeta.locker.Lock()
	defer newMeta.locker.Unlock()
	actual, find := m.cache.LoadOrStore(pathKey, newMeta)
	cache := actual.(*metaCache)
	localPath := filepath.Join(m.localDir, pathKey)
	if !find {
		data, err := os.ReadFile(localPath)
		if !create && os.IsNotExist(err) {
			m.cache.Delete(pathKey)
			return nil, os.ErrNotExist
		} else if err != nil && !os.IsNotExist(err) {
			return nil, err
		} else if err != nil {
			if err := os.MkdirAll(filepath.Dir(localPath), 0o700); err != nil && !os.IsExist(err) {
				return nil, err
			}
			file, err := os.Create(localPath)
			if err != nil {
				return nil, err
			}
			data = []byte(`{"_meta":"1.0"}`)
			if _, err = file.Write(data); err != nil {
				_ = file.Close()
				_ = os.Remove(localPath)
				return nil, err
			}
			_ = file.Close()
		}
		if err = json.Unmarshal(data, &cache.data); err != nil {
			return nil, err
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

func (m *FileMeta) GetLastUpdate(pathKey string) (*time.Time, error) {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()
	content, err := m.getContent(pathKey, false)
	if err != nil {
		return nil, err
	}
	return &content.update, nil
}

func (m *FileMeta) Exists(pathKey string) bool {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()

	_, err := m.getContent(pathKey, false)
	return err == nil
}

func (m *FileMeta) GetMeta(pathKey string) (map[string]string, error) {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()

	content, err := m.getContent(pathKey, false)
	if err != nil {
		return nil, err
	}
	content.locker.RLock()
	defer content.locker.RUnlock()
	result := make(map[string]string)
	for s, s2 := range content.data {
		result[s] = s2
	}
	return result, nil
}

func (m *FileMeta) Get(pathKey string, key string) (string, error) {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()
	content, err := m.getContent(pathKey, false)
	if err != nil {
		return "", err
	}
	content.locker.RLock()
	defer content.locker.RUnlock()
	return content.data[key], nil
}

func (m *FileMeta) Put(pathKey string, data map[string]string, safe bool) error {
	m.gcLocker.RLock()
	defer m.gcLocker.RUnlock()
	content, err := m.getContent(pathKey, true)
	if err != nil {
		return err
	}
	content.locker.Lock()
	defer content.locker.Unlock()
	for key, value := range data {
		content.data[key] = value
	}
	content.update = time.Now()
	if safe {
		data, err := json.Marshal(&content.data)
		if err != nil {
			return err
		}
		if _, find := m.cache.Load(pathKey); find {
			// 可能内容已被移除
			if err = os.WriteFile(filepath.Join(m.localDir, pathKey), data, 0o600); err != nil {
				return err
			}
		}
	}
	return nil
}
