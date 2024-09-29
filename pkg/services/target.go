package services

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Target struct {
	urls  []string  // 下载配置
	meta  *FileMeta // 文件摘要
	blobs *Blobs    // 文件归档信息

	rules []*TargetRule

	pathLocker sync.Map
}

func NewTarget(urls ...string) *Target {
	return &Target{
		urls: urls,
	}
}

func (t *Target) AddRule(regex string, cache, refresh time.Duration) error {
	compile, err := regexp.Compile(regex)
	if err != nil {
		return err
	}
	if cache <= refresh {
		return errors.New("invalid cache duration, cache less than refresh.")
	}
	t.rules = append(t.rules, &TargetRule{
		regex:   compile,
		cache:   cache,
		refresh: refresh,
	})
	return nil
}
func (t *Target) forward(resp http.ResponseWriter, req *http.Request, prefix string) {
	path := "/" + strings.TrimPrefix(req.URL.Path, prefix)
	var cacheTime time.Duration = -1
	var refreshTime time.Duration = -1
	for _, rule := range t.rules {
		if rule.regex.MatchString(path) {
			cacheTime = rule.cache
			refreshTime = rule.refresh
			break
		}
	}
	if cacheTime == -1 {
		// direct

	}
	value, _ := t.pathLocker.LoadOrStore(path, &sync.RWMutex{})
	locker := value.(*sync.RWMutex)
	locker.RLock()
	unlocker := locker.RUnlock
	_, err := t.meta.GetLastUpdate(path)
	var target *downReturn
	if errors.Is(err, os.ErrNotExist) {
		unlocker()
		locker.Lock()
		unlocker = locker.Unlock
		// 确保缓存仅仅被下载一次
		_, err = t.meta.GetAll(path)
		if err == nil {
			target, err = t.openBlob(path)
			if err != nil {
				unlocker()
				resp.WriteHeader(http.StatusNotFound)
				log.Println(err)
			}
		} else {
			target, err = t.download(t.urls, path, cacheTime, refreshTime)
			if err != nil {
				unlocker()
				resp.WriteHeader(http.StatusNotFound)
				log.Println(err)
			}
		}
	} else {
		target, err = t.openBlob(path)
		if err != nil {
			unlocker()
			resp.WriteHeader(http.StatusNotFound)
			log.Println(err)
		}
	}
	defer unlocker()
	for s, s2 := range target.headers {
		resp.Header().Add(s, s2)
	}
	resp.WriteHeader(200)
	_, _ = io.Copy(resp, req.Body)
}

func (t *Target) Close() error {
	return nil
}

type downReturn struct {
	headers map[string]string
	body    io.ReadCloser
}

func (t *Target) openBlob(path string) (*downReturn, error) {
	all, err := t.meta.GetAll(path)
	if err != nil {
		return nil, err
	}
	body, err := t.blobs.Get(all["blob"])
	if err != nil {
		return nil, err
	}
	return &downReturn{
		headers: map[string]string{
			"Content-Type":   "application/octet-stream",
			"Last-Modified":  all["last-modified"],
			"X-CACHE":        "HIT",
			"Content-Length": all["length"],
		},
		body: body,
	}, nil
}

func (t *Target) download(urls []string, path string, cache, refresh time.Duration) (*downReturn, error) {

	update, err := t.meta.GetLastUpdate(path)
	now := time.Now()
	if err == nil && cache == 0 {
		// 永久缓存
		return t.openBlob(path)
	}
	if err == nil && update.Add(cache).After(now) && update.Add(refresh).After(now) && cache != -1 {
		// 当前缓存正常，跳过刷新
		return t.openBlob(path)
	}
	for _, url := range urls {
		resp, err := http.Get(fmt.Sprintf("%s%s", url, path))
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
		}
		lastModified := resp.Header.Get("Last-Modified")
		length := resp.Header.Get("Content-Length")
		if lastModified == "" || length == "" {
			// 无法查询相关的缓存策略, 跳过
			return &downReturn{
				headers: map[string]string{
					"Content-Type": "application/octet-stream",
				},
				body: resp.Body,
			}, nil
		}
		get, err := t.meta.Get(path, "last-modified")
		if err == nil && get == lastModified {
			// 自上次以来文件未更新
			return t.openBlob(path)
		}
		token, err := t.blobs.Update(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if err = t.meta.Put(path, map[string]string{
			"blob":          token,
			"url":           fmt.Sprintf("%s%s", url, path),
			"last-modified": lastModified,
			"length":        length,
		}, true); err != nil {
			return nil, err
		}
		break
	}
	return nil, errors.Errorf("文件下载失败")
}

type TargetRule struct {
	regex   *regexp.Regexp // 正则匹配
	cache   time.Duration  // 缓存时间
	refresh time.Duration  // 刷新时间，缓存未过期时可用 (需要支持 head 请求查询状态)
}
