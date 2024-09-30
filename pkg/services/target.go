package services

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"code.d7z.net/d7z-project/cache-proxy/pkg/utils"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Target struct {
	name string
	urls []string  // 下载配置
	meta *FileMeta // 文件摘要

	blobs  *Blobs // 文件归档信息
	locker *utils.RWLockGroup

	rules []*TargetRule
	wait  *sync.WaitGroup
}

func NewTarget(name string, urls ...string) *Target {
	for i, url := range urls {
		urls[i] = strings.Trim(strings.TrimSpace(url), "/")
	}
	return &Target{
		name:   name,
		locker: utils.NewRWLockGroup(),
		urls:   urls,
		rules:  make([]*TargetRule, 0),
		wait:   new(sync.WaitGroup),
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

func (t *Target) forward(childPath string) (*utils.ResponseWrapper, error) {
	t.wait.Add(1)
	defer t.wait.Done()
	var cacheTime time.Duration = -1
	var refreshTime time.Duration = -1
	for _, rule := range t.rules {
		if rule.regex.MatchString(childPath) {
			cacheTime = rule.cache
			refreshTime = rule.refresh
			break
		}
	}
	if cacheTime == -1 {
		// direct
		remote, err := t.openRemote(childPath, true)
		if remote != nil {
			remote.Headers["Cache-Control"] = "no-cache"
			remote.Headers["X-Cache"] = "MISS"
		}
		return remote, err
	}
	pathLocker := t.locker.Open(childPath)
	lock := pathLocker.Lock(true)
	if !t.meta.Exists(childPath) {
		// 确保缓存仅仅被下载一次
		lock.AsLocker()
		if t.meta.Exists(childPath) {
			lock.Close()
			return t.openBlob(childPath)
		} else {
			return t.download(childPath, cacheTime, refreshTime, func() {
				lock.Close()
			})
		}
	} else {
		lock.Close()
		return t.openBlob(childPath)
	}
}

func (t *Target) Close() error {
	t.wait.Wait()
	return nil
}

func (t *Target) openBlob(path string) (*utils.ResponseWrapper, error) {
	all, err := t.meta.GetMeta(path)
	if err != nil {
		return nil, err
	}
	body, err := t.blobs.Get(all["blob"])
	if err != nil {
		return nil, err
	}
	return &utils.ResponseWrapper{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Last-Modified":  all["last-modified"],
			"X-Cache":        "HIT",
			"Content-Length": all["length"],
			"Content-Type":   all["content-type"],
		},
		Body: body,
	}, nil
}

func (t *Target) openRemote(path string, allowError bool) (*utils.ResponseWrapper, error) {
	var resp *utils.ResponseWrapper
	var err error
	for _, url := range t.urls {
		resp, err = utils.OpenRequest(fmt.Sprintf("%s/%s", url, path), allowError)
		if err != nil {
			resp = nil
			continue
		}
		break
	}
	return resp, err
}

func (t *Target) download(path string, cache, refresh time.Duration, finishHook func()) (*utils.ResponseWrapper, error) {
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
	resp, err := t.openRemote(path, false)
	if err != nil {
		return nil, errors.Wrapf(err, "文件下载失败")
	}
	lastModified, _ := resp.Headers["Last-Modified"]
	contentType, _ := resp.Headers["Content-Type"]
	length, _ := resp.Headers["Content-Length"]
	if lastModified == "" || length == "" {
		// 无法查询相关的缓存策略, 跳过
		return resp, nil
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	get, err := t.meta.Get(path, "last-modified")
	if err == nil && get == lastModified {
		// 自上次以来文件未更新
		return t.openBlob(path)
	}
	pipe1, writer1 := io.Pipe()
	pipe2, writer2 := io.Pipe()
	go func() {
		defer finishHook()
		defer writer1.Close()
		defer writer2.Close()
		defer resp.Body.Close()
		_, err := io.Copy(io.MultiWriter(writer1, writer2), resp.Body)
		if err != nil {
			_ = writer1.CloseWithError(err)
			_ = writer2.CloseWithError(err)
		}
		zap.L().Debug("结束内容缓存", zap.String("path", path))
	}()
	go func() {
		defer pipe1.Close()
		token, err := t.blobs.Update(fmt.Sprintf("%s@%s", t.name, path), pipe1)
		_ = resp.Body.Close()
		if err != nil {
			zap.L().Debug("保存内容错误", zap.String("path", path), zap.Error(err))
			return
		}
		if err = t.meta.Put(path, map[string]string{
			"blob":          token,
			"last-modified": lastModified,
			"length":        length,
			"content-type":  contentType,
		}, true); err != nil {
			_ = resp.Body.Close()
			zap.L().Debug("推送配置错误", zap.String("path", path), zap.Error(err))
			return
		}
	}()
	return &utils.ResponseWrapper{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Last-Modified":  lastModified,
			"X-Cache":        "WAIT",
			"Content-Length": length,
			"Content-Type":   contentType,
		},
		Body: pipe2,
	}, nil
}

func (t *Target) Gc() error {
	zap.L().Debug("执行 Meta GC", zap.String("name", t.name))
	result, err := t.meta.Gc(func(s string) time.Duration {
		for _, rule := range t.rules {
			if rule.regex.MatchString(s) {
				return rule.cache
			}
		}
		return -1
	})
	if err != nil {
		return err
	}
	var errs []error
	for path, m := range result {
		zap.L().Debug("删除文件", zap.String("path", path), zap.String("blob", m["blob"]))
		if err = t.blobs.DelPointer(m["blob"], fmt.Sprintf("%s@%s", t.name, path)); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Errorf("%v", err)
	}
	return nil
}

type TargetRule struct {
	regex   *regexp.Regexp // 正则匹配
	cache   time.Duration  // 缓存时间
	refresh time.Duration  // 刷新时间，缓存未过期时可用 (需要支持 head 请求查询状态)
}
