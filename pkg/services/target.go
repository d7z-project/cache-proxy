package services

import (
	"fmt"
	"io"
	"log"
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

	rules    []*TargetRule
	replaces []*TargetReplace

	wait *sync.WaitGroup

	httpClient *utils.HttpClientWrapper
}

func NewTarget(name string, urls ...string) *Target {
	for i, url := range urls {
		urls[i] = strings.Trim(strings.TrimSpace(url), "/")
	}

	return &Target{
		name:       name,
		locker:     utils.NewRWLockGroup(),
		urls:       urls,
		rules:      make([]*TargetRule, 0),
		replaces:   make([]*TargetReplace, 0),
		wait:       new(sync.WaitGroup),
		httpClient: utils.DefaultHttpClientWrapper(),
	}
}

func (t *Target) AddRule(regex string, cache, refresh time.Duration) error {
	compile, err := regexp.Compile(regex)
	if err != nil {
		return err
	}
	if cache <= refresh && cache > 0 {
		log.Printf("注意，%s 中缓存时间小于刷新时间，刷新时间可能无效", t.name)
		return errors.New("invalid cache duration, cache less than refresh.")
	}
	t.rules = append(t.rules, &TargetRule{
		regex:   compile,
		cache:   cache,
		refresh: refresh,
	})
	return nil
}

func (t *Target) AddReplace(regex, old, new string) error {
	compile, err := regexp.Compile(regex)
	if err != nil {
		return err
	}
	if len(old) == 0 {
		return errors.New("invalid old str , len() == 0.")
	}
	t.replaces = append(t.replaces, &TargetReplace{
		regex: compile,
		src:   []byte(old),
		dest:  []byte(new),
	})
	return nil
}

func (t *Target) SetHttpClient(client *utils.HttpClientWrapper) {
	t.httpClient = client
}

func (t *Target) forward(childPath string) (*utils.ResponseWrapper, error) {
	res, err := t.fetchResource(childPath)
	if err != nil {
		return nil, err
	}
	// post: replace
	for _, replace := range t.replaces {
		if replace.regex.MatchString(childPath) {
			zap.L().Debug("后处理替换内容", zap.String("childPath", childPath))
			delete(res.Headers, "Content-Length")
			res.Body = utils.NewKMPReplaceReader(res.Body, replace.src, replace.dest)
		}
	}
	return res, nil
}

func (t *Target) fetchResource(childPath string) (*utils.ResponseWrapper, error) {
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
	download := true
	lastUpdate, err := t.meta.GetLastUpdate(childPath)
	now := time.Now()
	if err == nil && (cacheTime == 0 || lastUpdate.Add(cacheTime).After(now)) {
		// 无 ttl ，跳过
		download = false
	}
	if download == false && err == nil && (refreshTime == 0 || lastUpdate.Add(refreshTime).After(now)) {
		// 当前缓存正常，跳过刷新
		download = false
	} else {
		zap.L().Debug("文件需要刷新", zap.String("child", childPath))
		download = true
	}
	if download || err != nil {
		lock.AsLocker()
		return t.download(childPath, func() {
			lock.Close()
		})
	} else {
		return t.openBlob(childPath, lock.Close)
	}
}

func (t *Target) Close() error {
	t.wait.Wait()
	return nil
}

func (t *Target) openBlob(path string, closeHook func()) (*utils.ResponseWrapper, error) {
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
		Body:   body,
		Closes: closeHook,
	}, nil
}

func (t *Target) openRemote(path string, errorAccept bool) (*utils.ResponseWrapper, error) {
	var resp *utils.ResponseWrapper
	var err error
	for _, url := range t.urls {
		resp, err = utils.OpenRequest(t.httpClient, fmt.Sprintf("%s/%s", url, path), errorAccept)
		if err != nil {
			resp = nil
			zap.L().Debug("请求失败", zap.String("url", url), zap.Error(err))
			continue
		}
		break
	}
	return resp, err
}

func (t *Target) download(path string, finishHook func()) (*utils.ResponseWrapper, error) {
	resp, respErr := t.openRemote(path, false)
	if respErr != nil {
		if t.meta.Exists(path) {
			// 回退到本地缓存
			return t.openBlob(path, finishHook)
		} else {
			finishHook()
			return nil, errors.Wrapf(respErr, "文件下载失败")
		}
	}
	lastModified, _ := resp.Headers["Last-Modified"]
	contentType, _ := resp.Headers["Content-Type"]
	length, _ := resp.Headers["Content-Length"]
	if lastModified == "" || length == "" {
		// 无法查询相关的缓存策略, 跳过
		finishHook()
		return resp, nil
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	get, err := t.meta.Get(path, "last-modified")
	if err == nil && get == lastModified {
		// 自上次以来文件未更新
		_ = resp.Close()
		_ = t.meta.Refresh(path)
		return t.openBlob(path, finishHook)
	}
	pointer := fmt.Sprintf("%s@%s", t.name, path)
	if lastBlobId, err := t.meta.Get(path, "blob"); err == nil {
		// 移除旧文件指针
		if err = t.blobs.DelPointer(lastBlobId, pointer); err != nil {
			return nil, err
		}
	}
	pipeBlob, writerBlob := io.Pipe()
	pipeSync, writerSync := io.Pipe()
	go func() {
		defer finishHook()
		defer writerBlob.Close()
		defer writerSync.Close()
		defer resp.Body.Close()
		_, err := io.Copy(io.MultiWriter(writerBlob, writerSync), resp.Body)
		if err != nil {
			_ = writerBlob.CloseWithError(err)
			_ = writerSync.CloseWithError(err)
			zap.L().Debug("内容缓存失败", zap.String("path", path), zap.Error(err))
			return
		}
		zap.L().Debug("结束内容缓存", zap.String("path", path))
	}()
	go func() {
		defer pipeBlob.Close()
		token, err := t.blobs.Update(pointer, pipeBlob)
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
		Body: pipeSync,
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
		zap.L().Debug("为 blob 删除索引", zap.String("path", path), zap.String("blob", m["blob"]))
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

type TargetReplace struct {
	regex *regexp.Regexp
	src   []byte
	dest  []byte
}
