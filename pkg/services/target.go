package services

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/icholy/replace"
	"golang.org/x/text/transform"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/utils"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Target struct {
	name   string
	urls   []string // 下载配置
	locker *utils.RWLockGroup

	storage blobfs.Objects

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

func (t *Target) forward(ctx context.Context, childPath string, headers map[string]string) (*utils.ResponseWrapper, error) {
	t.wait.Add(1)
	defer t.wait.Done()
	res, err := t.fetchResource(ctx, childPath, headers)
	if err != nil {
		return nil, err
	}
	tss := make([]transform.Transformer, 0)
	// post: replace
	for _, replaceRule := range t.replaces {
		if replaceRule.regex.MatchString(childPath) {
			zap.L().Debug("后处理替换内容", zap.String("childPath", childPath))
			tss = append(tss, replace.Bytes(replaceRule.src, replaceRule.dest))
		}
	}
	if len(tss) != 0 {
		delete(res.Headers, "Content-Length")
		res.Body = utils.NewReadCloserWrapper(replace.Chain(res.Body, tss...), res.Body)
	}
	return res, nil
}

func (t *Target) fetchResource(ctx context.Context, childPath string, headers map[string]string) (*utils.ResponseWrapper, error) {
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
		remote, err := t.openRemote(ctx, childPath, true, headers)
		if remote != nil {
			remote.Headers["Cache-Control"] = "no-cache"
			remote.Headers["X-Cache"] = "MISS"
		}
		return remote, err
	}

	mu := t.locker.Get(childPath)
	mu.RLock()

	blob, err := t.storage.Pull(childPath)
	now := time.Now()

	download := true
	if err == nil && (cacheTime == 0 || blob.CreateAt.Add(cacheTime).After(now)) {
		// No ttl, skip
		download = false
	}
	if !download && (refreshTime == 0 || blob.CreateAt.Add(refreshTime).After(now)) {
		// Valid cache, skip refresh
		return t.openObject(blob, mu.RUnlock)
	}

	// Needs download or refresh
	if blob != nil {
		_ = blob.Close()
	}
	mu.RUnlock()

	// Direct proxy for Range request if not cached
	if _, ok := headers["Range"]; ok {
		zap.L().Debug("Range 请求且无缓存，直接转发", zap.String("child", childPath))
		return t.openRemote(ctx, childPath, true, headers)
	}

	// Upgrade lock
	mu.Lock()

	// Double check
	blob, err = t.storage.Pull(childPath)
	download = true
	if err == nil && (cacheTime == 0 || blob.CreateAt.Add(cacheTime).After(now)) {
		download = false
	}
	if !download && (refreshTime == 0 || blob.CreateAt.Add(refreshTime).After(now)) {
		// Valid cache found after acquiring write lock
		mu.Unlock()
		mu.RLock()
		return t.openObject(blob, mu.RUnlock)
	}

	if blob != nil {
		_ = blob.Close()
	}

	zap.L().Debug("文件需要刷新", zap.String("child", childPath))
	return t.download(ctx, childPath, mu.Unlock)
}

func (t *Target) Close() error {
	t.wait.Wait()
	return nil
}

func (t *Target) openObject(path *blobfs.PullContent, closeHook func()) (*utils.ResponseWrapper, error) {
	return &utils.ResponseWrapper{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Last-Modified":  path.Options["last-modified"],
			"X-Cache":        "HIT",
			"X-Cache-Fetch":  path.CreateAt.Format(http.TimeFormat),
			"Content-Length": path.Options["length"],
			"Content-Type":   path.Options["content-type"],
		},
		Body:   path,
		Closes: closeHook,
	}, nil
}

func (t *Target) openBlob(path string, closeHook func()) (*utils.ResponseWrapper, error) {
	all, err := t.storage.Pull(path)
	if err != nil {
		closeHook()
		return nil, err
	}
	return t.openObject(all, closeHook)
}

func (t *Target) openRemote(ctx context.Context, path string, errorAccept bool, headers map[string]string) (*utils.ResponseWrapper, error) {
	var resp *utils.ResponseWrapper
	var err error
	for _, url := range t.urls {
		resp, err = t.httpClient.OpenRequestWithContext(ctx, fmt.Sprintf("%s/%s", url, path), errorAccept, headers)
		if err != nil {
			resp = nil
			zap.L().Debug("请求失败", zap.String("url", url), zap.Error(err))
			continue
		}
		break
	}
	return resp, err
}

func (t *Target) download(ctx context.Context, path string, finishHook func()) (*utils.ResponseWrapper, error) {
	resp, respErr := t.openRemote(ctx, path, false, nil)
	if respErr != nil || resp == nil {
		if resp != nil {
			_ = resp.Close()
		}
		return t.openBlob(path, finishHook)
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
	pull, err := t.storage.Pull(path)
	if err == nil && pull.Options["last-modified"] == lastModified {
		// 自上次以来文件未更新
		_ = resp.Close()
		_ = pull.Close()
		_ = t.storage.Cleanup(path)
		return t.openBlob(path, finishHook)
	} else if err == nil {
		_ = pull.Close()
	}

	return &utils.ResponseWrapper{
		Closes:     finishHook,
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Last-Modified":  lastModified,
			"X-Cache":        "WAIT",
			"Content-Length": length,
			"Content-Type":   contentType,
		},
		Body: t.storage.Transparent(path, resp.Body, map[string]string{
			"last-modified": lastModified,
			"length":        length,
			"content-type":  contentType,
		}),
	}, nil
}

func (t *Target) Gc() error {
	zap.L().Debug("执行 Meta GC", zap.String("name", t.name))
	for _, rule := range t.rules {
		if err := t.storage.Remove("", rule.regex, rule.cache); err != nil {
			return err
		}
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
