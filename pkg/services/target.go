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

	"code.d7z.net/d7z-team/blobfs"

	"code.d7z.net/d7z-project/cache-proxy/pkg/utils"

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

func (t *Target) forward(ctx context.Context, childPath string) (*utils.ResponseWrapper, error) {
	res, err := t.fetchResource(ctx, childPath)
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

func (t *Target) fetchResource(ctx context.Context, childPath string) (*utils.ResponseWrapper, error) {
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
		remote, err := t.openRemote(ctx, childPath, true)
		if remote != nil {
			remote.Headers["Cache-Control"] = "no-cache"
			remote.Headers["X-Cache"] = "MISS"
		}
		return remote, err
	}
	pathLocker := t.locker.Open(childPath)
	lock := pathLocker.Lock(true)
	download := true
	blob, err := t.storage.Pull(childPath)
	now := time.Now()
	if err == nil && (cacheTime == 0 || blob.CreateAt.Add(cacheTime).After(now)) {
		// 无 ttl ，跳过
		download = false
	}
	if download == false && (refreshTime == 0 || blob.CreateAt.Add(refreshTime).After(now)) {
		// 当前缓存正常，跳过刷新
		download = false
	} else {
		zap.L().Debug("文件需要刷新", zap.String("child", childPath))
		download = true
		if blob != nil {
			_ = blob.Close()
		}
	}
	if download {
		lock.AsLocker()
		return t.download(ctx, childPath, func() {
			lock.Close()
		})
	} else {
		return t.openObject(blob, lock.Close)
	}
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

func (t *Target) openRemote(ctx context.Context, path string, errorAccept bool) (*utils.ResponseWrapper, error) {
	var resp *utils.ResponseWrapper
	var err error
	for _, url := range t.urls {
		resp, err = t.httpClient.OpenRequestWithContext(ctx, fmt.Sprintf("%s/%s", url, path), errorAccept)
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
	resp, respErr := t.openRemote(ctx, path, false)
	if respErr != nil {
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
		_ = pull.Close()
		_ = t.storage.Refresh(path)
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
