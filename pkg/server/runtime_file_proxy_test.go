package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestFileProxyCachesImmutableAndRevalidatesMutableObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var version int32 = 1
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.LoadInt32(&version)
		body := []byte(fmt.Sprintf("file-%d", current))
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		w.Header().Set("Last-Modified", time.Unix(int64(1000+current), 0).UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			atomic.AddInt32(&getCount, 1)
			_, _ = w.Write(body)
		}
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass, Rules: []config.CacheRule{{Match: "**/*.bin", Policy: config.PolicyImmutable}, {Match: "**/*.json", Policy: config.PolicyRevalidate}}},
	}})
	defer closeRuntime(t, rt)
	handler := rt.mainHandler
	require.Equal(t, "file-1", requestBody(t, handler, http.MethodGet, "/files/a.bin", ""))
	require.Equal(t, "file-1", requestBody(t, handler, http.MethodGet, "/files/a.bin", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&getCount))
	require.Equal(t, "file-1", requestBody(t, handler, http.MethodGet, "/files/package.json", ""))
	atomic.StoreInt32(&version, 2)
	require.Equal(t, "file-2", requestBody(t, handler, http.MethodGet, "/files/package.json", ""))
}

func TestPathProxyExactPrefixRoutesToIndexObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/index", r.URL.Path)
		_, _ = w.Write([]byte("index"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "index", requestBody(t, rt.mainHandler, http.MethodGet, "/files", ""))
}

func TestMainListenerServesSPAAndPathProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("proxied"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)

	rec := httptest.NewRecorder()
	rt.mainHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, rec.Code)

	require.Equal(t, "proxied", requestBody(t, rt.mainHandler, http.MethodGet, "/files/a.txt", ""))
}

func TestRevalidateCacheFreshWindowSkipsUpstreamHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var version int32 = 1
	var headCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.LoadInt32(&version)
		body := []byte(fmt.Sprintf("fresh-%d", current))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", fmt.Sprintf(`"fresh-%d"`, current))
		if r.Method == http.MethodHead {
			atomic.AddInt32(&headCount, 1)
			return
		}
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyRevalidate, FreshFor: config.Duration(100 * time.Millisecond)},
	}})
	defer closeRuntime(t, rt)
	handler := rt.mainHandler
	require.Equal(t, "fresh-1", requestBody(t, handler, http.MethodGet, "/files/a.txt", ""))
	atomic.StoreInt32(&version, 2)
	require.Equal(t, "fresh-1", requestBody(t, handler, http.MethodGet, "/files/a.txt", ""))
	require.Equal(t, int32(0), atomic.LoadInt32(&headCount))
	time.Sleep(120 * time.Millisecond)
	require.Equal(t, "fresh-2", requestBody(t, handler, http.MethodGet, "/files/a.txt", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&headCount))
	stats := rt.stats.Snapshot()
	require.Equal(t, uint64(2), stats.Instances["files"].UpstreamRequests)
	require.Zero(t, stats.Instances["files"].UpstreamStatus["304"])
	require.Equal(t, uint64(2), stats.Instances["files"].UpstreamStatus["200"])
}

func TestConcurrentMissBypassesWhenSameObjectIsDownloading(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Length", "6")
		if current == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write([]byte("cached"))
			return
		}
		_, _ = w.Write([]byte("bypass"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyRevalidate, FreshFor: config.Duration(time.Minute)},
	}})
	defer closeRuntime(t, rt)
	handler := rt.mainHandler
	firstDone := make(chan string, 1)
	go func() {
		firstDone <- requestBody(t, handler, http.MethodGet, "/files/slow.txt", "")
	}()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal("first request did not reach upstream")
	}
	require.Equal(t, "bypass", requestBody(t, handler, http.MethodGet, "/files/slow.txt", ""))
	close(releaseFirst)
	select {
	case body := <-firstDone:
		require.Equal(t, "cached", body)
	case <-ctx.Done():
		t.Fatal("first request did not finish")
	}
	require.Equal(t, "cached", requestBody(t, handler, http.MethodGet, "/files/slow.txt", ""))
	require.Equal(t, int32(2), atomic.LoadInt32(&requests))
}

func TestConcurrentRefreshBypassesInsteadOfServingStaleCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var version int32 = 1
	var slowHead int32
	releaseHead := make(chan struct{})
	headStarted := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.LoadInt32(&version)
		body := []byte(fmt.Sprintf("value-%d", current))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		if r.Method == http.MethodHead && atomic.CompareAndSwapInt32(&slowHead, 0, 1) {
			close(headStarted)
			select {
			case <-releaseHead:
			case <-r.Context().Done():
				return
			}
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyRevalidate},
	}})
	defer closeRuntime(t, rt)
	handler := rt.mainHandler
	require.Equal(t, "value-1", requestBody(t, handler, http.MethodGet, "/files/current.txt", ""))
	atomic.StoreInt32(&version, 2)
	firstDone := make(chan string, 1)
	go func() {
		firstDone <- requestBody(t, handler, http.MethodGet, "/files/current.txt", "")
	}()
	select {
	case <-headStarted:
	case <-ctx.Done():
		t.Fatal("refresh request did not reach HEAD")
	}
	require.Equal(t, "value-2", requestBody(t, handler, http.MethodGet, "/files/current.txt", ""))
	close(releaseHead)
	select {
	case body := <-firstDone:
		require.Equal(t, "value-2", body)
	case <-ctx.Done():
		t.Fatal("refresh request did not finish")
	}
}

func TestConcurrentRefreshCanServeStaleCacheWhenConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var version int32 = 1
	var slowHead int32
	releaseHead := make(chan struct{})
	headStarted := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.LoadInt32(&version)
		body := []byte(fmt.Sprintf("value-%d", current))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		if r.Method == http.MethodHead && atomic.CompareAndSwapInt32(&slowHead, 0, 1) {
			close(headStarted)
			select {
			case <-releaseHead:
			case <-r.Context().Done():
				return
			}
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyRevalidate, BusyPolicy: config.BusyPolicyStale},
	}})
	defer closeRuntime(t, rt)
	handler := rt.mainHandler
	require.Equal(t, "value-1", requestBody(t, handler, http.MethodGet, "/files/current.txt", ""))
	atomic.StoreInt32(&version, 2)
	firstDone := make(chan string, 1)
	go func() {
		firstDone <- requestBody(t, handler, http.MethodGet, "/files/current.txt", "")
	}()
	select {
	case <-headStarted:
	case <-ctx.Done():
		t.Fatal("refresh request did not reach HEAD")
	}
	require.Equal(t, "value-1", requestBody(t, handler, http.MethodGet, "/files/current.txt", ""))
	close(releaseHead)
	select {
	case body := <-firstDone:
		require.Equal(t, "value-2", body)
	case <-ctx.Done():
		t.Fatal("refresh request did not finish")
	}
}

func TestFileProxyPassesConfiguredRequestHeaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "zh-CN", r.Header.Get("Accept-Language"))
		require.Empty(t, r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		PassHeaders: []string{"Accept-Language", "Authorization"},
		Cache:       config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)
	req := httptest.NewRequest(http.MethodGet, "/files/a.txt", nil)
	req.Header.Set("Accept-Language", "zh-CN")
	req.Header.Set("Authorization", "Bearer client-token")
	rec := httptest.NewRecorder()
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestFileProxyPassesConfiguredRequestHeadersOnCacheDownload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "zh-CN", r.Header.Get("Accept-Language"))
		atomic.AddInt32(&requests, 1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		PassHeaders: []string{"Accept-Language"},
		Cache:       config.CacheConfig{DefaultPolicy: config.PolicyImmutable},
	}})
	defer closeRuntime(t, rt)
	req := httptest.NewRequest(http.MethodGet, "/files/a.txt", nil)
	req.Header.Set("Accept-Language", "zh-CN")
	rec := httptest.NewRecorder()
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok", rec.Body.String())

	rec = httptest.NewRecorder()
	rt.mainHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/files/a.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok", rec.Body.String())
	require.Equal(t, int32(1), atomic.LoadInt32(&requests))
}

func TestCachedPolicyPassesThroughUpstreamNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyImmutable},
	}})
	defer closeRuntime(t, rt)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/files/missing.txt", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
}

func TestInstanceUsesConfiguredUpstreamProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var proxyRequests int32
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&proxyRequests, 1)
		require.Equal(t, "upstream.invalid", r.URL.Host)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer proxyServer.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{"http://upstream.invalid/root"},
		Transport: &config.TransportConfig{Proxy: proxyServer.URL},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "proxied", requestBody(t, rt.mainHandler, http.MethodGet, "/files/a.txt", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&proxyRequests))

	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"bad": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/bad"},
		Upstreams: []string{"https://example.com"},
		Transport: &config.TransportConfig{Proxy: "ftp://127.0.0.1:21"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "invalid upstream proxy")
}

func TestInstanceExpireAfterRefetchesOnAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []byte(fmt.Sprintf("value-%d", atomic.AddInt32(&getCount, 1)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		ExpireAfter: config.Duration(10 * time.Millisecond),
		Cache:       config.CacheConfig{DefaultPolicy: config.PolicyImmutable},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "value-1", requestBody(t, rt.mainHandler, http.MethodGet, "/files/a.bin", ""))
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, "value-2", requestBody(t, rt.mainHandler, http.MethodGet, "/files/a.bin", ""))
}

func TestConcurrentProxyRequestsDoNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var version int32 = 1
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(time.Now().UnixNano()%5+1) * time.Millisecond)
		current := atomic.LoadInt32(&version)
		body := []byte(fmt.Sprintf("value-%d", current))
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		w.Header().Set("Last-Modified", time.Unix(int64(1000+current), 0).UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL}, Cache: config.CacheConfig{Rules: []config.CacheRule{{Match: "**/*.txt", Policy: config.PolicyRevalidate}}}}})
	defer closeRuntime(t, rt)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for workerID := 0; workerID < 32; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if workerID == 0 && i%7 == 0 {
					atomic.AddInt32(&version, 1)
				}
				rangeHeader := ""
				if i%5 == 0 {
					rangeHeader = "bytes=0-2"
				}
				_ = requestBody(t, rt.mainHandler, http.MethodGet, "/files/chaos.txt", rangeHeader)
			}
		}(workerID)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent cache operations timed out, possible deadlock")
	}
}

func TestFileProxyRuleLevelFreshForAndExpireAfter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var version int32 = 1
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.LoadInt32(&version)
		body := []byte(fmt.Sprintf("fresh-%d", current))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		if r.Method == http.MethodGet {
			atomic.AddInt32(&getCount, 1)
			_, _ = w.Write(body)
		}
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{
			DefaultPolicy: config.PolicyRevalidate,
			FreshFor:      config.Duration(time.Minute), // global default: 1 minute
			Rules: []config.CacheRule{
				{Match: "**/*.txt", Policy: config.PolicyRevalidate, FreshFor: config.Duration(100 * time.Millisecond)}, // override: 100ms
				{Match: "**/*.json", Policy: config.PolicyRevalidate},                                                   // use global default
			},
		},
	}})
	defer closeRuntime(t, rt)
	handler := rt.mainHandler

	// .txt file: rule-level 100ms freshFor
	require.Equal(t, "fresh-1", requestBody(t, handler, http.MethodGet, "/files/data.txt", ""))
	atomic.StoreInt32(&version, 2)
	require.Equal(t, "fresh-1", requestBody(t, handler, http.MethodGet, "/files/data.txt", "")) // still fresh
	time.Sleep(120 * time.Millisecond)
	require.Equal(t, "fresh-2", requestBody(t, handler, http.MethodGet, "/files/data.txt", "")) // now stale

	// .json file: global 1 minute freshFor
	atomic.StoreInt32(&version, 3)
	require.Equal(t, "fresh-3", requestBody(t, handler, http.MethodGet, "/files/data.json", ""))
	atomic.StoreInt32(&version, 4)
	require.Equal(t, "fresh-3", requestBody(t, handler, http.MethodGet, "/files/data.json", "")) // still fresh (1 min)
}
