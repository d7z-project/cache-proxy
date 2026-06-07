package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

func TestRuntimeStoresConfigInBlobFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	snapshot, err := rt.Snapshot(ctx)
	require.NoError(t, err)
	require.NotZero(t, snapshot.Generation)
	require.Contains(t, snapshot.YAML, "instances:")
}

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
	handler := proxyHandler(rt)
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
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{upstream.URL},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "index", requestBody(t, proxyHandler(rt), http.MethodGet, "/files", ""))
}

func TestPathProxyUsesSeparateBindAndListsMountedPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("proxied"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{upstream.URL},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)

	adminRec := httptest.NewRecorder()
	rt.servers[0].Handler.ServeHTTP(adminRec, httptest.NewRequest(http.MethodGet, "/files/a.txt", nil).WithContext(ctx))
	require.NotContains(t, adminRec.Body.String(), "proxied")

	indexRec := httptest.NewRecorder()
	proxyHandler(rt).ServeHTTP(indexRec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, indexRec.Code)
	require.Contains(t, indexRec.Body.String(), "/files")
	require.Equal(t, "proxied", requestBody(t, proxyHandler(rt), http.MethodGet, "/files/a.txt", ""))
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
	handler := proxyHandler(rt)
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
	handler := proxyHandler(rt)
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
	handler := proxyHandler(rt)
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
	handler := proxyHandler(rt)
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

func TestOCIProxyCachesBlobByDigest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v2/library/alpine/blobs/sha256:abc", r.URL.Path)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Docker-Content-Digest", "sha256:abc")
		atomic.AddInt32(&getCount, 1)
		_, _ = w.Write([]byte("layer"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5000"}, Upstreams: []string{upstream.URL}, OCI: &config.OCIConfig{BlobPolicy: config.PolicyImmutable}}})
	defer closeRuntime(t, rt)
	handler := rt.handlers[0]
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&getCount))
}

func TestOCIProxyHandlesBearerChallengeAndTokenExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tokenRequests int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenRequests, 1)
		require.Equal(t, "registry.example", r.URL.Query().Get("service"))
		require.Equal(t, "repository:library/alpine:pull", r.URL.Query().Get("scope"))
		require.Equal(t, "Basic dXNlcjpwYXNz", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"token":"token-` + strconv.Itoa(int(atomic.LoadInt32(&tokenRequests))) + `","expires_in":1}`))
	}))
	defer tokenServer.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+tokenServer.URL+`/token",service="registry.example",scope="repository:library/alpine:pull"`)
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		require.Equal(t, "Bearer token-"+strconv.Itoa(int(atomic.LoadInt32(&tokenRequests))), r.Header.Get("Authorization"))
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5000"}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "pass"}},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "ok", requestBody(t, rt.handlers[0], http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, "ok", requestBody(t, rt.handlers[0], http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&tokenRequests))
	time.Sleep(1100 * time.Millisecond)
	require.Equal(t, "ok", requestBody(t, rt.handlers[0], http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, int32(2), atomic.LoadInt32(&tokenRequests))
	stats := rt.stats.Snapshot()
	require.Equal(t, uint64(1), stats.Instances["oci"].UpstreamStatus["200"])
	require.Zero(t, stats.Instances["oci"].UpstreamStatus["401"])
	require.Zero(t, stats.Instances["oci"].UpstreamErrors)
}

func TestOCIProxySetsManifestAcceptHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.Header.Get("Accept"), "application/vnd.oci.image.manifest.v1+json")
		require.Contains(t, r.Header.Get("Accept"), "application/vnd.docker.distribution.manifest.v2+json")
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5000"}, Upstreams: []string{upstream.URL}, OCI: &config.OCIConfig{ManifestPolicy: config.PolicyBypass}}})
	defer closeRuntime(t, rt)
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	req.Header.Set("Accept", "application/x-client-should-not-pass-through")
	rec := httptest.NewRecorder()
	rt.handlers[0].ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
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
	proxyHandler(rt).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
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
	require.Equal(t, "proxied", requestBody(t, proxyHandler(rt), http.MethodGet, "/files/a.txt", ""))
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

func TestOCIProxyUsesConfiguredBasicAuthAndSingleUpstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("Authorization"))
		w.Header().Set("Content-Length", "5")
		_, _ = w.Write([]byte("layer"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5000"}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{BlobPolicy: config.PolicyImmutable, Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "pass"}},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "layer", requestBody(t, rt.handlers[0], http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))

	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5001"}, Upstreams: []string{"https://a.example", "https://b.example"}, OCI: &config.OCIConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "exactly one upstream")
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Path: "/registry"}, Upstreams: []string{"https://registry-1.docker.io"}, OCI: &config.OCIConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "independent listen bind")
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5002"}, Upstreams: []string{"https://registry-1.docker.io"}, PassHeaders: []string{"Accept"}, OCI: &config.OCIConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "only supported in file mode")
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5003"}, Upstreams: []string{"https://registry-1.docker.io"}, OCI: &config.OCIConfig{BlobPolicy: "bad"}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "invalid oci blob policy")
}

func TestNPMProxyRewritesTarballURLsAndCachesTarballs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tarballGets int32
	upstreamURL := ""
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/left-pad":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"left-pad","homepage":"https://registry.npmjs.org/not-a-tarball","versions":{"1.0.0":{"dist":{"tarball":"` + upstreamURL + `/left-pad/-/left-pad-1.0.0.tgz","integrity":"sha512-test"}}}}`))
		case "/left-pad/-/left-pad-1.0.0.tgz":
			atomic.AddInt32(&tarballGets, 1)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", "7")
			_, _ = w.Write([]byte("tarball"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	upstreamURL = upstream.URL
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"npm": {
		Mode: config.ModeNPM, Listen: config.ListenConfig{Path: "/npm"}, Upstreams: []string{upstream.URL},
		NPM: &config.NPMConfig{MetadataPolicy: config.PolicyRevalidate, TarballPolicy: config.PolicyImmutable},
	}})
	defer closeRuntime(t, rt)

	body := requestBody(t, proxyHandler(rt), http.MethodGet, "/npm/left-pad", "")
	require.Contains(t, body, `"tarball":"http://example.com/npm/left-pad/-/left-pad-1.0.0.tgz"`)
	require.Contains(t, body, `"homepage":"https://registry.npmjs.org/not-a-tarball"`)
	require.Equal(t, "tarball", requestBody(t, proxyHandler(rt), http.MethodGet, "/npm/left-pad/-/left-pad-1.0.0.tgz", ""))
	require.Equal(t, "tarball", requestBody(t, proxyHandler(rt), http.MethodGet, "/npm/left-pad/-/left-pad-1.0.0.tgz", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&tarballGets))

	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeNPM, Listen: config.ListenConfig{Path: "/npm"}, Upstreams: []string{"https://a.example", "https://b.example"}, NPM: &config.NPMConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "exactly one upstream")
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
	require.Equal(t, "value-1", requestBody(t, proxyHandler(rt), http.MethodGet, "/files/a.bin", ""))
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, "value-2", requestBody(t, proxyHandler(rt), http.MethodGet, "/files/a.bin", ""))
}

func TestAdminConfigAPIExposesCurrentGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/config", nil).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "generation")
	require.Contains(t, rec.Body.String(), `"blob":"24h0m0s"`)
}

func TestAdminValidateAPIReportsRuntimeConfigErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{
		"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Path: "/registry"}, Upstreams: []string{"https://registry-1.docker.io"}, OCI: &config.OCIConfig{}},
	}
	body := strings.NewReader(mustJSON(t, cfg))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/config/validate", body).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "independent listen bind")
}

func TestInstancesImportExportAPIHandlesSuccessAndConflicts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{"https://example.com"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)

	exportRec := httptest.NewRecorder()
	exportReq := httptest.NewRequest(http.MethodGet, "/api/instances/export?name=files", nil).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(exportRec, exportReq)
	require.Equal(t, http.StatusOK, exportRec.Code)
	var exported instancesExportResponse
	require.NoError(t, json.NewDecoder(exportRec.Body).Decode(&exported))
	require.Contains(t, exported.Instances, "files")

	importBody := strings.NewReader(mustJSON(t, instancesImportRequest{Generation: exported.Generation, Instances: map[string]config.InstanceConfig{"files2": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files2"},
		Upstreams: []string{"https://example.com"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}}))
	importRec := httptest.NewRecorder()
	importReq := httptest.NewRequest(http.MethodPost, "/api/instances/import", importBody).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(importRec, importReq)
	require.Equal(t, http.StatusOK, importRec.Code)

	conflictBody := strings.NewReader(mustJSON(t, instancesImportRequest{Generation: rt.generation, Instances: exported.Instances}))
	conflictRec := httptest.NewRecorder()
	conflictReq := httptest.NewRequest(http.MethodPost, "/api/instances/import", conflictBody).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(conflictRec, conflictReq)
	require.Equal(t, http.StatusConflict, conflictRec.Code)

	emptyBody := strings.NewReader(`{"generation":` + strconv.FormatUint(rt.generation, 10) + `,"instances":{}}`)
	emptyRec := httptest.NewRecorder()
	emptyReq := httptest.NewRequest(http.MethodPost, "/api/instances/import", emptyBody).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(emptyRec, emptyReq)
	require.Equal(t, http.StatusBadRequest, emptyRec.Code)
}

func TestMetricsStatsAPIRecordsProxyRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []byte("metrics")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{upstream.URL},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "metrics", requestBody(t, proxyHandler(rt), http.MethodGet, "/files/a.txt", ""))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/metrics/stats", nil).WithContext(ctx)
	rt.servers[0].Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var stats proxy.StatsSnapshot
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&stats))
	require.Equal(t, uint64(1), stats.Total.Requests)
	require.Equal(t, uint64(1), stats.Total.UpstreamRequests)
	require.Equal(t, uint64(1), stats.Total.Cache["BYPASS"])
	require.Equal(t, uint64(7), stats.Total.ResponseBytes)
	require.Equal(t, uint64(1), stats.Instances["files"].UpstreamStatus["200"])
}

func TestPrometheusMetricsExposeBlobFSStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []byte("blobfs")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	rt := newTestRuntimeWithOptions(t, ctx, Options{Backend: t.TempDir(), AdminBind: "127.0.0.1:0", MetricsBind: freeLocalAddr(t), MetricsPath: "/metrics", GCInterval: time.Hour}, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{upstream.URL},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyImmutable},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "blobfs", requestBody(t, proxyHandler(rt), http.MethodGet, "/files/blobfs.txt", ""))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	metricsHandler(rt).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "cache_proxy_blobfs_objects")
	require.Contains(t, body, "cache_proxy_blobfs_bytes")
	require.Contains(t, body, "cache_proxy_blobfs_chunks")
	require.Contains(t, body, "cache_proxy_blobfs_gc_last_background_error 0")
	require.NotContains(t, body, "cache_proxy_blobfs_gc_last_background_error{")
	require.Contains(t, body, "cache_proxy_requests_total")
}

func TestStartReturnsPortBindingError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	rt := newTestRuntimeWithOptions(t, ctx, Options{Backend: t.TempDir(), AdminBind: listener.Addr().String(), MetricsBind: "", MetricsPath: "/metrics", GCInterval: time.Hour}, nil)
	defer closeRuntime(t, rt)
	require.ErrorContains(t, rt.Start(), "listen")
}

func TestInstanceBindCanChangeDynamically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("dynamic"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{})
	defer closeRuntime(t, rt)
	require.NoError(t, rt.Start())

	firstBind := freeLocalAddr(t)
	cfg := DefaultConfig()
	cfg.Server.Metrics.Bind = ""
	cfg.Instances = map[string]config.InstanceConfig{"files": {Mode: config.ModeFile, Listen: config.ListenConfig{Bind: firstBind}, Upstreams: []string{upstream.URL}, Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass}}}
	snapshot, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.NoError(t, err)
	require.Equal(t, "dynamic", httpBody(t, "http://"+firstBind+"/a.txt"))

	secondBind := freeLocalAddr(t)
	next := structuredCloneConfig(snapshot.Config)
	instance := next.Instances["files"]
	instance.Listen = config.ListenConfig{Bind: secondBind}
	next.Instances["files"] = instance
	_, err = rt.UpdateConfig(ctx, snapshot.Generation, next)
	require.NoError(t, err)
	require.Equal(t, "dynamic", httpBody(t, "http://"+secondBind+"/a.txt"))
	resp, err := (&http.Client{Timeout: 200 * time.Millisecond}).Get("http://" + firstBind + "/a.txt")
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
}

func TestUpdateConfigPreservesStartupOnlyFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	snapshot, err := rt.Snapshot(ctx)
	require.NoError(t, err)
	next := structuredCloneConfig(snapshot.Config)
	next.Version = 99
	next.Server.Metrics.Bind = "127.0.0.1:9999"
	next.Server.Metrics.Path = "/changed"
	next.Storage.GC.Blob = config.Duration(time.Minute)
	updated, err := rt.UpdateConfig(ctx, snapshot.Generation, next)
	require.NoError(t, err)
	require.Equal(t, snapshot.Config.Version, updated.Config.Version)
	require.Equal(t, snapshot.Config.Server, updated.Config.Server)
	require.Equal(t, snapshot.Config.Storage, updated.Config.Storage)
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
				_ = requestBody(t, proxyHandler(rt), http.MethodGet, "/files/chaos.txt", rangeHeader)
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

func structuredCloneConfig(cfg *config.Config) *config.Config {
	next := *cfg
	next.Instances = map[string]config.InstanceConfig{}
	for name, instance := range cfg.Instances {
		next.Instances[name] = instance
	}
	return &next
}

func requestBody(t *testing.T, handler http.Handler, method, target, rangeHeader string) string {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Less(t, rec.Code, 500)
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	return string(body)
}

func proxyHandler(rt *Runtime) http.Handler {
	for _, server := range rt.servers {
		if server.Addr == rt.proxyBind {
			return server.Handler
		}
	}
	return nil
}

func metricsHandler(rt *Runtime) http.Handler {
	for _, server := range rt.servers {
		if server.Addr == rt.metricsBind {
			return server.Handler
		}
	}
	return nil
}

func httpBody(t *testing.T, rawURL string) string {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(rawURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().String()
}

func newTestRuntime(t *testing.T, ctx context.Context, instances map[string]config.InstanceConfig) *Runtime {
	t.Helper()
	dir, err := os.MkdirTemp("", "cache-proxy-runtime-*")
	require.NoError(t, err)
	rt := newTestRuntimeWithOptions(t, ctx, Options{Backend: dir, AdminBind: "127.0.0.1:0", MetricsBind: "", MetricsPath: "/metrics", GCInterval: time.Hour}, instances)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return rt
}

func newTestRuntimeWithOptions(t *testing.T, ctx context.Context, options Options, instances map[string]config.InstanceConfig) *Runtime {
	t.Helper()
	if options.ProxyBind == "" {
		options.ProxyBind = freeLocalAddr(t)
	}
	rt, err := OpenWithOptions(ctx, options)
	require.NoError(t, err)
	if instances != nil {
		cfg := DefaultConfig()
		cfg.Server.Metrics.Bind = ""
		cfg.Instances = instances
		snapshot, err := rt.UpdateConfig(ctx, rt.generation, cfg)
		require.NoError(t, err)
		require.NotZero(t, snapshot.Generation)
	}
	return rt
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return string(data)
}

func closeRuntime(t *testing.T, rt *Runtime) {
	t.Helper()
	if rt == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, rt.Close(ctx))
}
