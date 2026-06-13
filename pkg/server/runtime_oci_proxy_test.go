package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

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
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL}, OCI: &config.OCIConfig{Rules: []config.OCICacheRule{{Match: "library/*", Policy: config.PolicyImmutable}}}}})
	defer closeRuntime(t, rt)
	rt.mu.RLock()
	handler := rt.handlers[0]
	rt.mu.RUnlock()
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&getCount))
}

func TestOCIProxyHandlesBearerChallengeAndTokenExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tokenRequests int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&tokenRequests, 1)
		require.Equal(t, "registry.example", r.URL.Query().Get("service"))
		require.Equal(t, "repository:library/alpine:pull", r.URL.Query().Get("scope"))
		require.Equal(t, "Basic dXNlcjpwYXNz", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"token":"token-` + strconv.Itoa(int(n)) + `","expires_in":1}`))
	}))
	defer tokenServer.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+tokenServer.URL+`/token",service="registry.example",scope="repository:library/alpine:pull"`)
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "pass"}},
	}})
	defer closeRuntime(t, rt)
	rt.mu.RLock()
	handler := rt.handlers[0]
	rt.mu.RUnlock()

	require.Equal(t, "ok", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, "ok", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&tokenRequests), "token should be cached for second request")

	time.Sleep(1100 * time.Millisecond)
	require.Equal(t, "ok", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, int32(2), atomic.LoadInt32(&tokenRequests), "expired token should trigger new fetch")

	stats := rt.stats.Snapshot()
	require.Equal(t, uint64(3), stats.Instances["oci"].UpstreamRequests)
	require.Equal(t, uint64(3), stats.Instances["oci"].UpstreamStatus["200"], "each request records final 200 after retry")
	require.Zero(t, stats.Instances["oci"].UpstreamStatus["401"], "401 is retried and not recorded")
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
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL}, OCI: &config.OCIConfig{Rules: []config.OCICacheRule{{Match: "library/*", Policy: config.PolicyBypass}}}}})
	defer closeRuntime(t, rt)
	rt.mu.RLock()
	handler := rt.handlers[0]
	rt.mu.RUnlock()
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	req.Header.Set("Accept", "application/x-client-should-not-pass-through")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
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
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{Rules: []config.OCICacheRule{{Match: "library/*", Policy: config.PolicyImmutable}}, Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "pass"}},
	}})
	defer closeRuntime(t, rt)
	rt.mu.RLock()
	handler := rt.handlers[0]
	rt.mu.RUnlock()
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))

	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5001"}, Upstreams: []string{"https://a.example", "https://b.example"}, OCI: &config.OCIConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "exactly one upstream")
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Path: "/registry"}, Upstreams: []string{"https://registry-1.docker.io"}, OCI: &config.OCIConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "independent listen bind")
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5002"}, Upstreams: []string{"https://registry-1.docker.io"}, PassHeaders: []string{"Accept"}, OCI: &config.OCIConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "only supported in file mode")
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5003"}, Upstreams: []string{"https://registry-1.docker.io"}, OCI: &config.OCIConfig{Rules: []config.OCICacheRule{{Match: "bad", Policy: "invalid-policy"}}}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "invalid policy")
}

func TestOCIGlobRulesMatchRepoByPattern(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&getCount, 1)
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Docker-Content-Digest", "sha256:abc")
		_, _ = w.Write([]byte("layer"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{
			DefaultPolicy: config.PolicyRevalidate,
			Rules: []config.OCICacheRule{
				{Match: "library/*", Policy: config.PolicyImmutable},
				{Match: "myorg/**", Policy: config.PolicyBypass},
			},
		},
	}})
	defer closeRuntime(t, rt)
	rt.mu.RLock()
	handler := rt.handlers[0]
	rt.mu.RUnlock()

	// library/* blob → immutable (cached once)
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&getCount))

	// myorg/app blob → bypass (always fetched)
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/myorg/app/blobs/sha256:abc", ""))
	require.Equal(t, "layer", requestBody(t, handler, http.MethodGet, "/v2/myorg/app/blobs/sha256:abc", ""))
	require.Equal(t, int32(3), atomic.LoadInt32(&getCount)) // +2 for myorg
}

func TestOCIGlobRulesFirstMatchWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&getCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{
			DefaultPolicy: config.PolicyBypass,
			Rules: []config.OCICacheRule{
				{Match: "library/*", Policy: config.PolicyRevalidate},
				{Match: "library/*", Policy: config.PolicyImmutable},
			},
		},
	}})
	defer closeRuntime(t, rt)
	rt.mu.RLock()
	handler := rt.handlers[0]
	rt.mu.RUnlock()

	// library/* → revalidate (first rule wins, second rule never reached)
	// manifest: revalidate → HEAD revalidation
	require.Equal(t, "{}", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, "{}", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/manifests/latest", ""))
	require.Equal(t, int32(2), atomic.LoadInt32(&getCount))

	// blob: also revalidate (same repo, same rule)
	require.Equal(t, "{}", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, "{}", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", ""))
	require.Equal(t, int32(4), atomic.LoadInt32(&getCount))

	// tags/list: also revalidate (same repo, same rule)
	require.Equal(t, "{}", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/tags/list", ""))
	require.Equal(t, "{}", requestBody(t, handler, http.MethodGet, "/v2/library/alpine/tags/list", ""))
	require.Equal(t, int32(6), atomic.LoadInt32(&getCount))
}
