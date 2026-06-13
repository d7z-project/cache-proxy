package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

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
		NPM: &config.NPMConfig{Rules: []config.NPMCacheRule{{Match: "**", ResourcePolicy: "metadata", Policy: config.PolicyRevalidate}, {Match: "**", ResourcePolicy: "tarball", Policy: config.PolicyImmutable}}},
	}})
	defer closeRuntime(t, rt)

	body := requestBody(t, rt.mainHandler, http.MethodGet, "/npm/left-pad", "")
	require.Contains(t, body, `"tarball":"http://example.com/npm/left-pad/-/left-pad-1.0.0.tgz"`)
	require.Contains(t, body, `"homepage":"https://registry.npmjs.org/not-a-tarball"`)
	require.Equal(t, "tarball", requestBody(t, rt.mainHandler, http.MethodGet, "/npm/left-pad/-/left-pad-1.0.0.tgz", ""))
	require.Equal(t, "tarball", requestBody(t, rt.mainHandler, http.MethodGet, "/npm/left-pad/-/left-pad-1.0.0.tgz", ""))
	require.Equal(t, int32(1), atomic.LoadInt32(&tarballGets))

	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"bad": {Mode: config.ModeNPM, Listen: config.ListenConfig{Path: "/npm"}, Upstreams: []string{"https://a.example", "https://b.example"}, NPM: &config.NPMConfig{}}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "exactly one upstream")
}

func TestNPMGlobRulesMatchPackageByPattern(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&getCount, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"test"}`))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"npm": {
		Mode: config.ModeNPM, Listen: config.ListenConfig{Path: "/npm"}, Upstreams: []string{upstream.URL},
		NPM: &config.NPMConfig{
			DefaultPolicy: config.PolicyBypass,
			Rules: []config.NPMCacheRule{
				{Match: "@angular/*", ResourcePolicy: "metadata", Policy: config.PolicyImmutable},
				{Match: "left-*", ResourcePolicy: "metadata", Policy: config.PolicyRevalidate},
			},
		},
	}})
	defer closeRuntime(t, rt)

	// @angular/core → immutable
	require.Contains(t, requestBody(t, rt.mainHandler, http.MethodGet, "/npm/@angular/core", ""), `"name"`)
	require.Contains(t, requestBody(t, rt.mainHandler, http.MethodGet, "/npm/@angular/core", ""), `"name"`)
	require.Equal(t, int32(1), atomic.LoadInt32(&getCount))

	// left-pad → revalidate (second request triggers HEAD revalidation)
	require.Contains(t, requestBody(t, rt.mainHandler, http.MethodGet, "/npm/left-pad", ""), `"name"`)
	require.Contains(t, requestBody(t, rt.mainHandler, http.MethodGet, "/npm/left-pad", ""), `"name"`)
	// getCount: 2 GET + 1 HEAD for revalidation = 3
	require.Equal(t, int32(3), atomic.LoadInt32(&getCount))
}
