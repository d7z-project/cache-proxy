package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestGoModuleProxyServesPathMountedGOPROXY(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var upstreamRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamRequests.Add(1)
		if req.URL.Path != "/example.com/cacheproxy/gomod/@v/list" {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "v1.0.0\n")
	}))
	defer upstream.Close()

	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"gomod": {
		Mode:        config.ModeGo,
		Listen:      config.ListenConfig{Path: "/go"},
		Upstreams:   []string{upstream.URL},
		ExpireAfter: config.Duration(time.Hour),
		Cache:       config.CacheConfig{},
		Go:          &config.GoConfig{SumDB: "off", DisableModuleFetchHeader: true},
	}})
	defer closeRuntime(t, rt)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/go/example.com/cacheproxy/gomod/@v/list", nil)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "v1.0.0", rec.Body.String())

	rec = httptest.NewRecorder()
	cachedReq := httptest.NewRequest(http.MethodGet, "/go/example.com/cacheproxy/gomod/@v/list", nil)
	cachedReq.Header.Set("Disable-Module-Fetch", "true")
	rt.mainHandler.ServeHTTP(rec, cachedReq)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "v1.0.0", rec.Body.String())
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestValidateGoModeConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"gomod": {
		Mode:      config.ModeGo,
		Listen:    config.ListenConfig{Path: "/go"},
		Upstreams: []string{"https://proxy.golang.org"},
		Go:        &config.GoConfig{SumDB: "off", MaxDirectFetches: 4},
	}}
	require.NoError(t, ValidateConfig(cfg, "127.0.0.1:0"))

	cfg.Instances["gomod"] = config.InstanceConfig{
		Mode:      config.ModeGo,
		Listen:    config.ListenConfig{Path: "/go"},
		Upstreams: []string{"https://proxy.golang.org"},
		NPM:       &config.NPMConfig{},
		Go:        &config.GoConfig{SumDB: "off"},
	}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must not have npm config")

	cfg.Instances["gomod"] = config.InstanceConfig{
		Mode:      config.ModeGo,
		Listen:    config.ListenConfig{Path: "/go"},
		Upstreams: []string{"https://proxy.golang.org"},
		Go:        &config.GoConfig{MaxDirectFetches: -1},
	}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must not be negative")
}
