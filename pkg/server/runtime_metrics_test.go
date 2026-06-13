package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

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
	require.Equal(t, "metrics", requestBody(t, rt.mainHandler, http.MethodGet, "/files/a.txt", ""))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/metrics/stats", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
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
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		MetricsToken: "",
	}, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{upstream.URL},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyImmutable},
	}})
	defer closeRuntime(t, rt)
	require.Equal(t, "blobfs", requestBody(t, rt.mainHandler, http.MethodGet, "/files/blobfs.txt", ""))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/metrics", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "cache_proxy_blobfs_objects")
	require.Contains(t, body, "cache_proxy_blobfs_bytes")
	require.Contains(t, body, "cache_proxy_blobfs_chunks")
	require.Contains(t, body, "cache_proxy_requests_total")
}

func TestPrometheusMetricsUsesConfiguredPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/metrics",
		GCInterval:   time.Hour,
		MetricsToken: "",
	}, nil)
	defer closeRuntime(t, rt)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "cache_proxy_blobfs_objects")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/api/runtime", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"metricsPath":"/metrics"`)
}

func TestMetricsPathConflictsWithInstanceListenPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/metrics",
		GCInterval:   time.Hour,
		MetricsToken: "",
	}, nil)
	defer closeRuntime(t, rt)
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/metrics"},
		Upstreams: []string{"https://example.com"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}
	_, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.ErrorContains(t, err, "conflicts with metrics path")
}

func TestInvalidMetricsPathIsRejected(t *testing.T) {
	for _, metricsPath := range []string{"metrics", "/", "/-/api/metrics", "/metrics/", "/{name}", "/bad path"} {
		t.Run(metricsPath, func(t *testing.T) {
			_, err := OpenWithOptions(context.Background(), Options{
				Backend:     t.TempDir(),
				Bind:        "127.0.0.1:0",
				MetricsPath: metricsPath,
				GCInterval:  time.Hour,
			})
			require.Error(t, err)
		})
	}
}

func TestMetricsTokenAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		MetricsToken: "my-metrics-token",
	}, nil)
	defer closeRuntime(t, rt)

	// without token: 401
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/metrics", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Header().Get("WWW-Authenticate"), "Bearer")

	// with correct token: 200
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/metrics", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer my-metrics-token")
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// with wrong token: 401
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/metrics", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}
