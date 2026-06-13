package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

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
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	}, instances)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return rt
}

func newTestRuntimeWithOptions(t *testing.T, ctx context.Context, options Options, instances map[string]config.InstanceConfig) *Runtime {
	t.Helper()
	rt, err := OpenWithOptions(ctx, options)
	require.NoError(t, err)
	if instances != nil {
		cfg := DefaultConfig()
		cfg.Server.Metrics.Path = ""
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
