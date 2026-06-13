package gomod

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

const testModulePath = "example.com/cacheproxy/gomod"
const testModuleVersion = "v1.0.0"

func TestGoModuleHandlerCachesModuleFilesInBlobFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, config.InstanceConfig{
		Mode:        config.ModeGo,
		Listen:      config.ListenConfig{Path: "/go"},
		Upstreams:   []string{upstream.URL},
		ExpireAfter: config.Duration(time.Hour),
		Cache:       config.CacheConfig{},
		Go:          &config.GoConfig{SumDB: "off"},
	})
	target := "/" + testModulePath + "/@v/" + testModuleVersion + ".mod"

	first := requestGoProxy(t, handler, target, "")
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(3), upstreamRequests.Load())

	reader, err := store.OpenObject(ctx, "gomod", "go/"+testModulePath+"/@v/"+testModuleVersion+".mod")
	require.NoError(t, err)
	_ = reader.Close()

	second := requestGoProxy(t, handler, target, "")
	require.Equal(t, http.StatusOK, second.Code)
	require.Contains(t, second.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(3), upstreamRequests.Load())
}

func TestGoModuleHandlerDisableModuleFetchHeader(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	cfg := config.InstanceConfig{
		Mode:        config.ModeGo,
		Listen:      config.ListenConfig{Path: "/go"},
		Upstreams:   []string{upstream.URL},
		ExpireAfter: config.Duration(time.Hour),
		Go:          &config.GoConfig{SumDB: "off", DisableModuleFetchHeader: true},
	}
	handler := newTestHandler(t, store, cfg)

	target := "/" + testModulePath + "/@v/list"
	blocked := requestGoProxy(t, handler, target, "true")
	require.Equal(t, http.StatusNotFound, blocked.Code)
	require.Zero(t, upstreamRequests.Load())

	cfg.Go.DisableModuleFetchHeader = false
	handler = newTestHandler(t, store, cfg)
	allowed := requestGoProxy(t, handler, target, "true")
	require.Equal(t, http.StatusOK, allowed.Code)
	require.Equal(t, "v1.0.0", allowed.Body.String())
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func newTestHandler(t *testing.T, store *blobfs.Store, cfg config.InstanceConfig) *Handler {
	t.Helper()
	handler, err := NewHandler("gomod", cfg, store, proxy.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(handler.Close)
	return handler
}

func newTestStore(t *testing.T) *blobfs.Store {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func requestGoProxy(t *testing.T, handler http.Handler, target, disableFetch string) *httptest.ResponseRecorder {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if disableFetch != "" {
		req.Header.Set("Disable-Module-Fetch", disableFetch)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func newGoProxyUpstream(t *testing.T, requests *atomic.Int64) *httptest.Server {
	t.Helper()
	zipContent := testModuleZip(t)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		switch req.URL.Path {
		case "/" + testModulePath + "/@v/list":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, testModuleVersion+"\n")
		case "/" + testModulePath + "/@v/" + testModuleVersion + ".info":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = fmt.Fprintf(w, `{"Version":%q,"Time":"2026-06-13T00:00:00Z"}`, testModuleVersion)
		case "/" + testModulePath + "/@v/" + testModuleVersion + ".mod":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, "module "+testModulePath+"\n\ngo 1.25\n")
		case "/" + testModulePath + "/@v/" + testModuleVersion + ".zip":
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(zipContent)
		default:
			http.NotFound(w, req)
		}
	}))
}

func testModuleZip(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+testModulePath+"\n\ngo 1.25\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "gomod.go"), []byte("package gomod\n"), 0o644))
	var buf bytes.Buffer
	require.NoError(t, modzip.CreateFromDir(&buf, module.Version{Path: testModulePath, Version: testModuleVersion}, dir))
	return buf.Bytes()
}
