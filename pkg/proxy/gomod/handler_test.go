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
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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
	handler := newTestHandler(t, store, config.Expiration(time.Hour), []string{upstream.URL}, nil, &Policy{SumDB: &SumDBConfig{Enabled: false}})
	target := "/" + testModulePath + "/@v/" + testModuleVersion + ".mod"

	first := requestGoProxy(t, handler, target, false)
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(1), upstreamRequests.Load())

	reader, err := store.OpenObject(ctx, "gomod", "go/"+testModulePath+"/@v/"+testModuleVersion+".mod")
	require.NoError(t, err)
	_ = reader.Close()

	second := requestGoProxy(t, handler, target, false)
	require.Equal(t, http.StatusOK, second.Code)
	require.Contains(t, second.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestGoModuleHandlerDisableModuleFetchHeader(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, config.Expiration(time.Hour), []string{upstream.URL}, nil, &Policy{SumDB: &SumDBConfig{Enabled: false}, DisableModuleFetchHeader: true})

	target := "/" + testModulePath + "/@v/list"
	blocked := requestGoProxy(t, handler, target, true)
	require.Equal(t, http.StatusNotFound, blocked.Code)
	require.Zero(t, upstreamRequests.Load())

	handler = newTestHandler(t, store, config.Expiration(time.Hour), []string{upstream.URL}, nil, &Policy{SumDB: &SumDBConfig{Enabled: false}, DisableModuleFetchHeader: false})
	allowed := requestGoProxy(t, handler, target, true)
	require.Equal(t, http.StatusOK, allowed.Code)
	require.Equal(t, "v1.0.0\n", allowed.Body.String())
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestGoModuleHandlerServesCachedModuleWhenFetchDisabled(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, config.Expiration(time.Hour), []string{upstream.URL}, nil, &Policy{SumDB: &SumDBConfig{Enabled: false}})

	target := "/" + testModulePath + "/@v/list"
	require.Equal(t, http.StatusOK, requestGoProxy(t, handler, target, false).Code)
	require.Equal(t, int64(1), upstreamRequests.Load())

	rec := requestGoProxy(t, handler, target, true)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "v1.0.0\n", rec.Body.String())
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestGoModuleHandlerSkipsPrivateModules(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, config.Expiration(time.Hour), []string{upstream.URL}, nil, &Policy{
		SumDB:     &SumDBConfig{Enabled: false},
		GOPrivate: []string{"example.com/cacheproxy/*"},
	})

	rec := requestGoProxy(t, handler, "/"+testModulePath+"/@v/list", false)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Zero(t, upstreamRequests.Load())
}

func TestGoModuleMatchesPrivateModule(t *testing.T) {
	require.True(t, matchesPrivateModule(&Policy{GOPrivate: []string{"example.com/cacheproxy/*"}}, "example.com/cacheproxy/gomod"))
	require.False(t, matchesPrivateModule(&Policy{GOPrivate: []string{"corp.example.com/*"}}, "example.com/cacheproxy/gomod"))
}

func TestGoModuleHandlerServesLatestAndHead(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, config.Expiration(time.Hour), []string{upstream.URL}, nil, &Policy{SumDB: &SumDBConfig{Enabled: false}})

	latest := requestGoProxyMethod(t, handler, http.MethodGet, "/"+testModulePath+"/@latest", false)
	require.Equal(t, http.StatusOK, latest.Code)
	require.JSONEq(t, `{"Version":"v1.0.0","Time":"2026-06-13T00:00:00Z"}`, latest.Body.String())

	head := requestGoProxyMethod(t, handler, http.MethodHead, "/"+testModulePath+"/@v/"+testModuleVersion+".mod", false)
	require.Equal(t, http.StatusOK, head.Code)
	require.Empty(t, head.Body.String())
	require.Equal(t, "text/plain; charset=utf-8", head.Header().Get("Content-Type"))
	require.Equal(t, int64(2), upstreamRequests.Load())
}

func TestGoModuleHandlerProxiesSumDB(t *testing.T) {
	var sumdbRequests atomic.Int64
	sumdb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		sumdbRequests.Add(1)
		require.Equal(t, "/latest", req.URL.Path)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "9")
	}))
	defer sumdb.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, config.Expiration(time.Hour), []string{"https://proxy.golang.org"}, nil, &Policy{
		SumDB: &SumDBConfig{
			Enabled: true,
			Name:    "sum.corp.example",
			URL:     sumdb.URL,
		},
	})

	rec := requestGoProxyMethod(t, handler, http.MethodGet, "/sumdb/sum.corp.example/latest", true)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "9", rec.Body.String())
	require.Equal(t, int64(1), sumdbRequests.Load())
}

func newTestHandler(t *testing.T, store *blobfs.Store, expireAfter config.Expiration, upstreams []string, transport *config.TransportConfig, policy *Policy) *Handler {
	t.Helper()
	handler, err := NewHandler("gomod", expireAfter, upstreams, transport, policy, store, httpcache.NewStats(prometheus.NewRegistry()), nil)
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

func requestGoProxy(t *testing.T, handler http.Handler, target string, disableFetch bool) *httptest.ResponseRecorder {
	t.Helper()
	return requestGoProxyMethod(t, handler, http.MethodGet, target, disableFetch)
}

func requestGoProxyMethod(t *testing.T, handler http.Handler, method, target string, disableFetch bool) *httptest.ResponseRecorder {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, method, target, nil)
	if disableFetch {
		req.Header.Set("Disable-Module-Fetch", "true")
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
		case "/" + testModulePath + "/@latest":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = fmt.Fprintf(w, `{"Version":%q,"Time":"2026-06-13T00:00:00Z"}`, testModuleVersion)
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
