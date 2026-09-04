package gomod

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const testModulePath = "example.com/cacheproxy/gomod"
const testModuleVersion = "v1.0.0"

func TestGoProxyReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer upstream.Close()
	handler := newTestHandler(t, newTestStore(t), upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}})
	response := requestGoProxyMethod(t, handler, http.MethodPut, "/example.com/mod/@v/v1.0.0.mod", false)
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestVersionedModuleFilesAreImmutableAndJoined(t *testing.T) {
	for _, suffix := range []string{".info", ".mod", ".zip"} {
		route, err := parseModuleRequest("example.com/mod/@v/v1.0.0" + suffix)
		require.NoError(t, err)
		require.NotEqual(t, moduleRequestInvalid, route.kind, suffix)
	}
}

func TestGoCacheabilityHonorsNoStore(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/example.com/mod/@v/list", nil)
	response := &http.Response{Header: http.Header{"Cache-Control": {"no-store"}}}
	require.False(t, goCacheable(request, response))
}

func TestGoModuleHandlerCachesModuleFilesInBlobFS(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}})
	target := "/" + testModulePath + "/@v/" + testModuleVersion + ".mod"

	first := requestGoProxy(t, handler, target, false)
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(1), upstreamRequests.Load())

	second := requestGoProxy(t, handler, target, false)
	require.Equal(t, http.StatusOK, second.Code)
	require.Contains(t, second.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestGoModuleCacheCommitFailurePreservesUpstreamResponse(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}})
	target := testModulePath + "/@v/" + testModuleVersion + ".mod"
	parsed, err := parseModuleRequest(target)
	require.NoError(t, err)
	key := "objects/" + hashKey(handler.origin.String()+"\x00"+parsed.cacheKey+"\x00anonymous")
	digest := sha256.Sum256([]byte(key))
	encoded := hex.EncodeToString(digest[:])
	require.NoError(t, store.MkdirAll(goTenant+"/responses/"+encoded[:2]+"/"+encoded[2:4]+"/"+encoded, 0o755))

	response := requestGoProxy(t, handler, "/"+target, false)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "MISS", response.Header().Get("X-Cache"))
	require.Contains(t, response.Body.String(), "module "+testModulePath)
	require.Equal(t, int64(1), upstreamRequests.Load())
	require.NoError(t, handler.CloseContext(context.Background()))
}

func TestGoModuleHandlerDisableModuleFetchHeader(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}, DisableModuleFetchHeader: true})

	target := "/" + testModulePath + "/@v/list"
	blocked := requestGoProxy(t, handler, target, true)
	require.Equal(t, http.StatusNotFound, blocked.Code)
	require.Zero(t, upstreamRequests.Load())

	handler = newTestHandler(t, store, upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}, DisableModuleFetchHeader: false})
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
	handler := newTestHandler(t, store, upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}})

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
	handler := newTestHandler(t, store, upstream.URL, nil, &Config{
		SumDB:     &SumDBConfig{Enabled: false},
		GOPrivate: []string{"example.com/cacheproxy/*"},
	})

	rec := requestGoProxy(t, handler, "/"+testModulePath+"/@v/list", false)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Zero(t, upstreamRequests.Load())
}

func TestGoModuleMatchesPrivateModule(t *testing.T) {
	require.True(t, matchesPrivateModule(&Config{GOPrivate: []string{"example.com/cacheproxy/*"}}, "example.com/cacheproxy/gomod"))
	require.False(t, matchesPrivateModule(&Config{GOPrivate: []string{"corp.example.com/*"}}, "example.com/cacheproxy/gomod"))
}

func TestGoModuleHandlerServesLatestAndHead(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := newGoProxyUpstream(t, &upstreamRequests)
	defer upstream.Close()
	store := newTestStore(t)
	handler := newTestHandler(t, store, upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}})

	latest := requestGoProxyMethod(t, handler, http.MethodGet, "/"+testModulePath+"/@latest", false)
	require.Equal(t, http.StatusOK, latest.Code)
	require.JSONEq(t, `{"Version":"v1.0.0","Time":"2026-06-13T00:00:00Z"}`, latest.Body.String())

	head := requestGoProxyMethod(t, handler, http.MethodHead, "/"+testModulePath+"/@v/"+testModuleVersion+".mod", false)
	require.Equal(t, http.StatusOK, head.Code)
	require.Empty(t, head.Body.String())
	require.Equal(t, "text/plain; charset=utf-8", head.Header().Get("Content-Type"))
	require.Equal(t, int64(2), upstreamRequests.Load())
}

func TestGoModuleMutableListPublishesUpstreamUpdate(t *testing.T) {
	var revision atomic.Int32
	revision.Store(1)
	var requests atomic.Int32
	var conditional atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		conditional.Store(request.Header.Get("If-None-Match"))
		current := revision.Load()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		_, _ = fmt.Fprintf(w, "v%d.0.0\n", current)
	}))
	defer upstream.Close()
	handler := newTestHandler(t, newTestStore(t), upstream.URL, nil, &Config{SumDB: &SumDBConfig{Enabled: false}})
	target := "/" + testModulePath + "/@v/list"
	parsed, err := parseModuleRequest(strings.TrimPrefix(target, "/"))
	require.NoError(t, err)
	key := "objects/" + hashKey(handler.origin.String()+"\x00"+parsed.cacheKey+"\x00anonymous")
	cachedBody := func() string {
		object, err := storeio.OpenResponse(context.Background(), handler.store, goTenant, key)
		if err != nil {
			return ""
		}
		defer func() { _ = object.Reader.Close() }()
		body, err := io.ReadAll(object.Reader)
		if err != nil {
			return ""
		}
		return string(body)
	}

	first := requestGoProxy(t, handler, target, false)
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "v1.0.0\n", first.Body.String())
	require.Eventually(t, func() bool { return cachedBody() == "v1.0.0\n" }, time.Second, time.Millisecond)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	require.NoError(t, handler.flights.Do(waitCtx, key, func() error { return nil }))
	// Model the interval where a streamed response is visible before its miss flight finishes.
	pendingInitialFill, leader := handler.flights.Begin(key)
	require.True(t, leader)
	defer handler.flights.Finish(key, pendingInitialFill, nil)

	revision.Store(2)
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), time.Second)
	defer cancelRequest()
	request := httptest.NewRequestWithContext(requestCtx, http.MethodGet, target, nil)
	request.Header.Set("Cache-Control", "no-cache")
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "v2.0.0\n", second.Body.String())
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Equal(t, `"v1"`, conditional.Load())
	require.Eventually(t, func() bool { return cachedBody() == "v2.0.0\n" }, time.Second, time.Millisecond)
	handler.flights.Finish(key, pendingInitialFill, nil)

	third := requestGoProxy(t, handler, target, false)
	require.Equal(t, http.StatusOK, third.Code)
	require.Equal(t, "v2.0.0\n", third.Body.String())
	require.Equal(t, "HIT", third.Header().Get("X-Cache"))
	require.Equal(t, int32(2), requests.Load())
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
	handler := newTestHandler(t, store, "https://proxy.golang.org", nil, &Config{
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

	for _, target := range []string{
		"/sumdb/sum.corp.example/../latest",
		"/sumdb/sum.corp.example/%2e%2e/latest",
	} {
		response := requestGoProxyMethod(t, handler, http.MethodGet, target, true)
		require.Equal(t, http.StatusNotFound, response.Code)
	}
	require.Equal(t, int64(1), sumdbRequests.Load())
}

func newTestHandler(t *testing.T, store *blobfs.Store, upstream string, transport *config.TransportConfig, options *Config) *handler {
	t.Helper()
	spooler := storeio.NewSpooler(t.TempDir(), 2<<30, nil)
	handler, err := newHandler("gomod", upstream, transport, options, store, metrics.NewStats(prometheus.NewRegistry()), nil, spooler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.CloseContext(context.Background())) })
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
