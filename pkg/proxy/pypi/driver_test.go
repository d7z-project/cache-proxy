package pypi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestPyPIReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	h := newPyPITestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/simple/demo/", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestNormalizeProjectName(t *testing.T) {
	require.Equal(t, "some-project", normalizeProjectName("Some...__Project"))
}

func TestPyPICacheabilityHonorsNoStore(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/simple/pkg/", nil)
	response := &http.Response{Header: http.Header{"Cache-Control": {"no-store"}}}
	require.False(t, pypiCacheable(request, response))
}

func TestPyPIRootDirectoriesAndUnknownResourcesRemainTransparent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, request.URL.RequestURI())
	}))
	defer server.Close()
	h := newPyPITestHandler(t, server.URL+"/index")

	for range 2 {
		for target, expected := range map[string]string{
			"/":                           "/index/",
			"/browse/":                    "/index/browse/",
			"/assets/site.css?theme=dark": "/index/assets/site.css?theme=dark",
		} {
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
			require.Equal(t, expected, response.Body.String())
		}
	}
	require.Equal(t, int32(6), requests.Load())
}

func TestSimpleJSONRewritesAndVerifiesExternalFile(t *testing.T) {
	fileBody := []byte("wheel bytes")
	digest := sha256.Sum256(fileBody)
	var fileRequests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/simple/demo/":
			w.Header().Set("Content-Type", "application/vnd.pypi.simple.v1+json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"meta": map[string]any{"api-version": "1.4"}, "future": true,
				"files": []map[string]any{{"filename": "demo.whl", "url": server.URL + "/files/demo.whl", "hashes": map[string]string{"sha256": hex.EncodeToString(digest[:])}, "future-file": true}},
			})
		case "/files/demo.whl":
			fileRequests.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileBody)))
			if request.Method == http.MethodHead {
				return
			}
			if request.Header.Get("Range") == "bytes=0-3" {
				w.Header().Set("Content-Length", "4")
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-3/%d", len(fileBody)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fileBody[:4])
				return
			}
			_, _ = w.Write(fileBody)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newPyPITestHandler(t, server.URL)

	request := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/simple/demo/", nil), "https://proxy.example/pypi")
	request.Header.Set("Accept", "application/vnd.pypi.simple.v1+json")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var document map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &document))
	require.Equal(t, true, document["future"])
	file := document["files"].([]any)[0].(map[string]any)
	require.Equal(t, true, file["future-file"])
	rewritten := file["url"].(string)
	require.Contains(t, rewritten, "https://proxy.example/pypi/-/file/")

	downloadPath := strings.TrimPrefix(rewritten, "https://proxy.example/pypi")
	head := httptest.NewRecorder()
	h.ServeHTTP(head, httptest.NewRequest(http.MethodHead, downloadPath, nil))
	require.Equal(t, http.StatusOK, head.Code)
	partialRequest := httptest.NewRequest(http.MethodGet, downloadPath, nil)
	partialRequest.Header.Set("Range", "bytes=0-3")
	partial := httptest.NewRecorder()
	h.ServeHTTP(partial, partialRequest)
	require.Equal(t, http.StatusPartialContent, partial.Code)
	require.Equal(t, fileBody[:4], partial.Body.Bytes())
	for index := range 2 {
		download := httptest.NewRecorder()
		h.ServeHTTP(download, httptest.NewRequest(http.MethodGet, downloadPath, nil))
		require.Equal(t, http.StatusOK, download.Code)
		if index == 0 {
			require.Equal(t, "MISS", download.Header().Get("X-Cache"))
		} else {
			require.Contains(t, []string{"HIT", "COALESCED"}, download.Header().Get("X-Cache"))
		}
		require.Equal(t, fileBody, download.Body.Bytes())
	}
	require.Equal(t, int32(3), fileRequests.Load())
}

func TestPyPISimplePublishesUpstreamUpdate(t *testing.T) {
	var revision atomic.Int32
	revision.Store(1)
	var requests atomic.Int32
	var conditional atomic.Value
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/simple/demo/", request.URL.Path)
		requests.Add(1)
		conditional.Store(request.Header.Get("If-None-Match"))
		current := revision.Load()
		w.Header().Set("Content-Type", "application/vnd.pypi.simple.v1+json")
		w.Header().Set("ETag", fmt.Sprintf(`"source-v%d"`, current))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"meta": map[string]any{"api-version": "1.4"},
			"files": []map[string]any{{
				"filename": fmt.Sprintf("demo-%d.whl", current),
				"url":      fmt.Sprintf("%s/files/demo-%d.whl", server.URL, current),
			}},
		})
	}))
	defer server.Close()
	h := newPyPITestHandler(t, server.URL)
	request := func(force bool) *http.Request {
		req := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/simple/demo/", nil), "https://proxy.example/pypi")
		req.Header.Set("Accept", "application/vnd.pypi.simple.v1+json")
		if force {
			req.Header.Set("Cache-Control", "no-cache")
		}
		return req
	}

	first := httptest.NewRecorder()
	h.ServeHTTP(first, request(false))
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), "demo-1.whl")
	firstETag := first.Header().Get("ETag")
	require.NotEqual(t, `"source-v1"`, firstETag)

	revision.Store(2)
	second := httptest.NewRecorder()
	h.ServeHTTP(second, request(true))
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Contains(t, second.Body.String(), "demo-2.whl")
	require.NotEqual(t, firstETag, second.Header().Get("ETag"))
	require.Equal(t, `"source-v1"`, conditional.Load())

	third := httptest.NewRecorder()
	h.ServeHTTP(third, request(false))
	require.Equal(t, "HIT", third.Header().Get("X-Cache"))
	require.Contains(t, third.Body.String(), "demo-2.whl")
	require.Equal(t, int32(2), requests.Load())
}

func newPyPITestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("pypi", "pypi", nil, nil, stats)
	require.NoError(t, err)
	h, err := newHandler(origin, root+"/state", root+"/work", store, client)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
