package npm

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func newNPMTestHandler(t *testing.T, upstream string) *handler {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("npm", "npm", nil, nil, stats)
	require.NoError(t, err)
	handler, err := newHandler("npm", upstream, root+"/state", root+"/work", store, client, stats)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, handler.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return handler
}

func TestPackumentSeparatesValidatorAndAuthorizesTarball(t *testing.T) {
	tarball := []byte("tarball body")
	digest := sha512.Sum512(tarball)
	var packumentRequests atomic.Int32
	var tarballRequests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/pkg":
			packumentRequests.Add(1)
			w.Header().Set("ETag", `"source-v1"`)
			if request.Header.Get("If-None-Match") == `"source-v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": "pkg", "future-field": true,
				"versions": map[string]any{"1.0.0": map[string]any{"dist": map[string]any{
					"tarball":   server.URL + "/files/pkg.tgz",
					"integrity": "sha512-" + base64.StdEncoding.EncodeToString(digest[:]),
				}}},
			})
		case "/files/pkg.tgz":
			tarballRequests.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(tarball)))
			if request.Method == http.MethodHead {
				return
			}
			if request.Header.Get("Range") == "bytes=0-3" {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-3/%d", len(tarball)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(tarball[:4])
				return
			}
			_, _ = w.Write(tarball)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	handler := newNPMTestHandler(t, server.URL)

	request := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/pkg", nil), "https://proxy.example/npm")
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request)
	require.Equal(t, http.StatusOK, first.Code)
	require.NotEqual(t, `"source-v1"`, first.Header().Get("ETag"))
	var payload map[string]any
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &payload))
	require.Equal(t, true, payload["future-field"])
	versions := payload["versions"].(map[string]any)
	dist := versions["1.0.0"].(map[string]any)["dist"].(map[string]any)
	rewritten := dist["tarball"].(string)
	require.Contains(t, rewritten, "https://proxy.example/npm/-/tarball/")

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/pkg", nil), "https://proxy.example/npm"))
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "HIT", second.Header().Get("X-Cache"))
	require.Equal(t, int32(1), packumentRequests.Load())

	revalidateRequest := httptest.NewRequest(http.MethodGet, "/pkg", nil)
	revalidateRequest.Header.Set("Cache-Control", "no-cache")
	revalidated := httptest.NewRecorder()
	handler.ServeHTTP(revalidated, proxyruntime.WithExternalBaseURL(revalidateRequest, "https://proxy.example/npm"))
	require.Equal(t, http.StatusOK, revalidated.Code)
	require.Equal(t, "REVALIDATED", revalidated.Header().Get("X-Cache"))
	require.Equal(t, int32(2), packumentRequests.Load())

	downloadPath := strings.TrimPrefix(rewritten, "https://proxy.example/npm")
	wrongScope := httptest.NewRequest(http.MethodGet, downloadPath, nil)
	wrongScope.Header.Set("Authorization", "Bearer private")
	forbidden := httptest.NewRecorder()
	handler.ServeHTTP(forbidden, wrongScope)
	require.Equal(t, http.StatusForbidden, forbidden.Code)
	head := httptest.NewRecorder()
	handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, downloadPath, nil))
	require.Equal(t, http.StatusOK, head.Code)
	require.Empty(t, head.Body.String())
	partialRequest := httptest.NewRequest(http.MethodGet, downloadPath, nil)
	partialRequest.Header.Set("Range", "bytes=0-3")
	partial := httptest.NewRecorder()
	handler.ServeHTTP(partial, partialRequest)
	require.Equal(t, http.StatusPartialContent, partial.Code)
	require.Equal(t, tarball[:4], partial.Body.Bytes())
	for range 2 {
		download := httptest.NewRecorder()
		handler.ServeHTTP(download, httptest.NewRequest(http.MethodGet, downloadPath, nil))
		require.Equal(t, http.StatusOK, download.Code)
		require.Equal(t, tarball, download.Body.Bytes())
	}
	require.Equal(t, int32(3), tarballRequests.Load())
}

func TestPackumentPublishesUpstreamUpdate(t *testing.T) {
	var revision atomic.Int32
	revision.Store(1)
	var requests atomic.Int32
	var conditional atomic.Value
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/pkg", request.URL.Path)
		requests.Add(1)
		conditional.Store(request.Header.Get("If-None-Match"))
		current := revision.Load()
		w.Header().Set("ETag", fmt.Sprintf(`"source-v%d"`, current))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "pkg",
			"versions": map[string]any{fmt.Sprintf("%d.0.0", current): map[string]any{
				"dist": map[string]any{"tarball": fmt.Sprintf("%s/pkg-%d.tgz", server.URL, current)},
			}},
		})
	}))
	defer server.Close()
	handler := newNPMTestHandler(t, server.URL)
	request := func(force bool) *http.Request {
		req := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/pkg", nil), "https://proxy.example/npm")
		if force {
			req.Header.Set("Cache-Control", "no-cache")
		}
		return req
	}

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request(false))
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), `"1.0.0"`)
	firstETag := first.Header().Get("ETag")
	require.NotEmpty(t, firstETag)
	require.NotEqual(t, `"source-v1"`, firstETag)

	revision.Store(2)
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request(true))
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Contains(t, second.Body.String(), `"2.0.0"`)
	require.NotEqual(t, firstETag, second.Header().Get("ETag"))
	require.NotEqual(t, `"source-v2"`, second.Header().Get("ETag"))
	require.Equal(t, `"source-v1"`, conditional.Load())

	third := httptest.NewRecorder()
	handler.ServeHTTP(third, request(false))
	require.Equal(t, "HIT", third.Header().Get("X-Cache"))
	require.Contains(t, third.Body.String(), `"2.0.0"`)
	require.Equal(t, int32(2), requests.Load())
}

func TestNPMAuditIsTheOnlyReadOnlyPOST(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "/-/npm/v1/security/advisories/bulk", request.URL.Path)
		body, _ := io.ReadAll(request.Body)
		require.Equal(t, "audit", string(body))
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	handler := newNPMTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/-/npm/v1/security/advisories/bulk", strings.NewReader("audit")))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, `{}`, recorder.Body.String())

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/pkg", strings.NewReader("publish")))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, int32(1), requests.Load())
}

func TestNPMRequestPathSupportsScopedFormsWithoutRewritingTraversal(t *testing.T) {
	for raw, expected := range map[string]string{"/": "", "/browse/": "browse/", "/@scope%2fpkg/": "@scope/pkg/"} {
		request := httptest.NewRequest(http.MethodGet, raw, nil)
		cleaned, err := npmRequestPath(request.URL)
		require.NoError(t, err, raw)
		require.Equal(t, expected, cleaned)
	}
	for _, raw := range []string{"/@scope/pkg", "/@scope%2fpkg", "/%40scope%2Fpkg"} {
		request := httptest.NewRequest(http.MethodGet, raw, nil)
		cleaned, err := npmRequestPath(request.URL)
		require.NoError(t, err, raw)
		require.Equal(t, "@scope/pkg", cleaned)
	}
	for _, raw := range []string{"/a/../b", "/a/%2e%2e/b", "/a%2fb", "/a//b", "/a%5cb"} {
		request := httptest.NewRequest(http.MethodGet, raw, nil)
		_, err := npmRequestPath(request.URL)
		require.Error(t, err, raw)
	}
}

func TestNPMRootDirectoriesAndUnknownResourcesRemainTransparent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, request.URL.RequestURI())
	}))
	defer server.Close()
	h := newNPMTestHandler(t, server.URL+"/registry")

	for range 2 {
		for target, expected := range map[string]string{
			"/":                           "/registry/",
			"/browse/":                    "/registry/browse/",
			"/assets/site.css?theme=dark": "/registry/assets/site.css?theme=dark",
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

func TestNPMPackumentAcceptRepresentationsDoNotShareCache(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", request.Header.Get("Accept"))
		_, _ = io.WriteString(w, `{"name":"pkg","versions":{}}`)
	}))
	defer upstream.Close()
	handler := newNPMTestHandler(t, upstream.URL)

	for _, accept := range []string{"application/json", "application/json; profile=custom", "application/json"} {
		request := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/pkg", nil), "https://proxy.example/npm")
		request.Header.Set("Accept", accept)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusOK, recorder.Code)
	}
	require.Equal(t, int32(2), requests.Load())
}

func TestNPMCacheabilityHonorsResponsePolicyAndCredentialScope(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/pkg", nil)
	response := &http.Response{Header: http.Header{"Cache-Control": {"no-store"}}}
	require.False(t, npmCacheable(request, response))
	response.Header.Set("Cache-Control", "private")
	require.False(t, npmCacheable(request, response))
	request.Header.Set("Authorization", "Bearer private")
	require.True(t, npmCacheable(request, response))
}

func TestMalformedPackumentFallbackReleasesSpoolBudget(t *testing.T) {
	const body = `{broken`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	handler := newNPMTestHandler(t, upstream.URL)
	budget := proxyruntime.NewSpoolBudget(int64(len(body)))
	handler.spooler = storeio.NewSpooler(t.TempDir(), int64(len(body)), budget)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/pkg", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, body, recorder.Body.String())
	used, limit := budget.Usage()
	require.Zero(t, used)
	require.Equal(t, int64(len(body)), limit)
}
