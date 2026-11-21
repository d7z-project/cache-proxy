package openvsx

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

func newTestHandler(t *testing.T, upstream, publicBase string, freshness time.Duration) *handler {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("openvsx", "openvsx", nil, nil, stats)
	require.NoError(t, err)
	originURL, err := url.Parse(upstream)
	require.NoError(t, err)
	publicURL, err := url.Parse(publicBase)
	require.NoError(t, err)
	h := newHandler(handlerConfig{
		name: "openvsx", origin: originURL, publicBase: publicURL, workDir: root + "/work",
		store: store, client: client, stats: stats, freshness: freshness,
	})
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}

func TestClassifyResource(t *testing.T) {
	tests := []struct {
		path string
		kind resourceKind
	}{
		{"api/redhat/java/1.0.0/file/download", artifactResource},
		{"api/ms-python/python/alpine-arm64/2026.10.0/file/download", artifactResource},
		{"vscode/asset/redhat/java/1.0.0/Microsoft.VisualStudio.Services.VSIXPackage", artifactResource},
		{"vscode/gallery/publishers/redhat/vsextensions/java/1.0.0/vspackage", artifactResource},
		{"vscode/unpkg/redhat/java/1.0.0/", redirectableDocumentResource},
		{"vscode/unpkg/redhat/java/1.0.0/extension/package.json", redirectableDocumentResource},
		{"api/redhat/java/1.0.0/files/download", documentResource},
		{"api/redhat/java/file/download", documentResource},
		{"other/redhat/java/1.0.0/file/download", documentResource},
		{"other/gallery/publishers/redhat/vsextensions/java/1.0.0/vspackage", documentResource},
		{"vscode/item", documentResource},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			require.Equal(t, test.kind, classifyResource(test.path))
		})
	}
}

func TestGalleryQueryRewritesCachesAndRefreshes(t *testing.T) {
	var requests atomic.Int32
	var revision atomic.Int32
	var upstreamURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/vscode/gallery/extensionquery", r.URL.Path)
		require.Empty(t, r.Header.Get("X-Market-User-Id"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"filters":[]}`, string(body))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		fmt.Fprintf(w, `{"revision":%d,"assetUri":%q,"files":[{"source":%q}],"homepage":"https://github.com/eclipse-openvsx/openvsx"}`,
			revision.Load(), upstreamURL+"/vscode/asset/redhat/java/1.0.0/vsix", upstreamURL+"/api/redhat/java/1.0.0/file/download")
	}))
	defer upstream.Close()
	upstreamURL = upstream.URL
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	query := func(noCache bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/vscode/gallery/extensionquery", strings.NewReader(`{"filters":[]}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Market-User-Id", "private-user")
		if noCache {
			req.Header.Set("Cache-Control", "no-cache")
		}
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, req)
		return recorder
	}

	first := query(false)
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "MISS", first.Header().Get("X-Cache"))
	require.Contains(t, first.Body.String(), `https://openvsx.proxy.test/vscode/asset/`)
	require.Contains(t, first.Body.String(), `https://openvsx.proxy.test/api/`)
	require.Contains(t, first.Body.String(), `https://github.com/eclipse-openvsx/openvsx`)

	second := query(false)
	require.Equal(t, "HIT", second.Header().Get("X-Cache"))
	require.Equal(t, int32(1), requests.Load())

	revision.Store(2)
	refreshed := query(true)
	require.Equal(t, http.StatusOK, refreshed.Code)
	require.Contains(t, refreshed.Body.String(), `"revision":2`)
	require.Equal(t, int32(2), requests.Load())

	rejected := httptest.NewRecorder()
	h.ServeHTTP(rejected, httptest.NewRequest(http.MethodPost, "/api/redhat/java/review", strings.NewReader("write")))
	require.Equal(t, http.StatusMethodNotAllowed, rejected.Code)
	require.Equal(t, int32(2), requests.Load())
}

func TestItemRedirectAndWebUIStayOnProxyOrigin(t *testing.T) {
	var upstreamURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/vscode/item":
			w.Header().Set("Location", upstreamURL+"/extension/redhat/java")
			w.WriteHeader(http.StatusFound)
		case "/extension/redhat/java":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=600")
			w.Header().Set("Content-Location", upstreamURL+"/extension/redhat/java")
			w.Header().Set("Link", "<"+upstreamURL+"/bundle-hash.js>; rel=preload")
			_, _ = io.WriteString(w, `<!doctype html><html><head>`+
				`<script src="https://tracker.example/analytics.js"></script>`+
				`<script>fetch("https://www.googletagmanager.com/gtm.js")</script>`+
				`<script type="module" src="`+upstreamURL+`/bundle-hash.js"></script>`+
				`<link rel="stylesheet" href="/index-hash.css"></head><body><a href="https://github.com/redhat-developer/vscode-java">source</a></body></html>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	upstreamURL = upstream.URL
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	redirect := httptest.NewRecorder()
	h.ServeHTTP(redirect, httptest.NewRequest(http.MethodGet, "/vscode/item?itemName=redhat.java", nil))
	require.Equal(t, http.StatusFound, redirect.Code)
	require.Equal(t, "https://openvsx.proxy.test/extension/redhat/java", redirect.Header().Get("Location"))

	page := httptest.NewRecorder()
	h.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/extension/redhat/java", nil))
	require.Equal(t, http.StatusOK, page.Code)
	require.Contains(t, page.Body.String(), `https://openvsx.proxy.test/bundle-hash.js`)
	require.Contains(t, page.Body.String(), `href="/index-hash.css"`)
	require.Contains(t, page.Body.String(), `https://github.com/redhat-developer/vscode-java`)
	require.NotContains(t, page.Body.String(), "tracker.example")
	require.NotContains(t, page.Body.String(), "googletagmanager.com")
	require.Contains(t, page.Header().Get("Content-Security-Policy"), "connect-src 'self'")
	require.Equal(t, "https://openvsx.proxy.test/extension/redhat/java", page.Header().Get("Content-Location"))
	require.Equal(t, "<https://openvsx.proxy.test/bundle-hash.js>; rel=preload", page.Header().Get("Link"))
	origin, err := url.Parse("https://open-vsx.org")
	require.NoError(t, err)
	publicBase, err := url.Parse("https://openvsx.proxy.test")
	require.NoError(t, err)
	require.Equal(t, `https://open-vsx.org.evil/api`, rewriteText(`https://open-vsx.org.evil/api`, origin, publicBase))
}

func TestTargetPlatformVSIXRedirectIsFollowedAndCached(t *testing.T) {
	var downloads atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		require.Empty(t, r.Header.Get("Authorization"))
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "vsix-package")
	}))
	defer cdn.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/ms-python/python/alpine-arm64/2026.10.0/file/download", r.URL.Path)
		w.Header().Set("Location", cdn.URL+"/packages/redhat.java.vsix")
		w.WriteHeader(http.StatusFound)
	}))
	defer upstream.Close()
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/ms-python/python/alpine-arm64/2026.10.0/file/download", nil)
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, req)
		return recorder
	}
	first := request()
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "vsix-package", first.Body.String())
	require.NotEqual(t, cdn.URL, first.Header().Get("Location"))
	require.Eventually(t, func() bool {
		warm := request()
		return warm.Code == http.StatusOK && warm.Header().Get("X-Cache") == "HIT"
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), downloads.Load())
}

func TestUnpkgRewritesDirectoryAndFollowsFileRedirect(t *testing.T) {
	var downloads atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		require.Equal(t, "/extension/module.wasm", r.URL.Path)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "application/wasm")
		_, _ = io.WriteString(w, "wasm-content")
	}))
	defer cdn.Close()

	var upstreamURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/vscode/unpkg/e2e/extension/1.0.0/extension":
			w.Header().Set("Cache-Control", "public, max-age=3600")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `[%q]`, upstreamURL+"/vscode/unpkg/e2e/extension/1.0.0/extension/module.wasm")
		case "/vscode/unpkg/e2e/extension/1.0.0/extension/module.wasm":
			w.Header().Set("Location", cdn.URL+"/extension/module.wasm")
			w.WriteHeader(http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	upstreamURL = upstream.URL
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	directory := httptest.NewRecorder()
	h.ServeHTTP(directory, httptest.NewRequest(http.MethodGet, "/vscode/unpkg/e2e/extension/1.0.0/extension", nil))
	require.Equal(t, http.StatusOK, directory.Code)
	require.Contains(t, directory.Body.String(), "https://openvsx.proxy.test/vscode/unpkg/")
	require.NotContains(t, directory.Body.String(), upstream.URL)

	requestFile := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/vscode/unpkg/e2e/extension/1.0.0/extension/module.wasm", nil))
		return response
	}
	first := requestFile()
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "wasm-content", first.Body.String())
	require.Empty(t, first.Header().Get("Location"))
	require.Equal(t, "MISS", first.Header().Get("X-Cache"))

	second := requestFile()
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "HIT", second.Header().Get("X-Cache"))
	require.Equal(t, "wasm-content", second.Body.String())
	require.Equal(t, int32(1), downloads.Load())
}

func TestRawDocumentUsesSourceValidator(t *testing.T) {
	const etag = `"public-key-v1"`
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "/api/-/public-key/key-id", r.URL.Path)
		w.Header().Set("Cache-Control", "public, max-age=0")
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "text/plain")
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		require.Empty(t, r.Header.Get("If-None-Match"))
		_, _ = io.WriteString(w, "public-key")
	}))
	defer upstream.Close()
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	request := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/-/public-key/key-id", nil))
		return response
	}
	first := request()
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "MISS", first.Header().Get("X-Cache"))
	require.Equal(t, "public-key", first.Body.String())
	require.Empty(t, first.Header().Get("X-Source-ETag"))

	second := request()
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "REVALIDATED", second.Header().Get("X-Cache"))
	require.Equal(t, "public-key", second.Body.String())
	require.Empty(t, second.Header().Get("X-Source-ETag"))
	require.Equal(t, int32(2), requests.Load())
}

func TestRewriteJSONDepthLimit(t *testing.T) {
	origin, err := url.Parse("https://open-vsx.org")
	require.NoError(t, err)
	publicBase, err := url.Parse("https://openvsx.proxy.test")
	require.NoError(t, err)

	for _, test := range []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{"maximum", maxJSONDepth, false},
		{"too_deep", maxJSONDepth + 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := strings.Repeat("[", test.depth) + `"https://open-vsx.org/api/test"` + strings.Repeat("]", test.depth)
			var output strings.Builder
			err := rewriteJSON(strings.NewReader(input), &output, origin, publicBase)
			if test.wantErr {
				require.ErrorContains(t, err, "nesting levels")
				return
			}
			require.NoError(t, err)
			require.Contains(t, output.String(), "https://openvsx.proxy.test/api/test")
		})
	}
}

func TestDocumentFillSurvivesCancellationAndCoalesces(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int32
	var signal sync.Once
	var upstreamURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		signal.Do(func() { close(started) })
		<-release
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"url":%q}`, upstreamURL+"/api/extension")
	}))
	defer upstream.Close()
	upstreamURL = upstream.URL
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	leaderContext, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/extension", nil).WithContext(leaderContext))
		leaderDone <- recorder
	}()
	<-started
	cancelLeader()

	const followers = 4
	results := make(chan *httptest.ResponseRecorder, followers)
	for range followers {
		go func() {
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/extension", nil))
			results <- recorder
		}()
	}
	close(release)
	require.Equal(t, http.StatusOK, (<-leaderDone).Code)
	for range followers {
		response := <-results
		require.Equal(t, http.StatusOK, response.Code)
		require.Contains(t, response.Body.String(), "https://openvsx.proxy.test/api/extension")
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestNoStoreDocumentIsRewrittenWithoutPersistence(t *testing.T) {
	var requests atomic.Int32
	var upstreamURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"url":%q}`, upstreamURL+"/api/extension")
	}))
	defer upstream.Close()
	upstreamURL = upstream.URL
	h := newTestHandler(t, upstream.URL, "https://openvsx.proxy.test", time.Hour)

	for range 2 {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/extension", nil))
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, "BYPASS", recorder.Header().Get("X-Cache"))
		require.Contains(t, recorder.Body.String(), "https://openvsx.proxy.test/api/extension")
	}
	require.Equal(t, int32(2), requests.Load())
}
