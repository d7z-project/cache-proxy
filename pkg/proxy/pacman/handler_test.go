package pacman

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestPacmanReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	h := newPacmanTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/pkg.pkg.tar.zst", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestPacmanDatabaseNamesAndCompressionAliases(t *testing.T) {
	for _, name := range []string{"custom.db", "custom.files", "custom.db.sig", "custom.db.tar.zst", "custom.files.tar.xz.sig", "repo-name.db.lz4"} {
		require.True(t, isIndexRequest(name), name)
	}
	for _, name := range []string{"package.pkg.tar.zst", "package.pkg.tar.zst.sig", "contains.db.backup", ".db"} {
		require.False(t, isIndexRequest(name), name)
	}
}

func TestPacmanDatabaseAndSignatureCacheSeparately(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(request.URL.Path))
	}))
	defer server.Close()
	h := newPacmanTestHandler(t, server.URL)

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/custom.db", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "MISS", first.Header().Get("X-Cache"))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	for _, target := range []string{"/custom.db", "/custom.db.sig"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
	}
	require.Equal(t, int32(2), requests.Load())
}

func TestPacmanArtifactRevalidationPublishesChangedResponse(t *testing.T) {
	var requests atomic.Int32
	conditional := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := requests.Add(1)
		if current == 2 {
			conditional <- request.Header.Get("If-None-Match")
		}
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		_, _ = fmt.Fprintf(w, "v%d", current)
	}))
	defer server.Close()
	h := newPacmanTestHandler(t, server.URL)
	origin, err := url.Parse(server.URL)
	require.NoError(t, err)
	key := pacmanKey(origin, "pkg.pkg.tar.zst", httptest.NewRequest(http.MethodGet, "/pkg.pkg.tar.zst", nil))
	cachedBody := func() string {
		object, err := storeio.OpenResponse(context.Background(), h.store, pacmanTenant, key)
		if err != nil {
			return ""
		}
		defer object.Reader.Close()
		body, err := io.ReadAll(object.Reader)
		if err != nil {
			return ""
		}
		return string(body)
	}

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/pkg.pkg.tar.zst", nil))
	require.Equal(t, "v1", first.Body.String())
	require.Eventually(t, func() bool { return cachedBody() == "v1" }, time.Second, time.Millisecond)
	revalidate := httptest.NewRequest(http.MethodGet, "/pkg.pkg.tar.zst", nil)
	revalidate.Header.Set("Cache-Control", "no-cache")
	second := httptest.NewRecorder()
	h.ServeHTTP(second, revalidate)
	require.Equal(t, "v2", second.Body.String())
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Equal(t, `"v1"`, <-conditional)

	require.Eventually(t, func() bool { return cachedBody() == "v2" }, time.Second, time.Millisecond)
	third := httptest.NewRecorder()
	h.ServeHTTP(third, httptest.NewRequest(http.MethodGet, "/pkg.pkg.tar.zst", nil))
	require.Equal(t, "v2", third.Body.String())
	require.Equal(t, "HIT", third.Header().Get("X-Cache"))
	require.Equal(t, int32(2), requests.Load())
}

func newPacmanTestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("pacman", "pacman", nil, nil, stats)
	require.NoError(t, err)
	root := t.TempDir()
	h, err := newHandler("pacman", root+"/state", origin, root+"/work", store, client, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
