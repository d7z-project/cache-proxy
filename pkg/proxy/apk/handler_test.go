package apk

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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

func TestAPKReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	h := newAPKTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/pkg.apk", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestAPKIndexAndPackageHaveIndependentRefs(t *testing.T) {
	indexBytes := makeAPKIndex(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		switch request.URL.Path {
		case "/v3.21/main/x86_64/APKINDEX.tar.gz":
			_, _ = w.Write(indexBytes)
		case "/v3.21/main/x86_64/pkg.apk":
			_, _ = w.Write([]byte("package bytes"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newAPKTestHandler(t, server.URL+"/v3.21/main")

	for _, target := range []string{"/x86_64/APKINDEX.tar.gz", "/x86_64/pkg.apk"} {
		for index := range 2 {
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
			require.Equal(t, http.StatusOK, response.Code)
			if index == 0 {
				require.Equal(t, "MISS", response.Header().Get("X-Cache"))
				if target == "/x86_64/APKINDEX.tar.gz" {
					_, err := h.metadata.Refresh(context.Background(), 10)
					require.NoError(t, err)
				}
			} else {
				require.Contains(t, []string{"HIT", "COALESCED"}, response.Header().Get("X-Cache"))
			}
		}
	}
	require.Equal(t, int32(2), requests.Load())
}

func makeAPKIndex(t *testing.T) []byte {
	t.Helper()
	var output bytes.Buffer
	compressed := gzip.NewWriter(&output)
	archive := tar.NewWriter(compressed)
	content := []byte("P:pkg\nV:1.0-r0\n\n")
	require.NoError(t, archive.WriteHeader(&tar.Header{Name: "APKINDEX", Mode: 0o644, Size: int64(len(content))}))
	_, err := archive.Write(content)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	require.NoError(t, compressed.Close())
	return output.Bytes()
}

func TestAPKDirectIndexUsesExactConfiguredURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/indexes/custom.ndx", request.URL.Path)
		_, _ = w.Write([]byte("direct index"))
	}))
	defer server.Close()
	h := newAPKTestHandler(t, server.URL+"/indexes/custom.ndx")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "direct index", response.Body.String())
}

func TestAPKRecognizesV2V3AndADBIndexes(t *testing.T) {
	for _, requestPath := range []string{"v2/main/x86_64/APKINDEX.tar.gz", "v3.21/main/x86_64/APKINDEX.tar.gz", "edge/main/x86_64/Packages.adb", directIndexMarker} {
		require.True(t, isIndexRequest(requestPath), requestPath)
	}
	require.False(t, isIndexRequest("v3.21/main/x86_64/package.apk"))
}

func TestAPKArtifactRevalidationPublishesChangedResponse(t *testing.T) {
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
	h := newAPKTestHandler(t, server.URL)
	origin, err := url.Parse(server.URL)
	require.NoError(t, err)
	key := apkKey(origin, "pkg.apk", httptest.NewRequest(http.MethodGet, "/pkg.apk", nil))
	cachedBody := func() string {
		object, err := storeio.OpenResponse(context.Background(), h.store, apkTenant, key)
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
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/pkg.apk", nil))
	require.Equal(t, "v1", first.Body.String())
	require.Eventually(t, func() bool { return cachedBody() == "v1" }, time.Second, time.Millisecond)
	revalidate := httptest.NewRequest(http.MethodGet, "/pkg.apk", nil)
	revalidate.Header.Set("Cache-Control", "no-cache")
	second := httptest.NewRecorder()
	h.ServeHTTP(second, revalidate)
	require.Equal(t, "v2", second.Body.String())
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Equal(t, `"v1"`, <-conditional)

	require.Eventually(t, func() bool { return cachedBody() == "v2" }, time.Second, time.Millisecond)
	third := httptest.NewRecorder()
	h.ServeHTTP(third, httptest.NewRequest(http.MethodGet, "/pkg.apk", nil))
	require.Equal(t, "v2", third.Body.String())
	require.Equal(t, "HIT", third.Header().Get("X-Cache"))
	require.Equal(t, int32(2), requests.Load())
}

func newAPKTestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("apk", "apk", nil, nil, stats)
	require.NoError(t, err)
	root := t.TempDir()
	h, err := newHandler("apk", root+"/state", origin, root+"/work", store, client, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
