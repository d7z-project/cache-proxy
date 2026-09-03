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
	"strings"
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
	indexBytes := makeAPKIndex(t, "1.0-r0")
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

func makeAPKIndex(t *testing.T, version string) []byte {
	t.Helper()
	var output bytes.Buffer
	compressed := gzip.NewWriter(&output)
	archive := tar.NewWriter(compressed)
	content := []byte("P:pkg\nV:" + version + "\n\n")
	require.NoError(t, archive.WriteHeader(&tar.Header{Name: "APKINDEX", Mode: 0o644, Size: int64(len(content))}))
	_, err := archive.Write(content)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	require.NoError(t, compressed.Close())
	return output.Bytes()
}

func TestAPKIndexPublishesUpstreamGenerationUpdate(t *testing.T) {
	indexes := [][]byte{makeAPKIndex(t, "1.0-r0"), makeAPKIndex(t, "2.0-r0")}
	var revision atomic.Int32
	revision.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/v3.21/main/x86_64/APKINDEX.tar.gz", request.URL.Path)
		current := int(revision.Load() - 1)
		w.Header().Set("ETag", fmt.Sprintf(`"index-v%d"`, current+1))
		_, _ = w.Write(indexes[current])
	}))
	defer server.Close()
	h := newAPKTestHandler(t, server.URL)
	target := "/v3.21/main/x86_64/APKINDEX.tar.gz"
	rootID := "apk:" + server.URL + ":" + strings.TrimPrefix(target, "/")

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, target, nil))
	require.Equal(t, indexes[0], first.Body.Bytes())
	_, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := h.metadata.Current(rootID)
	require.NotNil(t, previous)

	revision.Store(2)
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := h.metadata.Current(rootID)
	require.NotNil(t, current)
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "HIT", response.Header().Get("X-Cache"))
	require.Equal(t, indexes[1], response.Body.Bytes())
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
		defer func() { _ = object.Reader.Close() }()
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
	select {
	case validator := <-conditional:
		require.Equal(t, `"v1"`, validator)
	case <-time.After(time.Second):
		t.Fatal("artifact revalidation did not send a conditional request")
	}

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
