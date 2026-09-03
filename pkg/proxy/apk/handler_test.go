package apk

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	more, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
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

func TestAPKRecognizesSupportedIndexFormats(t *testing.T) {
	for _, requestPath := range []string{"v2/main/x86_64/APKINDEX.tar.gz", "v3.21/main/x86_64/APKINDEX.tar.gz", "edge/main/x86_64/Packages.adb"} {
		require.True(t, isAPKIndexPath(requestPath), requestPath)
	}
	require.False(t, isAPKIndexPath("v3.21/main/x86_64/package.apk"))
	for _, requestPath := range []string{"package.apk", "package.apk.sig", "package.apk.sha256"} {
		require.True(t, isAPKArtifactPath(requestPath), requestPath)
	}
	for _, requestPath := range []string{"", "packages/", "index.html", "APKINDEX.tar.gz", "package.apk/"} {
		require.False(t, isAPKArtifactPath(requestPath), requestPath)
	}
	require.False(t, isAPKIndexPath("APKINDEX.tar.gz/"))
}

func TestAPKTransparentResourcesBypassCacheAndPreserveQuery(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, request.URL.RequestURI())
	}))
	defer server.Close()
	h := newAPKTestHandler(t, server.URL+"/repository")

	for range 2 {
		for _, target := range []string{"/", "/browse/", "/assets/site.css?theme=dark"} {
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
		}
	}
	require.Equal(t, int32(6), requests.Load())
}

func TestAPKArtifactQuerySeparatesCacheEntriesAndIndexQueryBypasses(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, request.URL.RawQuery)
	}))
	defer server.Close()
	h := newAPKTestHandler(t, server.URL)

	for index, target := range []string{"/pkg.apk?token=one", "/pkg.apk?token=two", "/pkg.apk?token=one"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusOK, response.Code)
		if index == 2 {
			require.Contains(t, []string{"HIT", "COALESCED"}, response.Header().Get("X-Cache"))
		}
	}
	for range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/APKINDEX.tar.gz?mirror=one", nil))
		require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
	}
	require.Equal(t, int32(4), requests.Load())
}

func TestAPKNoQueryCacheKeyRemainsStable(t *testing.T) {
	origin, err := url.Parse("https://packages.example/repository")
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/pkg.apk", nil)
	request.Header.Set("Accept-Encoding", "gzip")

	digest := sha256.Sum256([]byte(origin.String() + "\x00pkg.apk\x00gzip"))
	require.Equal(t, "refs/"+hex.EncodeToString(digest[:]), artifactKey(origin, "pkg.apk", request))
	request.URL.RawQuery = "token=one"
	require.NotEqual(t, "refs/"+hex.EncodeToString(digest[:]), artifactKey(origin, "pkg.apk", request))
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
