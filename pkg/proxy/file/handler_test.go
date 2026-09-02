package file

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

func newTestHandler(t *testing.T, upstream string, rules []Rule) (*handler, *blobfs.Store) {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("files", "file", nil, nil, stats)
	require.NoError(t, err)
	handler, err := newHandler(handlerConfig{
		name: "files", upstream: upstream, rules: rules, workDir: root + "/work",
		store: store, client: client, stats: stats,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, handler.CloseContext(context.Background()))
		_ = store.Close()
	})
	return handler, store
}

func TestHTTPFileCacheRevalidatesAndHandlesClientCondition(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Cache-Control", "max-age=0")
		if request.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, "payload")
	}))
	defer upstream.Close()
	handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/artifact", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "payload", first.Body.String())

	require.Eventually(t, func() bool {
		reader, err := openStored(context.Background(), handler.store, cacheKey(handler.origin, "artifact", httptest.NewRequest(http.MethodGet, "/artifact", nil)))
		if err == nil {
			_ = reader.reader.Close()
			return true
		}
		return false
	}, time.Second, time.Millisecond)

	second := httptest.NewRecorder()
	secondRequest := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	secondRequest.Header.Set("Cache-Control", "max-age=0")
	handler.ServeHTTP(second, secondRequest)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "payload", second.Body.String())
	require.Equal(t, int32(2), requests.Load())

	conditional := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	conditional.Header.Set("If-None-Match", `"v1"`)
	third := httptest.NewRecorder()
	handler.ServeHTTP(third, conditional)
	require.Equal(t, http.StatusNotModified, third.Code)
}

func TestFileReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer upstream.Close()
	handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/artifact", strings.NewReader("content")))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Equal(t, "REJECTED", response.Header().Get("X-Cache"))
	require.Zero(t, requests.Load())
}

func TestFileConcurrentMissUsesSingleTransfer(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Cache-Control", "max-age=3600")
		time.Sleep(25 * time.Millisecond)
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer upstream.Close()
	handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

	var wait sync.WaitGroup
	errors := make(chan error, 32)
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/large", nil))
			if response.Code != http.StatusOK || response.Body.Len() != 4096 {
				errors <- io.ErrUnexpectedEOF
			}
		}()
	}
	wait.Wait()
	close(errors)
	require.Empty(t, errors)
	require.Equal(t, int32(1), requests.Load())
}

func TestFileCredentialsBypassAndCachedRangeUsesCompleteObject(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Cache-Control", "max-age=3600")
		_, _ = io.WriteString(w, "0123456789")
	}))
	defer upstream.Close()
	handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/artifact", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Eventually(t, func() bool {
		object, err := openStored(context.Background(), handler.store, cacheKey(handler.origin, "artifact", httptest.NewRequest(http.MethodGet, "/artifact", nil)))
		if err != nil {
			return false
		}
		_ = object.reader.Close()
		return true
	}, time.Second, time.Millisecond)

	rangeRequest := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	rangeRequest.Header.Set("Range", "bytes=2-5")
	ranged := httptest.NewRecorder()
	handler.ServeHTTP(ranged, rangeRequest)
	require.Equal(t, http.StatusPartialContent, ranged.Code)
	require.Equal(t, "2345", ranged.Body.String())
	require.Equal(t, int32(1), requests.Load())

	credentialed := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	credentialed.Header.Set("Authorization", "Bearer private")
	private := httptest.NewRecorder()
	handler.ServeHTTP(private, credentialed)
	require.Equal(t, http.StatusOK, private.Code)
	require.Equal(t, "BYPASS", private.Header().Get("X-Cache"))
	require.Equal(t, int32(2), requests.Load())
}
