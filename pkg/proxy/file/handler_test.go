package file

import (
	"context"
	"fmt"
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
	var revision atomic.Int32
	revision.Store(1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		current := revision.Load()
		etag := fmt.Sprintf(`"v%d"`, current)
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "max-age=3600")
		if request.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprintf(w, "payload-v%d", current)
	}))
	defer upstream.Close()
	handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/artifact", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "payload-v1", first.Body.String())

	key := cacheKey(handler.origin, "artifact", httptest.NewRequest(http.MethodGet, "/artifact", nil))
	require.Eventually(t, func() bool {
		reader, err := openStored(context.Background(), handler.store, key)
		if err == nil {
			_ = reader.reader.Close()
			return true
		}
		return false
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		flight, leader := handler.flights.Begin(key)
		if leader {
			handler.flights.Finish(key, flight, nil)
		}
		return leader
	}, time.Second, time.Millisecond)

	second := httptest.NewRecorder()
	secondRequest := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	revision.Store(2)
	secondRequest.Header.Set("Cache-Control", "no-cache")
	handler.ServeHTTP(second, secondRequest)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "payload-v2", second.Body.String())
	require.Equal(t, `"v2"`, second.Header().Get("ETag"))
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Equal(t, int32(2), requests.Load())
	require.Eventually(t, func() bool {
		reader, err := openStored(context.Background(), handler.store, key)
		if err != nil {
			return false
		}
		defer func() { _ = reader.reader.Close() }()
		body, err := io.ReadAll(reader.reader)
		return err == nil && string(body) == "payload-v2"
	}, time.Second, time.Millisecond)

	conditional := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	conditional.Header.Set("If-None-Match", `"v2"`)
	third := httptest.NewRecorder()
	handler.ServeHTTP(third, conditional)
	require.Equal(t, http.StatusNotModified, third.Code)
	require.Equal(t, int32(2), requests.Load())
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

func TestFileConcurrentRevalidationUsesSingleTransfer(t *testing.T) {
	var requests atomic.Int32
	var revision atomic.Int32
	revision.Store(1)
	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	var updateOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseUpdate) }) })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		current := revision.Load()
		state := "initial"
		if current == 2 {
			state = "updated"
		}
		etag := fmt.Sprintf(`"%s"`, state)
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "max-age=3600")
		if current == 2 {
			updateOnce.Do(func() { close(updateStarted) })
			<-releaseUpdate
		}
		if request.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, state)
	}))
	defer upstream.Close()
	handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/artifact", nil))
	require.Equal(t, http.StatusOK, first.Code)
	key := cacheKey(handler.origin, "artifact", httptest.NewRequest(http.MethodGet, "/artifact", nil))
	require.Eventually(t, func() bool {
		object, err := openStored(context.Background(), handler.store, key)
		if err != nil {
			return false
		}
		defer func() { _ = object.reader.Close() }()
		body, err := io.ReadAll(object.reader)
		return err == nil && string(body) == "initial"
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		flight, leader := handler.flights.Begin(key)
		if leader {
			handler.flights.Finish(key, flight, nil)
		}
		return leader
	}, time.Second, time.Millisecond)

	revision.Store(2)
	results := make(chan *httptest.ResponseRecorder, 2)
	requestUpdate := func() {
		request := httptest.NewRequest(http.MethodGet, "/artifact", nil)
		request.Header.Set("Cache-Control", "no-cache")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		results <- response
	}
	go requestUpdate()
	select {
	case <-updateStarted:
	case <-time.After(time.Second):
		t.Fatal("revalidation did not reach upstream")
	}
	go requestUpdate()
	require.Never(t, func() bool { return len(results) != 0 }, 25*time.Millisecond, time.Millisecond)
	releaseOnce.Do(func() { close(releaseUpdate) })

	cacheResults := map[string]int{}
	for range 2 {
		select {
		case response := <-results:
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "updated", response.Body.String())
			cacheResults[response.Header().Get("X-Cache")]++
		case <-time.After(time.Second):
			t.Fatal("revalidation request did not finish")
		}
	}
	require.Equal(t, map[string]int{"REFRESH": 1, "COALESCED": 1}, cacheResults)
	require.Equal(t, int32(2), requests.Load())
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
