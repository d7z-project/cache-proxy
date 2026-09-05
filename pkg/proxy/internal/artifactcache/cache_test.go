package artifactcache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestCacheCoalescesAndRefreshesArtifacts(t *testing.T) {
	var requests atomic.Int32
	var revision atomic.Int32
	var failureStatus atomic.Int32
	var blockFailure atomic.Bool
	firstRequest := make(chan struct{})
	releaseFirst := make(chan struct{})
	failureStarted := make(chan struct{}, 1)
	releaseFailure := make(chan struct{})
	revision.Store(1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestNumber := requests.Add(1)
		if requestNumber == 1 {
			close(firstRequest)
			<-releaseFirst
		}
		if status := failureStatus.Load(); status != 0 {
			if blockFailure.Load() {
				failureStarted <- struct{}{}
				<-releaseFailure
			}
			http.Error(w, "unavailable", int(status))
			return
		}
		current := revision.Load()
		etag := fmt.Sprintf(`"revision-%d"`, current)
		if request.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = fmt.Fprintf(w, "artifact-%d", current)
	}))
	t.Cleanup(upstream.Close)

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	lifecycle := storeio.NewLifecycle()
	t.Cleanup(func() { require.NoError(t, lifecycle.Close(context.Background())) })
	cache := Cache{
		Tenant: "test", Upstream: upstream.URL, Freshness: time.Hour,
		Store: store, Spooler: storeio.NewSpooler(t.TempDir(), 1<<20, proxyruntime.NewSpoolBudget(8<<20)),
		Lifecycle: lifecycle, Flights: &storeio.FlightGroup{},
		CacheKey: func(string, *http.Request) string { return "artifact" },
		FetchUpstream: func(ctx context.Context, method, requestPath, rawQuery string, header http.Header) (*http.Response, error) {
			target, err := url.Parse(upstream.URL + "/" + requestPath)
			if err != nil {
				return nil, err
			}
			target.RawQuery = rawQuery
			request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
			if err != nil {
				return nil, err
			}
			transport.CopyReadRequestHeaders(request.Header, header)
			return upstream.Client().Do(request)
		},
	}

	const clients = 8
	responses := make([]*httptest.ResponseRecorder, clients)
	var group sync.WaitGroup
	group.Add(clients)
	for i := range clients {
		go func() {
			defer group.Done()
			responses[i] = httptest.NewRecorder()
			cache.Serve(responses[i], httptest.NewRequest(http.MethodGet, "/package.rpm", nil), "package.rpm")
		}()
	}
	<-firstRequest
	time.Sleep(20 * time.Millisecond)
	close(releaseFirst)
	group.Wait()
	require.Equal(t, int32(1), requests.Load())
	for _, response := range responses {
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "artifact-1", response.Body.String())
		require.Contains(t, []string{"MISS", "COALESCED", "HIT"}, response.Header().Get("X-Cache"))
	}

	revalidate := httptest.NewRequest(http.MethodGet, "/package.rpm", nil)
	revalidate.Header.Set("Cache-Control", "no-cache")
	response := httptest.NewRecorder()
	cache.Serve(response, revalidate, "package.rpm")
	require.Equal(t, "artifact-1", response.Body.String())
	require.Equal(t, "REVALIDATED", response.Header().Get("X-Cache"))

	revision.Store(2)
	revalidate = httptest.NewRequest(http.MethodGet, "/package.rpm", nil)
	revalidate.Header.Set("Cache-Control", "no-cache")
	response = httptest.NewRecorder()
	cache.Serve(response, revalidate, "package.rpm")
	require.Equal(t, "artifact-2", response.Body.String())
	require.Equal(t, "REFRESH", response.Header().Get("X-Cache"))
	require.Eventually(t, func() bool {
		object, err := storeio.OpenResponse(context.Background(), store, "test", "artifact")
		if err != nil {
			return false
		}
		defer func() { _ = object.Reader.Close() }()
		body, err := io.ReadAll(object.Reader)
		return err == nil && string(body) == "artifact-2"
	}, time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		revalidate = httptest.NewRequest(http.MethodGet, "/package.rpm", nil)
		revalidate.Header.Set("Cache-Control", "no-cache")
		response = httptest.NewRecorder()
		cache.Serve(response, revalidate, "package.rpm")
		return response.Body.String() == "artifact-2" && response.Header().Get("X-Cache") == "REVALIDATED"
	}, time.Second, time.Millisecond)

	failureStatus.Store(http.StatusTooManyRequests)
	blockFailure.Store(true)
	requestsBeforeFailure := requests.Load()
	staleResponses := make([]*httptest.ResponseRecorder, 2)
	group.Add(len(staleResponses))
	for i := range staleResponses {
		go func() {
			defer group.Done()
			request := httptest.NewRequest(http.MethodGet, "/package.rpm", nil)
			request.Header.Set("Cache-Control", "no-cache")
			staleResponses[i] = httptest.NewRecorder()
			cache.Serve(staleResponses[i], request, "package.rpm")
		}()
	}
	<-failureStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseFailure)
	group.Wait()
	require.Equal(t, requestsBeforeFailure+1, requests.Load())
	for _, stale := range staleResponses {
		require.Contains(t, []int{http.StatusTooManyRequests, http.StatusBadGateway}, stale.Code)
	}

	blockFailure.Store(false)
	failureStatus.Store(http.StatusServiceUnavailable)
	revalidate = httptest.NewRequest(http.MethodGet, "/package.rpm", nil)
	revalidate.Header.Set("Cache-Control", "no-cache")
	response = httptest.NewRecorder()
	cache.Serve(response, revalidate, "package.rpm")
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}
