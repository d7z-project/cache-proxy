package maven

import (
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
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestMavenReadOnlyCacheContract(t *testing.T) {
	var gets atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			w.WriteHeader(http.StatusCreated)
			return
		}
		count := gets.Add(1)
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, count))
		_, _ = w.Write([]byte("artifact"))
	}))
	t.Cleanup(upstream.Close)
	h := newMavenTestHandler(t, upstream.URL)

	for index := range 2 {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/org/example/tool/1.0/tool-1.0.jar", nil))
		require.Equal(t, http.StatusOK, recorder.Code)
		if index == 0 {
			require.Equal(t, "MISS", recorder.Header().Get("X-Cache"))
		} else {
			require.Contains(t, []string{"HIT", "COALESCED"}, recorder.Header().Get("X-Cache"))
		}
		require.Equal(t, "artifact", recorder.Body.String())
	}
	require.Equal(t, int32(1), gets.Load())

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/org/example/tool/1.0/tool-1.0.jar", strings.NewReader("new")))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/org/example/tool/1.0/tool-1.0.jar", nil))
	require.Equal(t, "HIT", recorder.Header().Get("X-Cache"))
	require.Equal(t, int32(1), gets.Load())
}

func TestMavenMetadataPublishesUpstreamUpdate(t *testing.T) {
	var revision atomic.Int32
	revision.Store(1)
	var requests atomic.Int32
	var conditional atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		conditional.Store(request.Header.Get("If-None-Match"))
		current := revision.Load()
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		_, _ = fmt.Fprintf(w, "<metadata><versioning><latest>%d.0</latest></versioning></metadata>", current)
	}))
	t.Cleanup(upstream.Close)
	h := newMavenTestHandler(t, upstream.URL)
	target := "/org/example/tool/maven-metadata.xml"
	key := mavenKey(h.origin, strings.TrimPrefix(target, "/"), httptest.NewRequest(http.MethodGet, target, nil))
	cachedBody := func() string {
		object, err := storeio.OpenResponse(context.Background(), h.store, mavenTenant, key)
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
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, target, nil))
	require.Contains(t, first.Body.String(), "<latest>1.0</latest>")
	require.Eventually(t, func() bool { return strings.Contains(cachedBody(), "<latest>1.0</latest>") }, time.Second, time.Millisecond)

	revision.Store(2)
	request := httptest.NewRequest(http.MethodGet, target, nil)
	request.Header.Set("Cache-Control", "no-cache")
	second := httptest.NewRecorder()
	h.ServeHTTP(second, request)
	require.Equal(t, http.StatusOK, second.Code)
	require.Contains(t, second.Body.String(), "<latest>2.0</latest>")
	require.Equal(t, "REFRESH", second.Header().Get("X-Cache"))
	require.Equal(t, `"v1"`, conditional.Load())
	require.Eventually(t, func() bool { return strings.Contains(cachedBody(), "<latest>2.0</latest>") }, time.Second, time.Millisecond)

	third := httptest.NewRecorder()
	h.ServeHTTP(third, httptest.NewRequest(http.MethodGet, target, nil))
	require.Contains(t, third.Body.String(), "<latest>2.0</latest>")
	require.Equal(t, "HIT", third.Header().Get("X-Cache"))
	require.Equal(t, int32(2), requests.Load())
}

func newMavenTestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("test", "maven", nil, proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 8, MaxActivePerHost: 8}), stats)
	require.NoError(t, err)
	h := newHandler(origin, t.TempDir(), store, client)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
