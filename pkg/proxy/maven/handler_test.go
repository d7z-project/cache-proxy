package maven

import (
	"context"
	"fmt"
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
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
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
