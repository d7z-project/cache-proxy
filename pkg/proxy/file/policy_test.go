package file

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestHandlerStartsHealthAndRecordsUpstreamFailures(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	cfg := health.DefaultConfig()
	sh := health.New("files", config.ModeFile, cfg, []string{upstream.URL}, stats, "cache-proxy-test")
	base := httpcache.NewHandler("files", httpcache.RuntimeConfig{
		Mode:        config.ModeFile,
		ExpireAfter: config.DefaultExpireAfter,
		Upstreams:   []string{upstream.URL},
		BusyPolicy:  config.BusyPolicyBypass,
	}, store, fileResolver{policy: &Policy{DefaultPolicy: config.PolicyBypass}}, stats, sh)
	h := &handler{base: base, sh: sh}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, h.Start(ctx))
	defer func() { require.NoError(t, h.Stop(context.Background())) }()

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/object.txt", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	}

	require.Equal(t, health.StateUnhealthy, sh.AggregateState())
}

func TestHandlerStopClosesHealthBeforeBase(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	sh := health.New("files", config.ModeFile, health.DefaultConfig(), []string{"https://example.com"}, stats, "ua")
	base := httpcache.NewHandler("files", httpcache.RuntimeConfig{
		Mode:        config.ModeFile,
		ExpireAfter: config.Expiration(time.Hour),
		Upstreams:   []string{"https://example.com"},
	}, store, fileResolver{policy: &Policy{DefaultPolicy: config.PolicyBypass}}, stats, sh)
	h := &handler{base: base, sh: sh}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, h.Start(ctx))
	require.NoError(t, h.Stop(context.Background()))
}
