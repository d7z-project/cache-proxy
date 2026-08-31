package file

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestHandlerRecordsUpstreamFailures(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	stats := httpcache.NewStats(prometheus.NewRegistry())
	cfg := health.DefaultConfig()
	sh := health.New("files", config.ModeFile, cfg, []string{upstream.URL}, stats)
	base := httpcache.NewHandler("files", httpcache.RuntimeConfig{
		Mode:        config.ModeFile,
		ExpireAfter: config.DefaultExpireAfter,
		Upstreams:   []string{upstream.URL},
		BusyPolicy:  config.BusyPolicyBypass,
	}, store, fileResolver{policy: &Policy{DefaultPolicy: config.PolicyBypass}}, stats, sh)
	defer func() { require.NoError(t, base.CloseContext(context.Background())) }()

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/object.txt", nil)
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, req)
		require.Equal(t, http.StatusInternalServerError, rec.Code)
	}

	require.Equal(t, float64(1), stats.Snapshot().Instances["files"].Upstreams[upstream.URL].ErrorRate)
}
