package runtime

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

type testResolver struct{}

func (testResolver) Resolve(*http.Request) (httpcache.Route, error) {
	return httpcache.Route{}, nil
}

func TestInstancePlanNormalizesUpstreams(t *testing.T) {
	plan := &InstancePlan{decl: config.Instance{Upstreams: []string{" https://one.test/base ", "https://two.test"}}}
	require.Equal(t, []string{"https://one.test/base", "https://two.test"}, plan.Upstreams())
}

func TestBindHTTPPathRegistersCleanupAndRuntime(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	registry := prometheus.NewRegistry()
	stats := httpcache.NewStats(registry)
	sched := scheduler.New(store, registry)
	plan := NewPlanContext(store, stats, nil, config.DefaultCleanupConfig(), "127.0.0.1:8080", "/metrics", sched)
	instance, err := plan.Instance(
		config.Instance{Name: "cache", Enabled: true},
		config.SelectedMode{Name: "cache", Mode: config.ModeFile, Enabled: true},
	)
	require.NoError(t, err)

	handler := httpcache.NewHandler("cache", httpcache.RuntimeConfig{
		Mode:        config.ModeFile,
		ExpireAfter: config.DefaultRetention,
	}, store, testResolver{}, stats)
	t.Cleanup(func() { require.NoError(t, handler.CloseContext(context.Background())) })

	require.NoError(t, instance.BindHTTPPath("/cache", config.DefaultRetention, handler))
	result, err := plan.Finalize()
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	require.Equal(t, "/cache", result.Entries[0].Path)
	require.NotNil(t, result.Entries[0].Runtime)

	info, ok := sched.Info(scheduler.NewTaskKey("cache", scheduler.TypeExpireCleanup, ""))
	require.True(t, ok)
	require.Equal(t, httpCacheCleanupInterval, info.Interval)
}
