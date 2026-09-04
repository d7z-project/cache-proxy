package runtime

import (
	"net/http"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func TestInstancePlanNormalizesUpstream(t *testing.T) {
	plan := &InstancePlan{declaration: config.Instance{Upstream: " https://one.test/base "}}
	require.Equal(t, "https://one.test/base", plan.Upstream())
}

func TestBindPathRegistersRuntime(t *testing.T) {
	registry := prometheus.NewRegistry()
	stats := metrics.NewStats(registry)
	sched, err := scheduler.NewPersistent(filepath.Join(t.TempDir(), "scheduler.json"))
	require.NoError(t, err)
	backend := t.TempDir()
	plan := NewPlanContext(backend, stats, nil, NewSpoolBudget(8<<30), 2<<30, config.DefaultCleanupConfig(), "127.0.0.1:8080", "/metrics", sched)
	instance, err := plan.Instance(
		config.Instance{Name: "cache", Enabled: true},
		config.SelectedMode{Name: "cache", Mode: config.ModeFile, Enabled: true},
	)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(backend, "instances", "cache", "file"), instance.StoreRoot())
	t.Cleanup(plan.CloseStores)

	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	require.NoError(t, instance.BindPath("/cache", HandlerInstance{Handler: handler}))
	result, err := plan.Finalize()
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	require.Equal(t, "/cache", result.Entries[0].Path)
	require.NotNil(t, result.Entries[0].Runtime)
}
