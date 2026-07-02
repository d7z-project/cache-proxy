package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

func TestPreStartFactorySupportsRestore(t *testing.T) {
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)

	{
		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store, reg)
		sched.RegisterFactory(TaskFactory{
			Instance:        "repo",
			RefreshInterval: time.Hour,
			GCInterval:      2 * time.Hour,
			NewRefresh:      func(string) TaskHandler { return func(context.Context) error { return nil } },
			NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
		})
		ctx, cancel := context.WithCancel(context.Background())
		sched.Start(ctx)
		b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", RootID: "root"}})
		require.Eventually(t, func() bool {
			_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "root"))
			return ok
		}, time.Second, 10*time.Millisecond)
		cancel()
		require.NoError(t, sched.Stop(context.Background()))
		require.NoError(t, store.Close())
	}

	store, err = blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	sched := New(b, store, reg)
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: time.Hour,
		GCInterval:      2 * time.Hour,
		NewRefresh:      func(string) TaskHandler { return func(context.Context) error { return nil } },
		NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "root"))
		return ok
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestPersistenceRestoreSkipsMissingFactory(t *testing.T) {
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)

	{
		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store, reg)
		sched.RegisterFactory(TaskFactory{
			Instance:        "ghost",
			RefreshInterval: time.Hour,
			GCInterval:      time.Hour,
			NewRefresh:      func(string) TaskHandler { return func(context.Context) error { return nil } },
			NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
		})
		ctx, cancel := context.WithCancel(context.Background())
		sched.Start(ctx)
		b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "ghost", RootID: "x"}})
		require.Eventually(t, func() bool {
			_, ok := sched.Info(NewTaskKey("ghost", TypeMetadataRefresh, "x"))
			return ok
		}, time.Second, 10*time.Millisecond)
		cancel()
		require.NoError(t, sched.Stop(context.Background()))
		require.NoError(t, store.Close())
	}

	store, err = blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	sched := New(b, store, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("ghost", TypeMetadataRefresh, "x"))
		return !ok
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestRestoreMetrics(t *testing.T) {
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)

	{
		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store, reg)
		sched.RegisterFactory(TaskFactory{
			Instance:        "repo",
			RefreshInterval: time.Hour,
			GCInterval:      time.Hour,
			NewRefresh:      func(string) TaskHandler { return func(context.Context) error { return nil } },
			NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
		})
		ctx, cancel := context.WithCancel(context.Background())
		sched.Start(ctx)
		b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", RootID: "root"}})
		require.Eventually(t, func() bool {
			return metricValue(t, sched.m.registered.WithLabelValues("repo", string(TypeMetadataRefresh), "discovery")) == 1
		}, time.Second, 10*time.Millisecond)
		cancel()
		require.NoError(t, sched.Stop(context.Background()))
		require.NoError(t, store.Close())
	}

	store, err = blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	sched := New(b, store, reg)
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh:      func(string) TaskHandler { return func(context.Context) error { return nil } },
		NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	require.Eventually(t, func() bool {
		return metricValue(t, sched.m.stateRestore.WithLabelValues("success")) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1), metricValue(t, sched.m.restoredTasks.WithLabelValues(string(TypeMetadataRefresh))))
	require.Equal(t, float64(1), metricValue(t, sched.m.restoredTasks.WithLabelValues(string(TypeMetadataGC))))
	require.NoError(t, sched.Stop(context.Background()))
}
