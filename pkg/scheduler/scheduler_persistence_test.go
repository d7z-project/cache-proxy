package scheduler

import (
	"context"
	"io/fs"
	"path"
	"strings"
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
			NewRefresh:      func(string) TaskHandler { return noopTask },
			NewGC:           func(string) TaskHandler { return noopTask },
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
		NewRefresh:      func(string) TaskHandler { return noopTask },
		NewGC:           func(string) TaskHandler { return noopTask },
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
			NewRefresh:      func(string) TaskHandler { return noopTask },
			NewGC:           func(string) TaskHandler { return noopTask },
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
			NewRefresh:      func(string) TaskHandler { return noopTask },
			NewGC:           func(string) TaskHandler { return noopTask },
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
		NewRefresh:      func(string) TaskHandler { return noopTask },
		NewGC:           func(string) TaskHandler { return noopTask },
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

func TestLoadTaskStateTreatsOnlyMissingFileAsEmpty(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	data, err := loadTaskState(store, "_scheduler")
	require.NoError(t, err)
	require.Empty(t, data)

	require.NoError(t, store.MkdirAll("_scheduler/", 0o755))
	_, err = store.Put(context.Background(), "_scheduler", "tasks.yaml", strings.NewReader("tasks: ["), nil)
	require.NoError(t, err)

	data, err = loadTaskState(store, "_scheduler")
	require.Error(t, err)
	require.Empty(t, data)
}

func TestSaveStatePublishesThroughTempAndCleansTemps(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	reg := prometheus.NewRegistry()
	sched := New(bus.NewWithRegisterer(reg), store, reg)
	key := NewTaskKey("repo", TypeMetadataRefresh, "root")
	sched.tasks[key] = &taskState{
		TaskInfo: TaskInfo{
			Key:      key,
			Status:   StatusIdle,
			NextRun:  time.Now().Add(time.Hour),
			Interval: time.Hour,
		},
		handler: noopTask,
		index:   -1,
	}

	sched.saveState()

	data, err := loadTaskState(store, "_scheduler")
	require.NoError(t, err)
	require.Len(t, data, 1)
	require.Equal(t, "repo", data[0].Instance)
	require.False(t, schedulerStateTempExists(t, store))
}

func TestCleanStateTempsRemovesStaleSchedulerTemps(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.MkdirAll("_scheduler/", 0o755))
	_, err = store.Put(context.Background(), "_scheduler", "tasks.yaml.tmp.old", strings.NewReader("old"), nil)
	require.NoError(t, err)
	require.True(t, schedulerStateTempExists(t, store))

	reg := prometheus.NewRegistry()
	New(bus.NewWithRegisterer(reg), store, reg).cleanStateTemps()

	require.False(t, schedulerStateTempExists(t, store))
}

func schedulerStateTempExists(t *testing.T, store *blobfs.Store) bool {
	t.Helper()
	found := false
	err := fs.WalkDir(store.TenantFS("_scheduler"), ".", func(objectPath string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasPrefix(path.Base(objectPath), "tasks.yaml.tmp.") {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	return found
}
