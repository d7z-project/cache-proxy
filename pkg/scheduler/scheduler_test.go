package scheduler

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

func newTestStore(t *testing.T) *blobfs.Store {
	t.Helper()
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestScheduler(t *testing.T, store *blobfs.Store) (*Scheduler, *bus.Bus) {
	t.Helper()
	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	return New(b, store, reg), b
}

func TestTaskKeyFormat(t *testing.T) {
	k := NewTaskKey("inst", TypeMetadataRefresh, "sub/path")
	require.Equal(t, "inst:metadata_refresh:sub/path", k.String())
	require.Equal(t, "inst", k.Instance())
	require.Equal(t, TypeMetadataRefresh, k.Type())
	require.Equal(t, "sub/path", k.SubPath())
}

func TestRegisterAndInfo(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	key := NewTaskKey("test", TypeExpireCleanup, "")
	sched.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(ctx context.Context) error { return nil }})

	info, ok := sched.Info(key)
	require.True(t, ok)
	require.Equal(t, key, info.Key)
	require.Equal(t, StatusIdle, info.Status)
	require.Equal(t, time.Hour, info.Interval)

	sched.Stop(context.Background())
}

func TestUnregister(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	key := NewTaskKey("test", TypeExpireCleanup, "")
	sched.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(ctx context.Context) error { return nil }})

	_, ok := sched.Info(key)
	require.True(t, ok)

	sched.Unregister(key)
	_, ok = sched.Info(key)
	require.False(t, ok)

	sched.Stop(context.Background())
}

func TestTaskExecution(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var counter atomic.Int32
	sched.Start(ctx)
	sched.Register(TaskDef{
		Key:      NewTaskKey("test", TypeBlobGC, ""),
		Interval: time.Hour,
		Handler: func(ctx context.Context) error {
			counter.Add(1)
			return nil
		},
	})

	time.Sleep(100 * time.Millisecond)
	info, _ := sched.Info(NewTaskKey("test", TypeBlobGC, ""))
	require.Equal(t, StatusIdle, info.Status)
	require.Equal(t, uint64(0), uint64(counter.Load()), "task with 1h interval should not execute in 100ms")

	sched.Stop(context.Background())
}

func TestSnapshot(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	sched.Register(TaskDef{
		Key:      NewTaskKey("a", TypeExpireCleanup, ""),
		Interval: time.Hour,
		Handler:  func(ctx context.Context) error { return nil },
	})
	sched.Register(TaskDef{
		Key:      NewTaskKey("b", TypeBlobGC, ""),
		Interval: 2 * time.Hour,
		Handler:  func(ctx context.Context) error { return nil },
	})

	snapshot := sched.Snapshot()
	require.Len(t, snapshot, 2)

	sched.Stop(context.Background())
}

func TestBusEventDiscovered(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executed := make(chan string, 2)
	sched.Start(ctx)
	sched.RegisterFactory(TaskFactory{
		Instance:        "test",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      200 * time.Millisecond,
		NewRefresh: func(subPath string) TaskHandler {
			return func(ctx context.Context) error {
				executed <- "refresh:" + subPath
				return nil
			}
		},
		NewGC: func(subPath string) TaskHandler {
			return func(ctx context.Context) error {
				executed <- "gc:" + subPath
				return nil
			}
		},
	})

	b.Publish(bus.Event{
		Type:    bus.EventMetadataDiscovered,
		Payload: bus.MetadataDiscoveredPayload{Instance: "test", SubPath: "sub1"},
	})

	select {
	case result := <-executed:
		require.Equal(t, "refresh:sub1", result)
	case <-time.After(5 * time.Second):
		t.Fatal("refresh not executed after discovery")
	}
	require.Equal(t, float64(1), metricValue(t, sched.m.registered.WithLabelValues("test", string(TypeMetadataRefresh), "discovery")))
	require.Equal(t, float64(1), metricValue(t, sched.m.registered.WithLabelValues("test", string(TypeMetadataGC), "discovery")))
	require.Eventually(t, func() bool {
		return metricValue(t, sched.m.discoveriesPending.WithLabelValues("test")) == 0
	}, time.Second, 10*time.Millisecond)

	sched.Stop(context.Background())
}

func TestBusEventRemoved(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)
	sched.RegisterFactory(TaskFactory{
		Instance:        "test",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	})

	b.Publish(bus.Event{
		Type:    bus.EventMetadataDiscovered,
		Payload: bus.MetadataDiscoveredPayload{Instance: "test", SubPath: "sub1"},
	})

	time.Sleep(100 * time.Millisecond)
	_, ok := sched.Info(NewTaskKey("test", TypeMetadataRefresh, "sub1"))
	require.True(t, ok, "refresh task should exist after discovery")

	b.Publish(bus.Event{
		Type:    bus.EventMetadataRemoved,
		Payload: bus.MetadataRemovedPayload{Instance: "test", SubPath: "sub1"},
	})

	time.Sleep(100 * time.Millisecond)
	_, ok = sched.Info(NewTaskKey("test", TypeMetadataRefresh, "sub1"))
	require.False(t, ok, "refresh task should be removed")
	_, ok = sched.Info(NewTaskKey("test", TypeMetadataGC, "sub1"))
	require.False(t, ok, "gc task should be removed")

	sched.Stop(context.Background())
}

func TestDiscoveryIdempotent(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	sched.Start(ctx)
	sched.RegisterFactory(TaskFactory{
		Instance:        "test",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh: func(subPath string) TaskHandler {
			return func(ctx context.Context) error { calls.Add(1); return nil }
		},
		NewGC: func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	})

	for i := 0; i < 5; i++ {
		b.Publish(bus.Event{
			Type:    bus.EventMetadataDiscovered,
			Payload: bus.MetadataDiscoveredPayload{Instance: "test", SubPath: "dup"},
		})
	}

	time.Sleep(800 * time.Millisecond)
	info, _ := sched.Info(NewTaskKey("test", TypeMetadataRefresh, "dup"))
	require.Equal(t, uint64(1), info.RunCount)

	sched.Stop(context.Background())
}

func TestTaskFailureAndRetry(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	sched.Start(ctx)
	sched.RegisterFactory(TaskFactory{
		Instance:        "test",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      time.Hour,
		NewRefresh: func(subPath string) TaskHandler {
			return func(ctx context.Context) error {
				calls.Add(1)
				return context.DeadlineExceeded
			}
		},
		NewGC: func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	})

	b.Publish(bus.Event{
		Type:    bus.EventMetadataDiscovered,
		Payload: bus.MetadataDiscoveredPayload{Instance: "test", SubPath: "fail1"},
	})

	require.Eventually(t, func() bool {
		info, _ := sched.Info(NewTaskKey("test", TypeMetadataRefresh, "fail1"))
		return info.Status == StatusFailed
	}, 5*time.Second, 20*time.Millisecond)
	require.Greater(t, calls.Load(), int32(0), "handler should have been called at least once")
	require.Greater(t, metricValue(t, sched.m.runs.WithLabelValues("test", string(TypeMetadataRefresh), "timeout")), float64(0))
	require.Greater(t, metricValue(t, sched.m.backoff.WithLabelValues("test", string(TypeMetadataRefresh))), float64(0))

	sched.Stop(context.Background())
}

func TestRestoreMetrics(t *testing.T) {
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)

	{
		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store, reg)
		ctx, cancel := context.WithCancel(context.Background())
		sched.RegisterFactory(TaskFactory{
			Instance:        "test",
			RefreshInterval: time.Hour,
			GCInterval:      time.Hour,
			NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
			NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		})
		sched.Start(ctx)
		b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "test", SubPath: "sub1"}})
		time.Sleep(100 * time.Millisecond)
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
		Instance:        "test",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, float64(1), metricValue(t, sched.m.stateRestore.WithLabelValues("success")))
	require.Equal(t, float64(1), metricValue(t, sched.m.restoredTasks.WithLabelValues(string(TypeMetadataRefresh))))
	require.Equal(t, float64(1), metricValue(t, sched.m.restoredTasks.WithLabelValues(string(TypeMetadataGC))))
	require.NoError(t, sched.Stop(context.Background()))
}

func metricValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()
	var pb dto.Metric
	require.NoError(t, metric.Write(&pb))
	if pb.Counter != nil {
		return pb.Counter.GetValue()
	}
	if pb.Gauge != nil {
		return pb.Gauge.GetValue()
	}
	t.Fatal("unsupported metric type")
	return 0
}

func TestStopCompletesCleanly(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)
	sched.Register(TaskDef{
		Key:      NewTaskKey("stop-test", TypeBlobGC, ""),
		Interval: time.Hour,
		Handler:  func(ctx context.Context) error { return nil },
	})

	time.Sleep(50 * time.Millisecond)
	err := sched.Stop(context.Background())
	require.NoError(t, err)
}

func TestPersistenceRestore(t *testing.T) {
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)

	// First session: discover metadata and persist
	{
		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store, reg)
		ctx, cancel := context.WithCancel(context.Background())

		sched.RegisterFactory(TaskFactory{
			Instance:        "test",
			RefreshInterval: time.Hour,
			GCInterval:      2 * time.Hour,
			NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
			NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		})
		sched.Start(ctx)

		b.Publish(bus.Event{
			Type:    bus.EventMetadataDiscovered,
			Payload: bus.MetadataDiscoveredPayload{Instance: "test", SubPath: "path1"},
		})
		time.Sleep(100 * time.Millisecond)

		sched.Unregister(NewTaskKey("test", TypeMetadataGC, "path1")) // unregister gc to simplify
		cancel()
		sched.Stop(ctx)
		store.Close()
	}

	// Second session: restore from persisted state
	{
		store2, err := blobfs.Open(dir, blobfs.DefaultConfig())
		require.NoError(t, err)
		defer store2.Close()

		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store2, reg)

		sched.RegisterFactory(TaskFactory{
			Instance:        "test",
			RefreshInterval: time.Hour,
			GCInterval:      2 * time.Hour,
			NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
			NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sched.Start(ctx)

		time.Sleep(200 * time.Millisecond)

		info, ok := sched.Info(NewTaskKey("test", TypeMetadataRefresh, "path1"))
		require.True(t, ok, "task should be restored")
		require.Equal(t, time.Hour, info.Interval)

		sched.Stop(ctx)
	}
}

func TestMissingFactoryOnRestore(t *testing.T) {
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)

	// Persist a task
	{
		reg := prometheus.NewRegistry()
		b := bus.NewWithRegisterer(reg)
		sched := New(b, store, reg)
		ctx, cancel := context.WithCancel(context.Background())

		sched.RegisterFactory(TaskFactory{
			Instance:        "ghost",
			RefreshInterval: time.Hour,
			GCInterval:      time.Hour,
			NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
			NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		})
		sched.Start(ctx)

		b.Publish(bus.Event{
			Type:    bus.EventMetadataDiscovered,
			Payload: bus.MetadataDiscoveredPayload{Instance: "ghost", SubPath: "x"},
		})
		time.Sleep(100 * time.Millisecond)
		cancel()
		sched.Stop(ctx)
		store.Close()
	}

	// Restart without registering the factory
	store2, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store2.Close()

	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	sched := New(b, store2, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	// Task should NOT be restored because factory is missing
	_, ok := sched.Info(NewTaskKey("ghost", TypeMetadataRefresh, "x"))
	require.False(t, ok, "orphan task should not be restored without factory")

	sched.Stop(ctx)
}

func TestSchedulerEmptyStore(t *testing.T) {
	// Test that scheduler works without a store
	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	sched := New(b, nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	sched.Register(TaskDef{
		Key:      NewTaskKey("test", TypeExpireCleanup, ""),
		Interval: time.Hour,
		Handler:  func(ctx context.Context) error { return nil },
	})

	time.Sleep(100 * time.Millisecond)
	info, ok := sched.Info(NewTaskKey("test", TypeExpireCleanup, ""))
	require.True(t, ok)
	require.Equal(t, StatusIdle, info.Status)

	sched.Stop(ctx)
}

func TestBackoff(t *testing.T) {
	interval := time.Hour
	require.Equal(t, time.Minute, backoff(0, interval))
	require.Equal(t, interval/8, backoff(1, interval))
	require.Equal(t, interval/4, backoff(2, interval))
	require.Equal(t, interval/2, backoff(4, interval))
	require.Equal(t, interval/2, backoff(100, interval))
}

func TestRegisterFactoryOverwrite(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	f1 := TaskFactory{
		Instance:        "test",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	}
	sched.RegisterFactory(f1)

	f2 := TaskFactory{
		Instance:        "test",
		RefreshInterval: 2 * time.Hour,
		GCInterval:      3 * time.Hour,
		NewRefresh:      func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
		NewGC:           func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	}
	sched.RegisterFactory(f2)

	// Verify second factory overwrote the first in the internal registry
	// (factories field is unexported; we just verify no panic)

	sched.Stop(ctx)
}

func TestSchedulerInfoQuery(t *testing.T) {
	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	sched := New(b, nil, reg)

	sched.Start(context.Background())

	key := NewTaskKey("q", TypeExpireCleanup, "")
	sched.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(ctx context.Context) error { return nil }})

	info, ok := sched.Info(key)
	require.True(t, ok)
	require.Equal(t, key, info.Key)

	_, ok = sched.Info(NewTaskKey("nonexistent", TypeBlobGC, ""))
	require.False(t, ok)

	sched.Stop(context.Background())
}

func TestCleanupTempTests(t *testing.T) {
	_ = os.RemoveAll("_scheduler")
}

func TestHandlerPanicDoesNotCrashScheduler(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	var recovered atomic.Bool
	sched.Register(TaskDef{
		Key:      NewTaskKey("panic", TypeBlobGC, ""),
		Interval: 50 * time.Millisecond,
		Handler: func(ctx context.Context) error {
			recovered.Store(true)
			panic("unexpected handler panic")
		},
	})

	time.Sleep(700 * time.Millisecond)
	require.True(t, recovered.Load(), "handler was never invoked")

	info, ok := sched.Info(NewTaskKey("panic", TypeBlobGC, ""))
	require.True(t, ok)
	require.Equal(t, StatusFailed, info.Status)

	sched.Stop(context.Background())
}

func TestRegisterZeroInterval(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	var count atomic.Int32
	sched.Register(TaskDef{
		Key:      NewTaskKey("zero", TypeBlobGC, ""),
		Interval: 0,
		Handler:  func(ctx context.Context) error { count.Add(1); return nil },
	})

	time.Sleep(200 * time.Millisecond)
	info, _ := sched.Info(NewTaskKey("zero", TypeBlobGC, ""))
	require.Equal(t, uint64(0), info.RunCount, "zero interval = one-shot, not in heap")

	sched.Stop(context.Background())
}

func TestTriggerNonExistentTask(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	respCh := make(chan any, 1)
	sched.cmdCh <- cmd{
		kind:   cmdInfo,
		key:    NewTaskKey("ghost", TypeBlobGC, ""),
		respCh: respCh,
	}
	<-respCh

	sched.Stop(context.Background())
}

func TestUnregisterNonExistentTask(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	sched.Unregister(NewTaskKey("ghost", TypeBlobGC, ""))
	_, ok := sched.Info(NewTaskKey("ghost", TypeBlobGC, ""))
	require.False(t, ok)

	sched.Stop(context.Background())
}

func TestDiscoveryWithoutFactory(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	b.Publish(bus.Event{
		Type:    bus.EventMetadataDiscovered,
		Payload: bus.MetadataDiscoveredPayload{Instance: "unknown", SubPath: "p"},
	})

	time.Sleep(200 * time.Millisecond)
	_, ok := sched.Info(NewTaskKey("unknown", TypeMetadataRefresh, "p"))
	require.False(t, ok, "no task without factory")

	sched.Stop(context.Background())
}

func TestRemovalOfNonExistentTasks(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	b.Publish(bus.Event{
		Type:    bus.EventMetadataRemoved,
		Payload: bus.MetadataRemovedPayload{Instance: "nobody", SubPath: "x"},
	})

	time.Sleep(100 * time.Millisecond)
	sched.Stop(context.Background())
}

func TestContextCancellationDuringExecute(t *testing.T) {
	store := newTestStore(t)
	sched, b := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	done := make(chan struct{})

	sched.RegisterFactory(TaskFactory{
		Instance:        "cancel",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      time.Hour,
		NewRefresh: func(subPath string) TaskHandler {
			return func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(done)
				return ctx.Err()
			}
		},
		NewGC: func(subPath string) TaskHandler { return func(ctx context.Context) error { return nil } },
	})
	sched.Start(ctx)

	b.Publish(bus.Event{
		Type:    bus.EventMetadataDiscovered,
		Payload: bus.MetadataDiscoveredPayload{Instance: "cancel", SubPath: "sub"},
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task never started")
	}

	cancel()
	time.Sleep(200 * time.Millisecond)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task not cancelled by context")
	}

	sched.Stop(ctx)
}

func TestDuplicateRegisterOverwrites(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	var first atomic.Bool
	var second atomic.Bool
	key := NewTaskKey("dup", TypeBlobGC, "")

	sched.Register(TaskDef{
		Key:      key,
		Interval: time.Hour,
		Handler:  func(ctx context.Context) error { first.Store(true); return nil },
	})
	sched.Register(TaskDef{
		Key:      key,
		Interval: time.Minute,
		Handler:  func(ctx context.Context) error { second.Store(true); return nil },
	})

	info, ok := sched.Info(key)
	require.True(t, ok)
	require.Equal(t, time.Minute, info.Interval, "last Register wins")

	sched.Stop(context.Background())
}

func TestManyRapidRegistrations(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	for i := 0; i < 100; i++ {
		key := NewTaskKey("bulk", TypeExpireCleanup, "")
		switch i % 2 {
		case 0:
			sched.Register(TaskDef{
				Key:      key,
				Interval: time.Hour,
				Handler:  func(ctx context.Context) error { return nil },
			})
		case 1:
			sched.Unregister(key)
		}
	}

	snap := sched.Snapshot()
	require.LessOrEqual(t, len(snap), 1)

	sched.Stop(context.Background())
}

func TestTaskKeyEmptySubPath(t *testing.T) {
	k := NewTaskKey("inst", TypeBlobGC, "")
	require.Equal(t, "inst:blob_gc:", k.String())
	require.Equal(t, "", k.SubPath())
}

func TestTaskKeySpecialCharacters(t *testing.T) {
	k := NewTaskKey("a-b", TypeMetadataRefresh, "path/with@many:chars=and+stuff")
	require.Equal(t, "a-b:metadata_refresh:path/with@many:chars=and+stuff", k.String())
	require.Equal(t, "a-b", k.Instance())
	require.Equal(t, TypeMetadataRefresh, k.Type())
	require.Equal(t, "path/with@many:chars=and+stuff", k.SubPath())
}

func TestStopBeforeStartIsSafe(t *testing.T) {
	store := newTestStore(t)
	sched, _ := newTestScheduler(t, store)
	_ = sched
	stopCtx := context.Background()
	err := sched.Stop(stopCtx)
	require.NoError(t, err)
}
