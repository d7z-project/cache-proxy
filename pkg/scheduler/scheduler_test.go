package scheduler

import (
	"context"
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
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestScheduler(t *testing.T, store *blobfs.Store) (*Scheduler, *bus.Bus) {
	t.Helper()
	reg := prometheus.NewRegistry()
	b := bus.NewWithRegisterer(reg)
	return New(b, store, reg), b
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
	require.FailNow(t, "unsupported metric type")
	return 0
}

func TestTaskKey(t *testing.T) {
	key := NewTaskKey("inst", TypeMetadataRefresh, "sub/path")
	require.Equal(t, "inst:metadata_refresh:sub/path", key.String())
	require.Equal(t, "inst", key.Instance())
	require.Equal(t, TypeMetadataRefresh, key.Type())
	require.Equal(t, "sub/path", key.SubPath())
}

func TestRegisterInfoAndSnapshotBeforeStart(t *testing.T) {
	sched, _ := newTestScheduler(t, newTestStore(t))
	key := NewTaskKey("pre", TypeExpireCleanup, "")
	sched.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(context.Context) error { return nil }})

	info, ok := sched.Info(key)
	require.True(t, ok)
	require.Equal(t, StatusIdle, info.Status)
	require.Equal(t, time.Hour, info.Interval)
	require.Len(t, sched.Snapshot(), 1)
}

func TestRegisterAndUnregisterAfterStart(t *testing.T) {
	sched, _ := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	key := NewTaskKey("test", TypeExpireCleanup, "")
	sched.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(context.Context) error { return nil }})
	info, ok := sched.Info(key)
	require.True(t, ok)
	require.Equal(t, key, info.Key)

	sched.Unregister(key)
	_, ok = sched.Info(key)
	require.False(t, ok)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestDiscoveryCreatesRefreshAndGCTasks(t *testing.T) {
	sched, b := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	executed := make(chan string, 2)
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      time.Hour,
		NewRefresh: func(subPath string) TaskHandler {
			return func(context.Context) error {
				executed <- "refresh:" + subPath
				return nil
			}
		},
		NewGC: func(subPath string) TaskHandler {
			return func(context.Context) error {
				executed <- "gc:" + subPath
				return nil
			}
		},
	})

	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "root"}})
	select {
	case result := <-executed:
		require.Equal(t, "refresh:root", result)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "refresh task did not execute")
	}

	refreshKey := NewTaskKey("repo", TypeMetadataRefresh, "root")
	gcKey := NewTaskKey("repo", TypeMetadataGC, "root")
	_, ok := sched.Info(refreshKey)
	require.True(t, ok)
	_, ok = sched.Info(gcKey)
	require.True(t, ok)
	require.Equal(t, float64(1), metricValue(t, sched.m.registered.WithLabelValues("repo", string(TypeMetadataRefresh), "discovery")))
	require.NoError(t, sched.Stop(context.Background()))
}

func TestMetadataRemovedEventUnregistersTasks(t *testing.T) {
	sched, b := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh:      func(string) TaskHandler { return func(context.Context) error { return nil } },
		NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
	})
	sched.Start(ctx)

	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "root"}})
	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "root"))
		return ok
	}, time.Second, 10*time.Millisecond)

	b.Publish(bus.Event{Type: bus.EventMetadataRemoved, Payload: bus.MetadataRemovedPayload{Instance: "repo", SubPath: "root"}})
	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "root"))
		return !ok
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestRefreshTaskFailureUpdatesStatusAndBackoff(t *testing.T) {
	sched, b := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	var calls atomic.Int32
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      time.Hour,
		NewRefresh: func(string) TaskHandler {
			return func(context.Context) error {
				calls.Add(1)
				return context.DeadlineExceeded
			}
		},
		NewGC: func(string) TaskHandler { return func(context.Context) error { return nil } },
	})

	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "root"}})
	require.Eventually(t, func() bool {
		info, _ := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "root"))
		return info.Status == StatusFailed
	}, 5*time.Second, 20*time.Millisecond)
	require.Greater(t, calls.Load(), int32(0))
	require.Greater(t, metricValue(t, sched.m.backoff.WithLabelValues("repo", string(TypeMetadataRefresh))), float64(0))
	require.NoError(t, sched.Stop(context.Background()))
}

func TestHandlerPanicMarksTaskFailed(t *testing.T) {
	sched, _ := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	var called atomic.Bool
	key := NewTaskKey("panic", TypeBlobGC, "")
	sched.Register(TaskDef{
		Key:      key,
		Interval: 50 * time.Millisecond,
		Handler: func(context.Context) error {
			called.Store(true)
			panic("unexpected handler panic")
		},
	})

	require.Eventually(t, called.Load, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		info, _ := sched.Info(key)
		return info.Status == StatusFailed
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestContextCancellationStopsRunningTask(t *testing.T) {
	sched, b := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	done := make(chan struct{})
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      time.Hour,
		NewRefresh: func(string) TaskHandler {
			return func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(done)
				return ctx.Err()
			}
		},
		NewGC: func(string) TaskHandler { return func(context.Context) error { return nil } },
	})
	sched.Start(ctx)

	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "root"}})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "task did not start")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "task did not observe cancellation")
	}
	require.NoError(t, sched.Stop(context.Background()))
}

func TestZeroIntervalTaskRunsImmediately(t *testing.T) {
	sched, _ := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	var count atomic.Int32
	sched.Register(TaskDef{
		Key:      NewTaskKey("zero", TypeBlobGC, ""),
		Interval: 0,
		Handler:  func(context.Context) error { count.Add(1); return nil },
	})
	require.Eventually(t, func() bool { return count.Load() > 0 }, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestDuplicateRegisterKeepsLatestDefinition(t *testing.T) {
	sched, _ := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	key := NewTaskKey("dup", TypeBlobGC, "")
	sched.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(context.Context) error { return nil }})
	sched.Register(TaskDef{Key: key, Interval: time.Minute, Handler: func(context.Context) error { return nil }})

	info, ok := sched.Info(key)
	require.True(t, ok)
	require.Equal(t, time.Minute, info.Interval)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestBackoff(t *testing.T) {
	interval := time.Hour
	require.Equal(t, time.Minute, backoff(0, interval))
	require.Equal(t, interval/8, backoff(1, interval))
	require.Equal(t, interval/4, backoff(2, interval))
	require.Equal(t, interval/2, backoff(4, interval))
	require.Equal(t, interval/2, backoff(100, interval))
}

func TestStopBeforeStartIsSafe(t *testing.T) {
	sched, _ := newTestScheduler(t, newTestStore(t))
	require.NoError(t, sched.Stop(context.Background()))
}

func TestMetricInstancesCleanupOnRemove(t *testing.T) {
	sched, b := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)

	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: time.Hour,
		GCInterval:      time.Hour,
		NewRefresh: func(string) TaskHandler { return func(context.Context) error { return nil } },
		NewGC:           func(string) TaskHandler { return func(context.Context) error { return nil } },
	})

	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "a"}})
	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "b"}})
	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "b"))
		return ok
	}, time.Second, 10*time.Millisecond)

	_, aOk := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "a"))
	require.True(t, aOk)

	b.Publish(bus.Event{Type: bus.EventMetadataRemoved, Payload: bus.MetadataRemovedPayload{Instance: "repo", SubPath: "a"}})
	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "a"))
		return !ok
	}, time.Second, 10*time.Millisecond)

	_, bOk := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "b"))
	require.True(t, bOk, "subpath b should still be present after removing a")

	b.Publish(bus.Event{Type: bus.EventMetadataRemoved, Payload: bus.MetadataRemovedPayload{Instance: "repo", SubPath: "b"}})
	require.Eventually(t, func() bool {
		_, ok := sched.Info(NewTaskKey("repo", TypeMetadataRefresh, "b"))
		return !ok
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, sched.Stop(context.Background()))
}

func TestStopTimeoutMarksStopped(t *testing.T) {
	sched, b := newTestScheduler(t, newTestStore(t))
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	sched.RegisterFactory(TaskFactory{
		Instance:        "repo",
		RefreshInterval: 100 * time.Millisecond,
		GCInterval:      time.Hour,
		NewRefresh: func(string) TaskHandler {
			return func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				time.Sleep(200 * time.Millisecond)
				return ctx.Err()
			}
		},
		NewGC: func(string) TaskHandler { return func(context.Context) error { return nil } },
	})
	sched.Start(ctx)

	b.Publish(bus.Event{Type: bus.EventMetadataDiscovered, Payload: bus.MetadataDiscoveredPayload{Instance: "repo", SubPath: "root"}})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "task did not start")
	}

	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()
	err := sched.Stop(stopCtx)
	require.Error(t, err, "Stop should time out")

	require.True(t, sched.stopped.Load(), "stopped flag should be set after timeout")
}
