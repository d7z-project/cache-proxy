package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchedulerRunsTasksSerially(t *testing.T) {
	scheduler := newScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, scheduler.Stop(context.Background()))
	})

	runs := make(chan string, 2)
	for _, name := range []string{"a", "b"} {
		scheduler.Register(TaskDef{Key: NewTaskKey(name, TypeExpireCleanup, ""), Interval: time.Hour, RunImmediately: true, Handler: func(context.Context) (*TaskOutcome, error) {
			runs <- name
			return nil, nil
		}})
	}
	require.Eventually(t, func() bool { return len(runs) == 2 }, time.Second, time.Millisecond)
	require.Len(t, scheduler.Snapshot(), 2)
}

func TestSchedulerCheckpointTracksMetadataCompletion(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "scheduler.json")
	s, err := NewPersistent(statePath)
	require.NoError(t, err)
	s.ctx = context.Background()
	for _, typ := range []TaskType{TypeMetadataRefresh, TypeMetadataGC, TypeBlobGC, TypeExpireCleanup, TypeGitSync} {
		s.Register(TaskDef{Key: NewTaskKey("repo", typ, ""), Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	}
	s.runTask(NewTaskKey("repo", TypeMetadataRefresh, ""))
	checkpointTime := time.Unix(1700000000, 0)
	require.NoError(t, os.Chtimes(statePath, checkpointTime, checkpointTime))
	checkpoint, err := os.ReadFile(statePath)
	require.NoError(t, err)
	for _, typ := range []TaskType{TypeBlobGC, TypeExpireCleanup, TypeGitSync} {
		s.runTask(NewTaskKey("repo", typ, ""))
		info, err := os.Stat(statePath)
		require.NoError(t, err)
		require.Equal(t, checkpointTime, info.ModTime())
		data, err := os.ReadFile(statePath)
		require.NoError(t, err)
		require.Equal(t, checkpoint, data)
	}
	s.runTask(NewTaskKey("repo", TypeMetadataGC, ""))
	info, err := os.Stat(statePath)
	require.NoError(t, err)
	require.True(t, info.ModTime().After(checkpointTime))
	restored, err := NewPersistent(statePath)
	require.NoError(t, err)
	require.Len(t, restored.persisted, 2)
	for _, task := range s.Snapshot() {
		require.EqualValues(t, 1, task.RunCount)
		if persistentTask(task.Key.Type()) {
			require.True(t, task.NextRun.Equal(restored.persisted[task.Key.String()]))
		}
	}
}

func BenchmarkSchedulerMaintenance(b *testing.B) {
	for _, typ := range []TaskType{TypeMetadataRefresh, TypeExpireCleanup} {
		b.Run(string(typ), func(b *testing.B) {
			s, err := NewPersistent(filepath.Join(b.TempDir(), "scheduler.json"))
			require.NoError(b, err)
			s.ctx = context.Background()
			key := NewTaskKey("repo", typ, "")
			for _, taskType := range []TaskType{TypeMetadataRefresh, TypeExpireCleanup} {
				s.Register(TaskDef{Key: NewTaskKey("repo", taskType, ""), Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s.runTask(key)
			}
		})
	}
}

func TestSchedulerPersistsOnlyMetadataTiming(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "scheduler.json")
	scheduler, err := NewPersistent(statePath)
	require.NoError(t, err)
	metadataKey := NewTaskKey("deb", TypeMetadataRefresh, "deb")
	runs := make(chan struct{}, 1)
	scheduler.Register(TaskDef{Key: metadataKey, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) {
		runs <- struct{}{}
		return nil, nil
	}})
	scheduler.Register(TaskDef{Key: NewTaskKey("deb", TypeExpireCleanup, ""), Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	require.True(t, scheduler.TriggerNow(metadataKey))
	_, err = os.Stat(statePath)
	require.ErrorIs(t, err, os.ErrNotExist)
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	<-runs
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(statePath)
		return statErr == nil
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, scheduler.Stop(context.Background()))

	restored, err := NewPersistent(statePath)
	require.NoError(t, err)
	restored.Register(TaskDef{Key: metadataKey, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	infos := restored.Snapshot()
	require.Len(t, infos, 1)
	info := infos[0]
	require.Equal(t, metadataKey, info.Key)
	require.WithinDuration(t, time.Now().Add(time.Hour), info.NextRun, time.Second)

	require.NoError(t, os.WriteFile(statePath, []byte(`{"next_run":{},"unknown":true}`), 0o644))
	_, err = NewPersistent(statePath)
	require.Error(t, err)
}

func TestSchedulerTriggerNowAndContinuation(t *testing.T) {
	scheduler := newScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, scheduler.Stop(context.Background()))
	})

	key := NewTaskKey("cache", TypeExpireCleanup, "")
	runs := make(chan time.Time, 2)
	scheduler.Register(TaskDef{
		Key: key, Interval: time.Hour,
		Handler: func(context.Context) (*TaskOutcome, error) {
			runs <- time.Now()
			return &TaskOutcome{ContinueAfter: 10 * time.Millisecond}, nil
		},
	})
	require.True(t, scheduler.TriggerNow(key))
	first := <-runs
	second := <-runs
	require.Less(t, second.Sub(first), time.Second)
	require.False(t, scheduler.TriggerNow(NewTaskKey("missing", TypeExpireCleanup, "")))
}

func TestSchedulerRetainsTriggerDuringRunningTask(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "scheduler.json")
	scheduler, err := NewPersistent(statePath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, scheduler.Stop(context.Background()))
	})

	key := NewTaskKey("deb", TypeMetadataRefresh, "")
	started := make(chan struct{})
	completed := make(chan TaskRun, 2)
	scheduler.SetRunObserver(func(run TaskRun) { completed <- run })
	release := make(chan struct{})
	runs := 0
	scheduler.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(ctx context.Context) (*TaskOutcome, error) {
		runs++
		if runs == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	}})
	require.True(t, scheduler.TriggerNow(key))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not start")
	}
	var clients sync.WaitGroup
	var rejected atomic.Int32
	for range 32 {
		clients.Go(func() {
			if !scheduler.TriggerNow(key) {
				rejected.Add(1)
			}
			_ = scheduler.Snapshot()
		})
	}
	clients.Wait()
	require.Zero(t, rejected.Load())
	close(release)
	for range 2 {
		select {
		case run := <-completed:
			require.Empty(t, run.Err)
		case <-time.After(5 * time.Second):
			t.Fatal("trigger received while running was lost")
		}
	}
	cancel()
	require.NoError(t, scheduler.Stop(context.Background()))
	infos := scheduler.Snapshot()
	require.EqualValues(t, 2, infos[0].RunCount)
	restored, err := NewPersistent(statePath)
	require.NoError(t, err)
	restored.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	require.Equal(t, infos[0].NextRun.UTC(), restored.Snapshot()[0].NextRun.UTC())
}

func TestSchedulerHonorsErrorContinuation(t *testing.T) {
	scheduler := newScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, scheduler.Stop(context.Background()))
	})

	key := NewTaskKey("deb", TypeMetadataRefresh, "deb")
	runs := make(chan time.Time, 2)
	scheduler.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) {
		runs <- time.Now()
		return &TaskOutcome{ContinueAfter: 10 * time.Millisecond}, errors.New("temporary failure")
	}})
	require.True(t, scheduler.TriggerNow(key))
	first := <-runs
	second := <-runs
	require.Less(t, second.Sub(first), time.Second)
}
