package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestSchedulerPersistsOnlyMetadataTiming(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "scheduler.json")
	scheduler, err := NewPersistent(statePath)
	require.NoError(t, err)
	metadataKey := NewTaskKey("deb", TypeMetadataRefresh, "deb")
	scheduler.Register(TaskDef{Key: metadataKey, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	scheduler.Register(TaskDef{Key: NewTaskKey("deb", TypeExpireCleanup, ""), Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	require.True(t, scheduler.TriggerNow(metadataKey))

	restored, err := NewPersistent(statePath)
	require.NoError(t, err)
	restored.Register(TaskDef{Key: metadataKey, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) { return nil, nil }})
	infos := restored.Snapshot()
	require.Len(t, infos, 1)
	info := infos[0]
	require.Equal(t, metadataKey, info.Key)
	require.WithinDuration(t, time.Now(), info.NextRun, time.Second)

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
	scheduler := newScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, scheduler.Stop(context.Background()))
	})

	key := NewTaskKey("deb", TypeMetadataRefresh, "")
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	scheduler.Register(TaskDef{Key: key, Interval: time.Hour, Handler: func(context.Context) (*TaskOutcome, error) {
		started <- struct{}{}
		if len(started) == 1 {
			<-release
		}
		return nil, nil
	}})
	require.True(t, scheduler.TriggerNow(key))
	<-started
	require.True(t, scheduler.TriggerNow(key))
	close(release)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("trigger received while running was lost")
	}
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
