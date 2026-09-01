package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
)

func TestSchedulerRunsTasksSerially(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	scheduler := New(store, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, scheduler.Stop(context.Background()))
	})

	runs := make(chan string, 2)
	for _, name := range []string{"a", "b"} {
		name := name
		scheduler.Register(TaskDef{Key: NewTaskKey(name, TypeExpireCleanup, ""), Interval: time.Hour, RunImmediately: true, Handler: func(context.Context) (*TaskOutcome, error) {
			runs <- name
			return nil, nil
		}})
	}
	require.Eventually(t, func() bool { return len(runs) == 2 }, time.Second, time.Millisecond)
	require.Len(t, scheduler.Snapshot(), 2)
}
