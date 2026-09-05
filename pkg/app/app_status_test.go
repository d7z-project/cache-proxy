package app

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestStatusSummaryReadsLatestDiskSample(t *testing.T) {
	for _, test := range []struct {
		next   int
		filled bool
		want   string
	}{
		{0, false, ""}, {1, false, "first"}, {2, false, "second"},
		{0, true, "third"}, {1, true, "first"}, {2, true, "second"},
	} {
		status := &appStatus{disk: []diskSample{{At: "first"}, {At: "second"}, {At: "third"}}, diskNext: test.next, diskFilled: test.filled}
		require.Equal(t, test.want, status.summary(&App{}).LastSampleAt)
	}
}

func TestStatusHistoryConcurrentAccess(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	app := &App{stores: map[string]*blobfs.Store{"files": store}}
	status := newAppStatus(config.ServerStatusConfig{DiskSampleInterval: config.Duration(time.Second), DiskHistoryWindow: config.Duration(3 * time.Second), EventLimit: 8})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Go(func() {
			for i := 0; i < 32; i++ {
				if ctx.Err() != nil {
					return
				}
				if worker == 0 {
					status.recordDiskUsage(ctx, app)
					status.appendEvent(taskEvent{Message: fmt.Sprint(i)})
				} else {
					_ = status.summary(app)
					_ = status.taskEvents(i % 10)
					_ = status.diskSamples()
				}
			}
		})
	}
	err = waitForGroup(ctx, &group)
	cancel()
	group.Wait()
	require.NoError(t, err)
	require.Len(t, status.diskSamples(), 3)
	require.Equal(t, "31", status.taskEvents(1)[0].Message)
}

func TestTaskEventsReturnLatestChronologicalWindow(t *testing.T) {
	for _, capacity := range []int{1, 3, 8} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			status := newAppStatus(config.ServerStatusConfig{
				DiskSampleInterval: config.Duration(time.Second),
				DiskHistoryWindow:  config.Duration(time.Minute),
				EventLimit:         capacity,
			})
			for count := 0; count <= 2*capacity+1; count++ {
				if count > 0 {
					status.appendEvent(taskEvent{Message: fmt.Sprint(count)})
				}
				for _, limit := range []int{-1, 0, 1, capacity, capacity + 1} {
					wantCount := min(count, capacity)
					if limit > 0 {
						wantCount = min(wantCount, limit)
					}
					events := status.taskEvents(limit)
					require.Len(t, events, wantCount)
					for i, event := range events {
						require.Equal(t, fmt.Sprint(count-wantCount+i+1), event.Message)
					}
					if len(events) > 0 {
						events[len(events)-1].Message = "modified copy"
						require.Equal(t, fmt.Sprint(count), status.taskEvents(1)[0].Message)
					}
				}
			}
		})
	}
}

func BenchmarkStatusHistoryReads(b *testing.B) {
	status := newAppStatus(config.ServerStatusConfig{
		DiskSampleInterval: config.Duration(time.Second),
		DiskHistoryWindow:  config.Duration(time.Hour),
		EventLimit:         1024,
	})
	for range 1024 {
		status.appendEvent(taskEvent{Message: "metadata refresh completed"})
	}
	status.diskFilled = true
	for i := range status.disk {
		status.disk[i] = diskSample{At: "2026-09-06T00:00:00Z"}
	}
	b.Run("summary", func(b *testing.B) {
		app := &App{}
		b.ReportAllocs()
		for b.Loop() {
			_ = status.summary(app)
		}
	})
	b.Run("latest_event", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = status.taskEvents(1)
		}
	})
	b.Run("all_events", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = status.taskEvents(0)
		}
	})
}
