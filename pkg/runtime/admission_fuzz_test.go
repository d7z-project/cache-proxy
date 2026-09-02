package runtime

import (
	"context"
	"fmt"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func FuzzUpstreamGateParallelAcquire(f *testing.F) {
	f.Add(uint8(1), uint8(1), uint8(1), uint8(1), uint64(0), uint64(0))
	f.Add(uint8(24), uint8(4), uint8(3), uint8(2), uint64(0xaaaa), uint64(0x5555))
	f.Fuzz(func(t *testing.T, workerInput, hostInput, activeInput, hostActiveInput uint8, cancelMask, refreshMask uint64) {
		workerCount := int(workerInput%24) + 1
		hostCount := min(int(hostInput%4)+1, workerCount)
		maxActive := int(activeInput%4) + 1
		maxActivePerHost := int(hostActiveInput%4) + 1
		gate := NewUpstreamGate(UpstreamGateConfig{MaxActive: maxActive, MaxActivePerHost: maxActivePerHost})

		var active atomic.Int32
		hostActive := make([]atomic.Int32, hostCount)
		var violated atomic.Bool
		starts := make(chan struct{})
		errors := make(chan error, workerCount)
		var wait sync.WaitGroup
		for i := range workerCount {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				if cancelMask&(uint64(1)<<index) != 0 {
					cancel()
				} else {
					defer cancel()
				}
				<-starts
				hostIndex := index % hostCount
				class := AdmissionForeground
				if refreshMask&(uint64(1)<<index) != 0 {
					class = AdmissionRefresh
				}
				release, err := gate.Acquire(ctx, fmt.Sprintf("https://host-%d.example/object", hostIndex), class)
				if cancelMask&(uint64(1)<<index) != 0 {
					if err != context.Canceled {
						errors <- fmt.Errorf("pre-canceled acquire returned %v", err)
					}
					return
				}
				if err != nil {
					errors <- err
					return
				}
				current := active.Add(1)
				hostCurrent := hostActive[hostIndex].Add(1)
				if current > int32(maxActive) || hostCurrent > int32(maxActivePerHost) {
					violated.Store(true)
				}
				goruntime.Gosched()
				hostActive[hostIndex].Add(-1)
				active.Add(-1)
				release()
			}(i)
		}
		close(starts)
		wait.Wait()
		close(errors)
		for err := range errors {
			require.NoError(t, err)
		}
		require.False(t, violated.Load())
		require.Zero(t, active.Load())
		for i := range hostActive {
			require.Zero(t, hostActive[i].Load())
		}
		snapshot := gate.Snapshot()
		require.Zero(t, snapshot.Active)
		require.Zero(t, snapshot.Queued)
	})
}
