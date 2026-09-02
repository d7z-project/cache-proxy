package storeio

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLifecycleAdmissionAndCloseDoNotRace(t *testing.T) {
	for range 100 {
		lifecycle := NewLifecycle()
		var started atomic.Int64
		var workers sync.WaitGroup
		for range 32 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, done, err := lifecycle.Begin()
				if err != nil {
					require.ErrorIs(t, err, ErrLifecycleClosed)
					return
				}
				started.Add(1)
				time.Sleep(time.Microsecond)
				done()
				done()
			}()
		}
		require.NoError(t, lifecycle.Close(context.Background()))
		workers.Wait()
		_, done, err := lifecycle.Begin()
		require.Nil(t, done)
		require.True(t, errors.Is(err, ErrLifecycleClosed))
		require.GreaterOrEqual(t, started.Load(), int64(0))
	}
}
