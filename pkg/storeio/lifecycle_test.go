package storeio

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLifecycleAdmissionAndCloseDoNotRace(t *testing.T) {
	for range 100 {
		lifecycle := NewLifecycle()
		var workers sync.WaitGroup
		for range 32 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, done, err := lifecycle.Begin()
				if err != nil {
					if !errors.Is(err, ErrLifecycleClosed) {
						t.Errorf("begin lifecycle: %v", err)
					}
					return
				}
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
	}
}

func TestLifecycleBeginCloseCancelsBeforeWaiting(t *testing.T) {
	lifecycle := NewLifecycle()
	workCtx, done, err := lifecycle.Begin()
	require.NoError(t, err)

	lifecycle.BeginClose()
	lifecycle.BeginClose()
	require.ErrorIs(t, workCtx.Err(), context.Canceled)
	_, rejectedDone, err := lifecycle.Begin()
	require.Nil(t, rejectedDone)
	require.ErrorIs(t, err, ErrLifecycleClosed)

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, lifecycle.Close(waitCtx), context.DeadlineExceeded)
	done()
	require.NoError(t, lifecycle.Close(context.Background()))
}
