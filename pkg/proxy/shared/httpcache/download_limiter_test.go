package httpcache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDownloadLimiterWaitsForInstanceCapacity(t *testing.T) {
	limiter := NewDownloadLimiter(2, 1)
	releaseA, err := limiter.Acquire(context.Background(), "a")
	require.NoError(t, err)

	type acquireResult struct {
		release func()
		err     error
	}
	result := make(chan acquireResult, 1)
	go func() {
		release, acquireErr := limiter.Acquire(context.Background(), "a")
		result <- acquireResult{release: release, err: acquireErr}
	}()

	select {
	case <-result:
		t.Fatal("acquire returned before capacity was released")
	case <-time.After(25 * time.Millisecond):
	}
	releaseA()

	acquired := <-result
	require.NoError(t, acquired.err)
	acquired.release()
}

func TestDownloadLimiterWaitCanBeCanceled(t *testing.T) {
	limiter := NewDownloadLimiter(1, 1)
	release, err := limiter.Acquire(context.Background(), "a")
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = limiter.Acquire(ctx, "b")
	require.ErrorIs(t, err, ErrDownloadLimit)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDownloadLimiterBoundsWaitingQueue(t *testing.T) {
	limiter := NewDownloadLimiter(1, 1)
	releaseActive, err := limiter.Acquire(context.Background(), "a")
	require.NoError(t, err)

	waiterDone := make(chan struct{})
	go func() {
		release, acquireErr := limiter.Acquire(context.Background(), "b")
		if acquireErr == nil {
			release()
		}
		close(waiterDone)
	}()
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters.Len() == 1
	}, time.Second, 5*time.Millisecond)
	_, err = limiter.Acquire(context.Background(), "c")
	require.ErrorIs(t, err, ErrDownloadLimit)

	releaseActive()
	<-waiterDone
}

func TestDownloadLimiterUpdateWakesWaiter(t *testing.T) {
	limiter := NewDownloadLimiter(1, 1)
	releaseA, err := limiter.Acquire(context.Background(), "a")
	require.NoError(t, err)
	defer releaseA()

	result := make(chan error, 1)
	go func() {
		release, acquireErr := limiter.Acquire(context.Background(), "b")
		if acquireErr == nil {
			release()
		}
		result <- acquireErr
	}()
	limiter.Update(2, 2)
	require.NoError(t, <-result)
}
