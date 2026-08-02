package httpcache

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDownloadLimiterSharesHostCooldownAcrossInstances(t *testing.T) {
	limiter := NewDownloadLimiter(8, 4)
	until := limiter.ObserveResponse("https://mirror.example/repo", http.StatusTooManyRequests, "60")
	require.True(t, until.After(time.Now()))

	for _, instance := range []string{"arch", "alpine"} {
		_, err := limiter.AcquireUpstream(context.Background(), instance, "https://mirror.example/other", false)
		var limited *RateLimitError
		require.ErrorAs(t, err, &limited)
		require.Equal(t, "mirror.example", limited.Host)
		require.WithinDuration(t, until, limited.RetryAfter, time.Second)
	}

	release, err := limiter.AcquireUpstream(context.Background(), "arch", "https://other.example/repo", false)
	require.NoError(t, err)
	release()
}

func TestDownloadLimiterBackgroundFailsFastWhenCapacityIsReserved(t *testing.T) {
	limiter := NewDownloadLimiter(4, 4)
	limiter.Configure(4, 4, 4, 1)
	release, err := limiter.AcquireUpstream(context.Background(), "a", "https://one.example", true)
	require.NoError(t, err)

	started := time.Now()
	_, err = limiter.AcquireUpstream(context.Background(), "b", "https://two.example", true)
	require.ErrorIs(t, err, ErrDownloadLimit)
	require.Less(t, time.Since(started), 20*time.Millisecond)

	foreground, err := limiter.AcquireUpstream(context.Background(), "b", "https://two.example", false)
	require.NoError(t, err)
	foreground()
	release()
}

func TestDownloadLimiterBoundsForegroundQueueWait(t *testing.T) {
	limiter := NewDownloadLimiter(1, 1)
	limiter.ConfigurePacing(100, 1, 25*time.Millisecond)
	release, err := limiter.Acquire(context.Background(), "active")
	require.NoError(t, err)
	defer release()

	started := time.Now()
	_, err = limiter.Acquire(context.Background(), "queued")
	require.ErrorIs(t, err, ErrDownloadLimit)
	require.GreaterOrEqual(t, time.Since(started), 20*time.Millisecond)
	require.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestDownloadLimiterPacesRequestsPerHost(t *testing.T) {
	limiter := NewDownloadLimiter(4, 4)
	limiter.ConfigurePacing(5, 1, time.Second)
	release, err := limiter.AcquireUpstream(context.Background(), "a", "https://mirror.example/a", false)
	require.NoError(t, err)
	release()

	started := time.Now()
	release, err = limiter.AcquireUpstream(context.Background(), "b", "https://mirror.example/b", false)
	require.NoError(t, err)
	release()
	require.GreaterOrEqual(t, time.Since(started), 150*time.Millisecond)
}

func TestDownloadLimiterRateLimitReleasesQueuedHostWaiters(t *testing.T) {
	limiter := NewDownloadLimiter(1, 1)
	release, err := limiter.AcquireUpstream(context.Background(), "active", "https://mirror.example/repo", false)
	require.NoError(t, err)
	defer release()

	result := make(chan error, 1)
	go func() {
		_, acquireErr := limiter.AcquireUpstream(context.Background(), "queued", "https://mirror.example/other", false)
		result <- acquireErr
	}()
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters.Len() == 1
	}, time.Second, 5*time.Millisecond)

	until := limiter.ObserveResponse("https://mirror.example/repo", http.StatusTooManyRequests, "60")
	var limited *RateLimitError
	require.ErrorAs(t, <-result, &limited)
	require.Equal(t, "mirror.example", limited.Host)
	require.WithinDuration(t, until, limited.RetryAfter, time.Second)
}

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
