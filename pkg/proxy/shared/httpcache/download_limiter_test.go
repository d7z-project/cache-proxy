package httpcache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadLimiterEnforcesGlobalAndInstanceLimits(t *testing.T) {
	ctx := context.Background()
	limiter := NewDownloadLimiter(2, 1)

	releaseA, err := limiter.Acquire(ctx, "a")
	require.NoError(t, err)
	defer releaseA()

	_, err = limiter.Acquire(ctx, "a")
	require.ErrorIs(t, err, ErrDownloadLimit)

	releaseB, err := limiter.Acquire(ctx, "b")
	require.NoError(t, err)
	defer releaseB()

	_, err = limiter.Acquire(ctx, "c")
	require.ErrorIs(t, err, ErrDownloadLimit)

	releaseA()
	releaseA = func() {}
	releaseC, err := limiter.Acquire(ctx, "c")
	require.NoError(t, err)
	releaseC()
}

func TestDownloadLimiterUpdate(t *testing.T) {
	ctx := context.Background()
	limiter := NewDownloadLimiter(1, 1)

	releaseA, err := limiter.Acquire(ctx, "a")
	require.NoError(t, err)
	defer releaseA()

	_, err = limiter.Acquire(ctx, "b")
	require.ErrorIs(t, err, ErrDownloadLimit)

	limiter.Update(2, 2)
	releaseB, err := limiter.Acquire(ctx, "b")
	require.NoError(t, err)
	releaseB()
}
