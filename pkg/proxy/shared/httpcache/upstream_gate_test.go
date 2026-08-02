package httpcache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testUpstreamGate(cfg UpstreamGateConfig) *UpstreamGate {
	if cfg.MaxActive == 0 {
		cfg.MaxActive = 4
	}
	if cfg.MaxActivePerHost == 0 {
		cfg.MaxActivePerHost = 2
	}
	return NewUpstreamGate(cfg)
}

func TestUpstreamGateBoundsForegroundWait(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{
		MaxActive:           1,
		MaxActivePerHost:    1,
		ForegroundQueueWait: 30 * time.Millisecond,
	})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionForeground)
	require.NoError(t, err)
	defer release()

	started := time.Now()
	_, err = gate.Acquire(context.Background(), "https://two.example/b", AdmissionForeground)
	require.ErrorIs(t, err, ErrAdmissionWaitTimeout)
	require.GreaterOrEqual(t, time.Since(started), 25*time.Millisecond)
}

func TestUpstreamGateRefreshWaitsForCapacity(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{
		MaxActive:           1,
		MaxActivePerHost:    1,
		ForegroundQueueWait: time.Millisecond,
	})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionForeground)
	require.NoError(t, err)

	type result struct {
		release func()
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		next, acquireErr := gate.Acquire(context.Background(), "https://one.example/b", AdmissionRefresh)
		resultCh <- result{release: next, err: acquireErr}
	}()
	require.Never(t, func() bool { return len(resultCh) != 0 }, 20*time.Millisecond, 2*time.Millisecond)
	release()

	acquired := <-resultCh
	require.NoError(t, acquired.err)
	acquired.release()
}

func TestUpstreamGatePacesRequestStarts(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{RequestInterval: 40 * time.Millisecond})
	release, err := gate.Acquire(context.Background(), "https://mirror.example/a", AdmissionRefresh)
	require.NoError(t, err)
	release()

	started := time.Now()
	release, err = gate.Acquire(context.Background(), "https://mirror.example/b", AdmissionRefresh)
	require.NoError(t, err)
	release()
	require.GreaterOrEqual(t, time.Since(started), 35*time.Millisecond)
}

func TestUpstreamGateDoesNotHeadOfLineBlockOtherHosts(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 2, MaxActivePerHost: 1})
	releaseA, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionRefresh)
	require.NoError(t, err)
	defer releaseA()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queued := make(chan error, 1)
	go func() {
		_, acquireErr := gate.Acquire(ctx, "https://one.example/b", AdmissionRefresh)
		queued <- acquireErr
	}()
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)

	releaseB, err := gate.Acquire(context.Background(), "https://two.example/a", AdmissionForeground)
	require.NoError(t, err)
	releaseB()
	cancel()
	require.ErrorIs(t, <-queued, context.Canceled)
}

func TestUpstreamGateHostOverrideDisablesPacing(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{
		RequestInterval: 200 * time.Millisecond,
		Hosts: map[string]UpstreamHostGateConfig{
			"packages.d7z.net": {MaxActive: 8, RequestInterval: 0},
		},
	})
	for range 3 {
		release, err := gate.Acquire(context.Background(), "https://packages.d7z.net/repo", AdmissionRefresh)
		require.NoError(t, err)
		release()
	}
	snapshot := gate.Snapshot().Hosts["packages.d7z.net"]
	require.Equal(t, 8, snapshot.MaxActive)
	require.Zero(t, snapshot.RequestInterval)
}

func TestUpstreamGateProbeSkipsWithoutQueueing(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionForeground)
	require.NoError(t, err)
	defer release()

	probeRelease, ok := gate.TryAcquireProbe("https://two.example/a")
	require.False(t, ok)
	require.Nil(t, probeRelease)
	require.Zero(t, gate.Snapshot().Queued)
}

func TestUpstreamGateRateLimitReleasesSameHostWaiters(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://mirror.example/a", AdmissionForeground)
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() {
		_, acquireErr := gate.Acquire(context.Background(), "https://mirror.example/b", AdmissionRefresh)
		resultCh <- acquireErr
	}()
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)

	limited := gate.RateLimited("https://mirror.example/a", "60")
	var queuedLimit *UpstreamRateLimitError
	require.ErrorAs(t, <-resultCh, &queuedLimit)
	require.Equal(t, "mirror.example", queuedLimit.Host)
	require.WithinDuration(t, limited.RetryAfter, queuedLimit.RetryAfter, time.Millisecond)
	release()

	otherRelease, err := gate.Acquire(context.Background(), "https://other.example/a", AdmissionRefresh)
	require.NoError(t, err)
	otherRelease()
}

func TestUpstreamGateCanceledWaiterDoesNotConsumeCapacity(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionRefresh)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, acquireErr := gate.Acquire(ctx, "https://two.example/a", AdmissionRefresh)
		resultCh <- acquireErr
	}()
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)
	cancel()
	require.True(t, errors.Is(<-resultCh, context.Canceled))
	release()
	require.Zero(t, gate.Snapshot().Active)
}
