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

type gateAcquireResult struct {
	release func()
	err     error
}

func acquireAsync(gate *UpstreamGate, ctx context.Context, upstream string, class AdmissionClass) <-chan gateAcquireResult {
	result := make(chan gateAcquireResult, 1)
	go func() {
		release, err := gate.Acquire(ctx, upstream, class)
		result <- gateAcquireResult{release: release, err: err}
	}()
	return result
}

func TestUpstreamGateSaturationWaitsUntilRelease(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionForeground)
	require.NoError(t, err)

	queued := acquireAsync(gate, context.Background(), "https://two.example/b", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)
	require.Never(t, func() bool { return len(queued) > 0 }, 30*time.Millisecond, 2*time.Millisecond)
	release()

	result := <-queued
	require.NoError(t, result.err)
	result.release()
}

func TestUpstreamGateForegroundPrecedesRefresh(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://busy.example/a", AdmissionForeground)
	require.NoError(t, err)
	refresh := acquireAsync(gate, context.Background(), "https://refresh.example/a", AdmissionRefresh)
	require.Eventually(t, func() bool { return gate.Snapshot().RefreshQueued == 1 }, time.Second, time.Millisecond)
	foreground := acquireAsync(gate, context.Background(), "https://client.example/a", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().ForegroundQueued == 1 }, time.Second, time.Millisecond)

	release()
	clientResult := <-foreground
	require.NoError(t, clientResult.err)
	require.Never(t, func() bool { return len(refresh) > 0 }, 20*time.Millisecond, 2*time.Millisecond)
	clientResult.release()
	refreshResult := <-refresh
	require.NoError(t, refreshResult.err)
	refreshResult.release()
}

func TestUpstreamGateDoesNotHeadOfLineBlockOtherHosts(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 2, MaxActivePerHost: 1})
	releaseA, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionForeground)
	require.NoError(t, err)
	defer releaseA()

	ctx, cancel := context.WithCancel(context.Background())
	blocked := acquireAsync(gate, ctx, "https://one.example/b", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)

	releaseB, err := gate.Acquire(context.Background(), "https://two.example/a", AdmissionForeground)
	require.NoError(t, err)
	releaseB()
	cancel()
	require.ErrorIs(t, (<-blocked).err, context.Canceled)
}

func TestUpstreamGateRoundRobinsHostsWithinPriority(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://busy.example/a", AdmissionForeground)
	require.NoError(t, err)
	firstA := acquireAsync(gate, context.Background(), "https://a.example/one", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)
	secondA := acquireAsync(gate, context.Background(), "https://a.example/two", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 2 }, time.Second, time.Millisecond)
	requestB := acquireAsync(gate, context.Background(), "https://b.example/one", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 3 }, time.Second, time.Millisecond)

	release()
	resultA := <-firstA
	require.NoError(t, resultA.err)
	resultA.release()
	resultB := <-requestB
	require.NoError(t, resultB.err)
	require.Never(t, func() bool { return len(secondA) > 0 }, 20*time.Millisecond, 2*time.Millisecond)
	resultB.release()
	lastA := <-secondA
	require.NoError(t, lastA.err)
	lastA.release()
}

func TestUpstreamGateHasNoRequestStartPacing(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{})
	start := time.Now()
	for range 100 {
		release, err := gate.Acquire(context.Background(), "https://fast.example/file", AdmissionForeground)
		require.NoError(t, err)
		release()
	}
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestUpstreamGateHostOverride(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{Hosts: map[string]UpstreamHostGateConfig{
		"packages.example": {MaxActive: 8},
	}})
	require.Equal(t, 8, gate.Snapshot().Hosts["packages.example"].MaxActive)
}

func TestUpstreamGateRateLimitKeepsForegroundQueued(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://mirror.example/a", AdmissionForeground)
	require.NoError(t, err)
	queued := acquireAsync(gate, context.Background(), "https://mirror.example/b", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)

	limited := gate.RateLimited("https://mirror.example/a", "1")
	require.False(t, limited.RetryAfter.IsZero())
	release()
	require.Never(t, func() bool { return len(queued) > 0 }, 50*time.Millisecond, 5*time.Millisecond)

	otherRelease, err := gate.Acquire(context.Background(), "https://other.example/a", AdmissionForeground)
	require.NoError(t, err)
	otherRelease()
	result := <-queued
	require.NoError(t, result.err)
	result.release()
}

func TestUpstreamGateRefreshReturnsCooldownWithoutQueueing(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{})
	limited := gate.RateLimited("https://mirror.example/a", "2")
	_, err := gate.Acquire(context.Background(), "https://mirror.example/b", AdmissionRefresh)
	var acquiredLimit *UpstreamRateLimitError
	require.ErrorAs(t, err, &acquiredLimit)
	require.WithinDuration(t, limited.RetryAfter, acquiredLimit.RetryAfter, time.Millisecond)
	require.Zero(t, gate.Snapshot().Queued)
}

func TestUpstreamGateInvalidRetryAfterDoesNotCreateCooldown(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{})
	require.True(t, gate.RateLimited("https://mirror.example/a", "invalid").RetryAfter.IsZero())
	require.True(t, gate.RateLimited("https://mirror.example/a", "").RetryAfter.IsZero())
	release, err := gate.Acquire(context.Background(), "https://mirror.example/b", AdmissionRefresh)
	require.NoError(t, err)
	release()
}

func TestUpstreamGateCanceledWaiterDoesNotConsumeCapacity(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionRefresh)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	queued := acquireAsync(gate, ctx, "https://two.example/a", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)
	cancel()
	require.True(t, errors.Is((<-queued).err, context.Canceled))
	release()
	require.Zero(t, gate.Snapshot().Active)
	require.Zero(t, gate.Snapshot().Queued)
}

func TestUpstreamGateEmergencyWaiterCeiling(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{})
	gate.mu.Lock()
	for range maxAdmissionWaiters {
		waiter := &gateWaiter{host: "queued.example", waitQueue: &gate.foreground}
		waiter.element = gate.foreground.PushBack(waiter)
	}
	gate.mu.Unlock()

	_, err := gate.Acquire(context.Background(), "https://overflow.example", AdmissionForeground)
	require.ErrorIs(t, err, ErrAdmissionOverloaded)
	seconds, ok := AdmissionRetryAfterSeconds(err)
	require.True(t, ok)
	require.Equal(t, 1, seconds)
}
