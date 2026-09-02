package runtime

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

func acquireAsync(ctx context.Context, gate *UpstreamGate, upstream string, class AdmissionClass) <-chan gateAcquireResult {
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

	queued := acquireAsync(context.Background(), gate, "https://two.example/b", AdmissionForeground)
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
	refresh := acquireAsync(context.Background(), gate, "https://refresh.example/a", AdmissionRefresh)
	require.Eventually(t, func() bool { return gate.Snapshot().RefreshQueued == 1 }, time.Second, time.Millisecond)
	foreground := acquireAsync(context.Background(), gate, "https://client.example/a", AdmissionForeground)
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

func TestUpstreamGateRefreshRunsAfterBoundedForegroundBurst(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://busy.example/a", AdmissionForeground)
	require.NoError(t, err)
	type result struct {
		class   AdmissionClass
		release func()
		err     error
	}
	granted := make(chan result, maxForegroundBurst+2)
	acquire := func(class AdmissionClass, upstream string) {
		go func() {
			release, err := gate.Acquire(context.Background(), upstream, class)
			granted <- result{class: class, release: release, err: err}
		}()
	}
	acquire(AdmissionRefresh, "https://refresh.example/metadata")
	for i := 0; i < maxForegroundBurst+1; i++ {
		acquire(AdmissionForeground, "https://client.example/object")
	}
	require.Eventually(t, func() bool {
		snapshot := gate.Snapshot()
		return snapshot.RefreshQueued == 1 && snapshot.ForegroundQueued == maxForegroundBurst+1
	}, time.Second, time.Millisecond)
	release()
	for range maxForegroundBurst - 1 {
		item := <-granted
		require.NoError(t, item.err)
		require.Equal(t, AdmissionForeground, item.class)
		item.release()
	}
	item := <-granted
	require.NoError(t, item.err)
	require.Equal(t, AdmissionRefresh, item.class)
	item.release()
}

func TestUpstreamGateDoesNotHeadOfLineBlockOtherHosts(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 2, MaxActivePerHost: 1})
	releaseA, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionForeground)
	require.NoError(t, err)
	defer releaseA()

	ctx, cancel := context.WithCancel(context.Background())
	blocked := acquireAsync(ctx, gate, "https://one.example/b", AdmissionForeground)
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
	firstA := acquireAsync(context.Background(), gate, "https://a.example/one", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 1 }, time.Second, time.Millisecond)
	secondA := acquireAsync(context.Background(), gate, "https://a.example/two", AdmissionForeground)
	require.Eventually(t, func() bool { return gate.Snapshot().Queued == 2 }, time.Second, time.Millisecond)
	requestB := acquireAsync(context.Background(), gate, "https://b.example/one", AdmissionForeground)
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

func TestUpstreamGateEnforcesPerHostRequestStartSpacing(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MinInterval: 60 * time.Millisecond})
	firstStarted := time.Now()
	release, err := gate.Acquire(context.Background(), "https://paced.example/one", AdmissionForeground)
	require.NoError(t, err)
	release()

	second := acquireAsync(context.Background(), gate, "https://paced.example/two", AdmissionForeground)
	require.Never(t, func() bool { return len(second) > 0 }, 30*time.Millisecond, 2*time.Millisecond)
	result := <-second
	require.NoError(t, result.err)
	require.GreaterOrEqual(t, time.Since(firstStarted), 50*time.Millisecond)
	result.release()
}

func TestUpstreamGateHostOverride(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{Hosts: map[string]UpstreamHostGateConfig{
		"packages.example": {MaxActive: 8, MinInterval: 25 * time.Millisecond},
	}})
	require.Equal(t, 8, gate.Snapshot().Hosts["packages.example"].MaxActive)
	require.Equal(t, 25*time.Millisecond, gate.Snapshot().Hosts["packages.example"].MinInterval)
}

func TestUpstreamGateNormalizesDefaultPorts(t *testing.T) {
	require.Equal(t, "packages.example", normalizeUpstreamHost("https://PACKAGES.example:443/path"))
	require.Equal(t, "packages.example", normalizeUpstreamHost("http://packages.example:80/path"))
	require.Equal(t, "packages.example:8443", normalizeUpstreamHost("https://packages.example:8443/path"))
	require.Equal(t, "2001:db8::1", normalizeUpstreamHost("https://[2001:db8::1]/path"))
	require.Equal(t, "[2001:db8::1]:8443", normalizeUpstreamHost("https://[2001:db8::1]:8443/path"))
}

func TestUpstreamGateRateLimitKeepsForegroundQueued(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://mirror.example/a", AdmissionForeground)
	require.NoError(t, err)
	queued := acquireAsync(context.Background(), gate, "https://mirror.example/b", AdmissionForeground)
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

func TestUpstreamGateUsesDefaultCooldownWithoutRetryAfter(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{})
	for _, value := range []string{"invalid", ""} {
		limited := gate.RateLimited("https://mirror.example/a", value)
		require.WithinDuration(t, time.Now().Add(defaultRateLimitCooldown), limited.RetryAfter, time.Second)
	}
	_, err := gate.Acquire(context.Background(), "https://mirror.example/b", AdmissionRefresh)
	var limited *UpstreamRateLimitError
	require.ErrorAs(t, err, &limited)
}

func TestUpstreamGateCanceledWaiterDoesNotConsumeCapacity(t *testing.T) {
	gate := testUpstreamGate(UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), "https://one.example/a", AdmissionRefresh)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	queued := acquireAsync(ctx, gate, "https://two.example/a", AdmissionForeground)
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
