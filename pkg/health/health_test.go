package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

type testStats struct {
	health map[string]float64
	events []string
	mu     sync.Mutex
}

func (s *testStats) RecordUpstream(instance, mode, method string, status int) {}
func (s *testStats) RecordMetadataRefresh(instance, mode, result string, duration time.Duration, ready bool) {
}
func (s *testStats) SetMetadataState(instance, mode, state string, ready bool) {}
func (s *testStats) SetUpstreamHealth(instance, mode, upstream string, state int, weight, errorRate, latency float64) {
	s.mu.Lock()
	if s.health == nil {
		s.health = map[string]float64{}
	}
	s.health["weight"] = weight
	s.health["error_rate"] = errorRate
	s.health["state"] = float64(state)
	s.mu.Unlock()
}
func (s *testStats) RecordCircuitEvent(instance, mode, upstream, event string) {
	s.mu.Lock()
	s.events = append(s.events, fmt.Sprintf("%s:%s", upstream, event))
	s.mu.Unlock()
}

func TestNewServiceHealth(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "test-ua")
	require.NotNil(t, h)
	require.Equal(t, StateHealthy, h.AggregateState())
	require.Len(t, h.upstreams, 2)
}

func TestApplyConfigPatchKeepsDefaults(t *testing.T) {
	enabled := false
	tripRate := 0.42
	defaults := DefaultConfig()
	cfg := ApplyConfigPatch(defaults, &ConfigPatch{
		Enabled:  &enabled,
		TripRate: &tripRate,
	})
	require.False(t, cfg.Enabled)
	require.Equal(t, 0.42, cfg.TripRate)
	require.Equal(t, defaults.DegradeRate, cfg.DegradeRate)
	require.Equal(t, defaults.ProbeTimeout, cfg.ProbeTimeout)
}

func TestWeightedUpstreamsAllHealthy(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com", "https://c.example.com"}, &testStats{}, "ua")
	result := h.WeightedUpstreams(upstreamURLs(t, h))
	require.Len(t, result, 3)
	for _, wu := range result {
		require.Equal(t, 1.0, wu.Weight)
	}
}

func TestWeightedUpstreamsByWeight(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	h.mu.Lock()
	h.upstreams["https://a.example.com"].weight = 1.0
	h.upstreams["https://b.example.com"].weight = 0.3
	h.mu.Unlock()

	result := h.WeightedUpstreams(upstreamURLs(t, h))
	require.Len(t, result, 2)
	require.Equal(t, "https://a.example.com", result[0].URL)
	require.Equal(t, "https://b.example.com", result[1].URL)
}

func TestWeightedUpstreamsBypassWhenAllDead(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	h.mu.Lock()
	h.upstreams["https://a.example.com"].weight = 0
	h.upstreams["https://b.example.com"].weight = 0
	h.mu.Unlock()

	result := h.WeightedUpstreams(upstreamURLs(t, h))
	require.Len(t, result, 2)
	for _, wu := range result {
		require.Equal(t, 1.0, wu.Weight)
	}
}

func TestDegradeByErrorRate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EvaluationWindow = time.Second
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)
	require.Equal(t, SClosed, uh.State)

	for range 9 {
		uh.recordSuccess(10*time.Millisecond, cfg)
	}
	require.Equal(t, SClosed, uh.State)

	uh.recordFailure(fmt.Errorf("err"), cfg)
	require.Equal(t, SClosed, uh.State)

	uh.recordFailure(fmt.Errorf("err"), cfg)
	require.Equal(t, SDegraded, uh.State)
}

func TestTripByErrorRate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EvaluationWindow = time.Second
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 7 {
		uh.recordSuccess(10*time.Millisecond, cfg)
	}
	for range 3 {
		uh.recordFailure(fmt.Errorf("err"), cfg)
	}
	require.Equal(t, SOpen, uh.State)
	require.Equal(t, 0.0, uh.weight)
}

func TestCanaryRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EvaluationWindow = time.Second
	cfg.CanaryCooldown = 0
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 7 {
		uh.recordSuccess(10*time.Millisecond, cfg)
	}
	for range 3 {
		uh.recordFailure(fmt.Errorf("err"), cfg)
	}
	require.Equal(t, SOpen, uh.State)

	uh.recordSuccess(10*time.Millisecond, cfg)
	require.Equal(t, SHalfOpen, uh.State)
	require.Equal(t, 0.1, uh.weight)

	uh.recordSuccess(10*time.Millisecond, cfg)
	require.InDelta(t, 0.2, uh.weight, 0.001)
	uh.recordSuccess(10*time.Millisecond, cfg)
	require.InDelta(t, 0.3, uh.weight, 0.001)
	uh.recordSuccess(10*time.Millisecond, cfg)
	require.InDelta(t, 0.4, uh.weight, 0.001)
	uh.recordSuccess(10*time.Millisecond, cfg)
	require.Equal(t, SClosed, uh.State)
	require.Equal(t, 1.0, uh.weight)
}

func TestCanaryFailureRevertsToOpen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EvaluationWindow = time.Second
	cfg.CanaryCooldown = 0
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 7 {
		uh.recordSuccess(10*time.Millisecond, cfg)
	}
	for range 3 {
		uh.recordFailure(fmt.Errorf("err"), cfg)
	}
	require.Equal(t, SOpen, uh.State)

	uh.recordSuccess(10*time.Millisecond, cfg)
	require.Equal(t, SHalfOpen, uh.State)

	uh.recordFailure(fmt.Errorf("err"), cfg)
	require.Equal(t, SOpen, uh.State)
	require.Equal(t, 0.0, uh.weight)
}

func TestOpenStateFailureStaysOpen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EvaluationWindow = time.Second
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 7 {
		uh.recordSuccess(10*time.Millisecond, cfg)
	}
	for range 3 {
		uh.recordFailure(fmt.Errorf("err"), cfg)
	}
	require.Equal(t, SOpen, uh.State)

	uh.recordFailure(fmt.Errorf("err"), cfg)
	require.Equal(t, SOpen, uh.State, "failure while open must stay open, not jump to closed")
}

func TestDegradeByLatency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DegradeLatency = 100 * time.Millisecond
	cfg.EvaluationWindow = time.Second
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 10 {
		uh.recordSuccess(200*time.Millisecond, cfg)
	}
	require.Equal(t, SDegraded, uh.State)
	require.Less(t, uh.weight, 1.0)
	require.Greater(t, uh.weight, 0.0)

	for range 20 {
		uh.recordSuccess(50*time.Millisecond, cfg)
	}
	require.Equal(t, SClosed, uh.State)
}

func TestDegradeByLatencyNeedsMinSamples(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DegradeLatency = 100 * time.Millisecond
	cfg.EvaluationWindow = time.Second
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 9 {
		uh.recordSuccess(200*time.Millisecond, cfg)
	}
	require.Equal(t, SClosed, uh.State, "latency samples < minSampleSize should not degrade")
}

func TestCanaryCooldownNotElapsed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EvaluationWindow = time.Second
	cfg.CanaryCooldown = time.Hour
	uh := newUpstreamHealth("https://a.example.com", cfg.EvaluationWindow)

	for range 7 {
		uh.recordSuccess(10*time.Millisecond, cfg)
	}
	for range 3 {
		uh.recordFailure(fmt.Errorf("err"), cfg)
	}
	require.Equal(t, SOpen, uh.State)

	uh.recordSuccess(10*time.Millisecond, cfg)
	require.Equal(t, SOpen, uh.State)
}

func TestResourceStateTransitions(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	require.Equal(t, RPending, rh.State)

	copy, cancel, err := h.TryStartRefresh(rh.Path, time.Now())
	require.NoError(t, err)
	require.NotNil(t, cancel)
	require.Equal(t, copy.Path, rh.Path)

	h.FinishRefresh(rh.Path, copy.Generation, nil, []ProbeTarget{{Path: "dists/bookworm/InRelease"}})
	cancel()

	final, ok := h.ResourceHealth(rh.Path)
	require.True(t, ok)
	require.Equal(t, RActive, final.State)
}

func TestResourceHealthReturnsSnapshot(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource(
		"dists/bookworm",
		[]ProbeTarget{{Path: "dists/bookworm/InRelease"}},
		[]string{"https://a.example.com"},
	)
	rh.State = RRemoved
	rh.LastTargets[0].Path = "mutated"
	rh.UpstreamURLs[0] = "https://mutated.example.com"

	final, ok := h.ResourceHealth("dists/bookworm")
	require.True(t, ok)
	require.Equal(t, RPending, final.State)
	require.Equal(t, "dists/bookworm/InRelease", final.LastTargets[0].Path)
	require.Equal(t, "https://a.example.com", final.UpstreamURLs[0])
}

func TestResourceSnapshotRestoresRetryStateAndClearsRefreshing(t *testing.T) {
	nextRefreshAt := time.Now().Add(time.Minute).UTC().Round(time.Second)
	firstNotFoundAt := time.Now().Add(-time.Minute).UTC().Round(time.Second)
	snapshot := ResourceSnapshot{
		Path:                "dists/bookworm",
		State:               "blocked",
		Refreshing:          true,
		NextRefreshAt:       nextRefreshAt,
		FirstNotFoundAt:     firstNotFoundAt,
		ConsecutiveNotFound: 3,
		LastTargets:         []ProbeTarget{{Path: "dists/bookworm/InRelease"}},
		UpstreamURLs:        []string{"https://a.example.com"},
	}

	rh := ResourceFromSnapshot(snapshot)

	require.Equal(t, RBlocked, rh.State)
	require.False(t, rh.Refreshing)
	require.Equal(t, nextRefreshAt, rh.NextRefreshAt)
	require.Equal(t, firstNotFoundAt, rh.FirstNotFoundAt)
	require.Equal(t, 3, rh.ConsecutiveNotFound)
	require.Equal(t, []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, rh.LastTargets)
	require.Equal(t, []string{"https://a.example.com"}, rh.UpstreamURLs)
}

func TestAddResourceUpdatesExistingTargetsWithoutResettingState(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", nil, nil)
	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceForbidden, nil)

	updated := h.AddResource(
		"dists/bookworm",
		[]ProbeTarget{{Path: "dists/bookworm/InRelease"}},
		[]string{"https://b.example.com"},
	)

	require.Equal(t, RBlocked, updated.State)
	require.Equal(t, []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, updated.LastTargets)
	require.Equal(t, []string{"https://b.example.com"}, updated.UpstreamURLs)
}

func TestResourceNotFoundRemoval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ResourceRemoveCount = 2
	cfg.ResourceRemoveAge = 0
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")

	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	gen := rh.Generation

	h.FinishRefresh(rh.Path, gen, ErrResourceNotFound, nil)
	state, ok := h.ResourceState(rh.Path)
	require.True(t, ok)
	require.Equal(t, RSuspect, state)

	h.FinishRefresh(rh.Path, gen, ErrResourceNotFound, nil)
	_, ok = h.ResourceState(rh.Path)
	require.False(t, ok)
}

func TestResourceSuccessClearsFirstNotFoundWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ResourceRemoveCount = 2
	cfg.ResourceRemoveAge = time.Hour
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceNotFound, nil)
	suspect, ok := h.ResourceHealth(rh.Path)
	require.True(t, ok)
	require.Equal(t, RSuspect, suspect.State)
	require.NotZero(t, suspect.FirstNotFoundAt)

	h.FinishRefresh(rh.Path, rh.Generation, nil, nil)
	active, ok := h.ResourceHealth(rh.Path)
	require.True(t, ok)
	require.Equal(t, RActive, active.State)
	require.Zero(t, active.FirstNotFoundAt)
	require.Zero(t, active.ConsecutiveNotFound)
}

func TestResourceForbiddenBlocked(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceForbidden, nil)
	state, ok := h.ResourceState(rh.Path)
	require.True(t, ok)
	require.Equal(t, RBlocked, state)
}

func TestResourceTransientToBlocked(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	for range resourceFailCount - 1 {
		h.FinishRefresh(rh.Path, rh.Generation, ErrResourceTransient, nil)
	}
	state, _ := h.ResourceState(rh.Path)
	require.Equal(t, RSuspect, state)

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceTransient, nil)
	state, _ = h.ResourceState(rh.Path)
	require.Equal(t, RBlocked, state)
}

func TestTryStartRefreshRejectsConcurrent(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	_, cancel1, err := h.TryStartRefresh("dists/bookworm", time.Now())
	require.NoError(t, err)

	_, _, err = h.TryStartRefresh("dists/bookworm", time.Now())
	require.ErrorIs(t, err, ErrRefreshAlreadyRunning)

	cancel1()
	_, cancel3, err := h.TryStartRefresh("dists/bookworm", time.Now())
	require.NoError(t, err)
	cancel3()
}

func TestTryStartRefreshRejectsBlockedResourceUntilWindow(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceForbidden, nil)

	_, _, err := h.TryStartRefresh(rh.Path, time.Now())
	require.ErrorIs(t, err, ErrRefreshBlockedUntil)

	blockedUntil, ok := h.RefreshBlockedUntil(rh.Path)
	require.True(t, ok)
	_, cancel, err := h.TryStartRefresh(rh.Path, blockedUntil)
	require.NoError(t, err)
	cancel()
}

func TestRemovedResourcePublishesRemovedEvent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ResourceRemoveCount = 1
	cfg.ResourceRemoveAge = 0
	b := bus.New()
	ch := b.Subscribe(bus.EventMetadataRemoved)
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")
	h.SetBus(b)
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceNotFound, nil)

	select {
	case evt := <-ch:
		payload, ok := evt.Payload.(bus.MetadataRemovedPayload)
		require.True(t, ok)
		require.Equal(t, "test", payload.Instance)
		require.Equal(t, rh.Path, payload.RootID)
	case <-time.After(time.Second):
		t.Fatal("expected metadata removed event")
	}
}

func TestFinishRefreshRejectsStaleGeneration(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	gen := rh.Generation

	h.FinishRefresh(rh.Path, gen+999, nil, nil)

	final, _ := h.ResourceHealth(rh.Path)
	require.Equal(t, RPending, final.State)
}

func TestAggregateStateTransitions(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	require.Equal(t, StateHealthy, h.AggregateState())

	h.mu.Lock()
	h.upstreams["https://a.example.com"].State = SOpen
	h.recomputeAggregateLocked()
	h.mu.Unlock()
	require.Equal(t, StateUnhealthy, h.AggregateState())

	h.mu.Lock()
	h.upstreams["https://a.example.com"].State = SDegraded
	h.recomputeAggregateLocked()
	h.mu.Unlock()
	require.Equal(t, StateDegraded, h.AggregateState())
}

func TestDashboardStatus(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	color, label, _ := h.DashboardStatus()
	require.Equal(t, "green", color)
	require.Equal(t, "healthy", label)

	h.mu.Lock()
	h.upstreams["https://a.example.com"].State = SOpen
	h.recomputeAggregateLocked()
	h.mu.Unlock()
	color, label, _ = h.DashboardStatus()
	require.Equal(t, "red", color)
	require.Equal(t, "unhealthy", label)
}

func TestRecordResultUpdatesUpstream(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	for range 20 {
		h.RecordResult("https://a.example.com", 200, 50*time.Millisecond)
	}
	uh := h.upstreams["https://a.example.com"]
	require.Equal(t, SClosed, uh.State)

	h.RecordResult("https://a.example.com", 500, 0)
	uh = h.upstreams["https://a.example.com"]
	require.Equal(t, SClosed, uh.State)

	for range 9 {
		h.RecordResult("https://a.example.com", 500, 0)
	}
	uh = h.upstreams["https://a.example.com"]
	require.Equal(t, SOpen, uh.State)
}

func TestRecordResultCountsNonNotFoundClientErrorsAsFailures(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	for range 20 {
		h.RecordResult("https://a.example.com", http.StatusOK, 50*time.Millisecond)
	}
	for range 10 {
		h.RecordResult("https://a.example.com", http.StatusBadRequest, 0)
	}

	uh := h.upstreams["https://a.example.com"]
	require.Equal(t, SOpen, uh.State)
}

func TestRecordResultDoesNotCountNotFoundAsFailure(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	for range 20 {
		h.RecordResult("https://a.example.com", http.StatusNotFound, 50*time.Millisecond)
	}

	uh := h.upstreams["https://a.example.com"]
	require.Equal(t, SClosed, uh.State)
	require.Zero(t, uh.window.errorRate())
}

func TestRecordFailure(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	for range 20 {
		h.RecordResult("https://a.example.com", 200, 10*time.Millisecond)
	}
	h.RecordFailure("https://a.example.com", context.DeadlineExceeded)
	uh := h.upstreams["https://a.example.com"]
	require.Equal(t, SClosed, uh.State)

	for range 9 {
		h.RecordFailure("https://a.example.com", context.DeadlineExceeded)
	}
	uh = h.upstreams["https://a.example.com"]
	require.Equal(t, SOpen, uh.State)
}

func TestContextCanceledNotRecorded(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	for range 20 {
		h.RecordResult("https://a.example.com", 200, 10*time.Millisecond)
	}
	for range 5 {
		h.RecordFailure("https://a.example.com", context.Canceled)
	}
	uh := h.upstreams["https://a.example.com"]
	require.Equal(t, SClosed, uh.State, "context.Canceled must not count as failure")
}

func TestCircuitEventEmitted(t *testing.T) {
	ts := &testStats{}
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, ts, "ua")

	for range 7 {
		h.RecordResult("https://a.example.com", 200, 10*time.Millisecond)
	}
	for range 3 {
		h.RecordResult("https://a.example.com", 500, 0)
	}

	ts.mu.Lock()
	require.NotEmpty(t, ts.events)
	ts.mu.Unlock()
}

func TestStopCleanShutdown(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	parent, parentCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer parentCancel()
	h.Start(parent)
	require.NotNil(t, h.ctx)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h.Stop(ctx)
	require.NoError(t, err)
}

func TestStartIsIdempotentWhileRunning(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	firstParent := context.Background()
	secondParent, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()

	h.Start(firstParent)
	firstCtx := h.ctx
	h.Start(secondParent)

	require.True(t, firstCtx == h.ctx)
	secondCancel()
	require.NoError(t, h.Stop(context.Background()))
}

func TestStopWithDisabledProbing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")
	parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
	defer parentCancel()
	h.Start(parent)
	require.Nil(t, h.ctx)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := h.Stop(ctx)
	require.NoError(t, err)
}

func TestActiveProbeWaitChannelIsRecreated(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	require.True(t, h.beginActiveProbe())
	h.finishActiveProbe()

	h.lifecycleMu.Lock()
	require.Zero(t, h.activeProbes)
	require.Nil(t, h.activeDone)
	h.lifecycleMu.Unlock()

	require.True(t, h.beginActiveProbe())
	h.lifecycleMu.Lock()
	require.Equal(t, 1, h.activeProbes)
	require.NotNil(t, h.activeDone)
	h.lifecycleMu.Unlock()
	h.finishActiveProbe()
}

func TestParentContextStopsActiveProbes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeInterval = 10 * time.Millisecond
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")
	parent, parentCancel := context.WithCancel(context.Background())
	h.Start(parent)
	require.NotNil(t, h.ctx)
	parentCancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.Stop(ctx))
}

func TestProbeHeadSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	h.AddResource("repo", []ProbeTarget{{Path: "probe"}}, []string{server.URL})
	h.MarkResourceActive("repo", []ProbeTarget{{Path: "probe"}})
	h.probeOne(h.upstreams[server.URL])
	uh := h.upstreams[server.URL]
	require.Equal(t, SClosed, uh.State)
}

func TestProbeHead404IsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	h.AddResource("repo", []ProbeTarget{{Path: "probe"}}, []string{server.URL})
	h.MarkResourceActive("repo", []ProbeTarget{{Path: "probe"}})
	h.probeOne(h.upstreams[server.URL])
	uh := h.upstreams[server.URL]
	require.Equal(t, SClosed, uh.State)
}

func TestProbeDirectConnectionFailure(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"http://127.0.0.1:1"}, &testStats{}, "ua")
	h.AddResource("repo", []ProbeTarget{{Path: "probe"}}, []string{"http://127.0.0.1:1"})
	h.MarkResourceActive("repo", []ProbeTarget{{Path: "probe"}})
	h.probeOne(h.upstreams["http://127.0.0.1:1"])
	uh := h.upstreams["http://127.0.0.1:1"]
	require.Equal(t, SClosed, uh.State)
	require.NotEmpty(t, uh.lastProbeErr)
}

func TestProbeFailurePublishesOriginalErrorDetail(t *testing.T) {
	b := bus.New()
	h := New("test", "apk", DefaultConfig(), []string{"http://127.0.0.1:1"}, &testStats{}, "ua")
	h.AddResource("repo", []ProbeTarget{{Path: "probe"}}, []string{"http://127.0.0.1:1"})
	h.MarkResourceActive("repo", []ProbeTarget{{Path: "probe"}})
	h.SetBus(b)
	ch := b.Subscribe(bus.EventUpstreamState)

	for i := 0; i < minSampleSize; i++ {
		h.probeOne(h.upstreams["http://127.0.0.1:1"])
	}

	select {
	case evt := <-ch:
		payload, ok := evt.Payload.(bus.UpstreamStatePayload)
		require.True(t, ok)
		require.Equal(t, "failure", payload.Reason)
		require.NotEmpty(t, payload.Detail)
	case <-time.After(time.Second):
		t.Fatal("expected upstream state event")
	}
}

func TestProbeHTTP5xxPublishesStatusDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	b := bus.New()
	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	h.AddResource("repo", []ProbeTarget{{Path: "probe"}}, []string{server.URL})
	h.MarkResourceActive("repo", []ProbeTarget{{Path: "probe"}})
	h.SetBus(b)
	ch := b.Subscribe(bus.EventUpstreamState)

	for i := 0; i < minSampleSize; i++ {
		h.probeOne(h.upstreams[server.URL])
	}

	select {
	case evt := <-ch:
		payload, ok := evt.Payload.(bus.UpstreamStatePayload)
		require.True(t, ok)
		require.Equal(t, "failure", payload.Reason)
		require.Equal(t, "HTTP 502", payload.Detail)
	case <-time.After(time.Second):
		t.Fatal("expected upstream state event")
	}
}

func TestProbeRecoversBlockedResource(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceForbidden, nil)

	state, _ := h.ResourceState(rh.Path)
	require.Equal(t, RBlocked, state)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h.mu.Lock()
	h.upstreams[server.URL] = newUpstreamHealth(server.URL, time.Minute)
	h.resources["dists/bookworm"].UpstreamURLs = []string{server.URL}
	h.resources["dists/bookworm"].FirstNotFoundAt = time.Now().Add(-time.Hour)
	h.resources["dists/bookworm"].ConsecutiveNotFound = 2
	h.mu.Unlock()

	h.probeOne(h.upstreams[server.URL])

	recovered, ok := h.ResourceHealth(rh.Path)
	require.True(t, ok)
	require.Equal(t, RPending, recovered.State, "successful probe should recover blocked resource to pending")
	require.Zero(t, recovered.FirstNotFoundAt)
	require.Zero(t, recovered.ConsecutiveNotFound)
}

func TestProbeNonNotFoundClientErrorIsUpstreamFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	addActiveProbeResource(h, "repo", "probe", server.URL)
	h.probeOne(h.upstreams[server.URL])

	require.Equal(t, "HTTP 400", h.upstreams[server.URL].lastProbeErr)
}

func TestProbeNotFoundIsNotUpstreamFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	addActiveProbeResource(h, "repo", "probe", server.URL)
	h.probeOne(h.upstreams[server.URL])

	require.Empty(t, h.upstreams[server.URL].lastProbeErr)
}

func TestProbeHeadMethodNotAllowedFallsBackToRangeGet(t *testing.T) {
	var methods []string
	var rangeHeader string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		rangeHeader = r.Header.Get("Range")
		mu.Unlock()
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	addActiveProbeResource(h, "repo", "probe", server.URL)
	h.probeOne(h.upstreams[server.URL])

	mu.Lock()
	require.Equal(t, []string{http.MethodHead, http.MethodGet}, methods)
	require.Equal(t, "bytes=0-0", rangeHeader)
	mu.Unlock()
	require.True(t, h.upstreams[server.URL].rangeProbeOnly)
	require.Empty(t, h.upstreams[server.URL].lastProbeErr)
}

func TestProbeSchedulerThrottlesSharedHost(t *testing.T) {
	requests := make(chan time.Time, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 80 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	cfg := fastProbeConfig()
	h1 := New("one", "apk", cfg, []string{server.URL + "/one"}, nil, "ua")
	h2 := New("two", "apk", cfg, []string{server.URL + "/two"}, nil, "ua")
	addActiveProbeResource(h1, "repo-one", "probe", server.URL+"/one")
	addActiveProbeResource(h2, "repo-two", "probe", server.URL+"/two")
	h1.SetProbeScheduler(s)
	h2.SetProbeScheduler(s)
	h1.Start(ctx)
	h2.Start(ctx)
	defer func() { require.NoError(t, h1.Stop(context.Background())) }()
	defer func() { require.NoError(t, h2.Stop(context.Background())) }()

	times := collectProbeTimes(t, requests, 2, time.Second)
	require.GreaterOrEqual(t, times[1].Sub(times[0]), 60*time.Millisecond)
}

func TestProbeSchedulerRunsDifferentHostsIndependently(t *testing.T) {
	requests := make(chan time.Time, 4)
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer serverB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 200 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	cfg := fastProbeConfig()
	h1 := New("one", "apk", cfg, []string{serverA.URL}, nil, "ua")
	h2 := New("two", "apk", cfg, []string{serverB.URL}, nil, "ua")
	addActiveProbeResource(h1, "repo-one", "probe", serverA.URL)
	addActiveProbeResource(h2, "repo-two", "probe", serverB.URL)
	h1.SetProbeScheduler(s)
	h2.SetProbeScheduler(s)
	h1.Start(ctx)
	h2.Start(ctx)
	defer func() { require.NoError(t, h1.Stop(context.Background())) }()
	defer func() { require.NoError(t, h2.Stop(context.Background())) }()

	times := collectProbeTimes(t, requests, 2, time.Second)
	require.Less(t, times[1].Sub(times[0]), 120*time.Millisecond)
}

func TestProbeSchedulerSkipsServicesWithoutTargets(t *testing.T) {
	requests := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 5 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	h := New("test", "apk", fastProbeConfig(), []string{server.URL}, nil, "ua")
	h.SetProbeScheduler(s)
	h.Start(ctx)
	defer func() { require.NoError(t, h.Stop(context.Background())) }()

	select {
	case <-requests:
		t.Fatal("unexpected active probe without metadata target")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestProbeSchedulerWakesWhenTargetIsAdded(t *testing.T) {
	requests := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 5 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	h := New("test", "apk", fastProbeConfig(), []string{server.URL}, nil, "ua")
	h.SetProbeScheduler(s)
	h.Start(ctx)
	defer func() { require.NoError(t, h.Stop(context.Background())) }()

	addActiveProbeResource(h, "repo", "probe", server.URL)
	collectProbeTimes(t, requests, 1, time.Second)
}

func TestProbeSchedulerUsesCanaryCooldownForOpenUpstream(t *testing.T) {
	requests := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 5 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	cfg := fastProbeConfig()
	cfg.ProbeInterval = 200 * time.Millisecond
	cfg.CanaryCooldown = 5 * time.Millisecond
	h := New("test", "apk", cfg, []string{server.URL}, nil, "ua")
	addActiveProbeResource(h, "repo", "probe", server.URL)
	h.mu.Lock()
	h.upstreams[server.URL].State = SOpen
	h.upstreams[server.URL].lastProbeAt = time.Now()
	h.mu.Unlock()
	h.SetProbeScheduler(s)
	h.Start(ctx)
	defer func() { require.NoError(t, h.Stop(context.Background())) }()

	collectProbeTimes(t, requests, 1, 100*time.Millisecond)
}

func TestProbeSchedulerRecomputesNextProbeAfterStateChange(t *testing.T) {
	cfg := fastProbeConfig()
	cfg.ProbeInterval = time.Minute
	cfg.CanaryCooldown = 5 * time.Millisecond
	h := New("test", "apk", cfg, []string{"https://example.com/repo"}, nil, "ua")
	addActiveProbeResource(h, "repo", "probe", "https://example.com/repo")
	h.mu.Lock()
	h.upstreams["https://example.com/repo"].State = SClosed
	h.upstreams["https://example.com/repo"].lastProbeAt = time.Now()
	h.mu.Unlock()

	key := probeJobKey{service: h, upstreamURL: "https://example.com/repo"}
	s := &ProbeScheduler{
		config:   probeSchedulerConfig{MinHostInterval: 5 * time.Millisecond},
		services: map[*ServiceHealth]struct{}{h: struct{}{}},
		jobs: map[probeJobKey]*probeJob{
			key: &probeJob{
				key:     key,
				hostKey: "https://example.com",
				nextAt:  time.Now().Add(cfg.CanaryCooldown),
			},
		},
		hosts: map[string]*probeHost{},
	}
	s.reconcileLocked(time.Now())
	nextAt := s.jobs[key].nextAt

	require.WithinDuration(t, time.Now().Add(cfg.ProbeInterval), nextAt, time.Second)
}

func TestProbeSchedulerUnregisterStopsFutureProbes(t *testing.T) {
	requests := make(chan time.Time, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 5 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	h := New("test", "apk", fastProbeConfig(), []string{server.URL}, nil, "ua")
	addActiveProbeResource(h, "repo", "probe", server.URL)
	h.SetProbeScheduler(s)
	h.Start(ctx)
	collectProbeTimes(t, requests, 1, time.Second)
	require.NoError(t, h.Stop(context.Background()))

	select {
	case <-requests:
		t.Fatal("unexpected probe after service stop")
	case <-time.After(40 * time.Millisecond):
	}
}

func TestProbeSchedulerRoundRobinsSharedHost(t *testing.T) {
	paths := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newProbeScheduler(ctx, probeSchedulerConfig{MinHostInterval: 20 * time.Millisecond})
	defer func() { require.NoError(t, s.Stop(context.Background())) }()

	cfg := fastProbeConfig()
	h1 := New("one", "apk", cfg, []string{server.URL + "/one"}, nil, "ua")
	h2 := New("two", "apk", cfg, []string{server.URL + "/two"}, nil, "ua")
	addActiveProbeResource(h1, "repo-one", "probe", server.URL+"/one")
	addActiveProbeResource(h2, "repo-two", "probe", server.URL+"/two")
	h1.SetProbeScheduler(s)
	h2.SetProbeScheduler(s)
	h1.Start(ctx)
	h2.Start(ctx)
	defer func() { require.NoError(t, h1.Stop(context.Background())) }()
	defer func() { require.NoError(t, h2.Stop(context.Background())) }()

	seen := map[string]bool{}
	for _, item := range collectProbePaths(t, paths, 4, time.Second) {
		seen[item] = true
	}
	require.True(t, seen["/one/probe"], "expected first service to be probed")
	require.True(t, seen["/two/probe"], "expected second service to be probed")
}

func TestResourceSnapshotRoundtrip(t *testing.T) {
	rh := &ResourceHealth{
		Path:         "dists/bookworm",
		State:        RActive,
		LastTargets:  []ProbeTarget{{Path: "dists/bookworm/InRelease"}},
		UpstreamURLs: []string{"https://a.example.com"},
	}
	snap := rh.Snapshot()
	restored := ResourceFromSnapshot(snap)
	require.Equal(t, rh.Path, restored.Path)
	require.Equal(t, rh.State, restored.State)
}

func TestConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()
	require.True(t, cfg.Enabled)
	require.Equal(t, 2*time.Minute, cfg.ProbeInterval)
	require.Equal(t, 5*time.Second, cfg.ProbeTimeout)
	require.Equal(t, 0.1, cfg.DegradeRate)
	require.Equal(t, 0.3, cfg.TripRate)
	require.Equal(t, 2*time.Minute, cfg.EvaluationWindow)
	require.Equal(t, 2*time.Second, cfg.DegradeLatency)
	require.Equal(t, 0.1, cfg.MinWeight)
	require.Equal(t, 30*time.Second, cfg.CanaryCooldown)
	require.Equal(t, 0.1, cfg.CanaryStep)
	require.Equal(t, 2*time.Minute, cfg.ResourceBlockInterval)
	require.Equal(t, 5*time.Minute, cfg.ResourceRemoveAge)
	require.Equal(t, 5, cfg.ResourceRemoveCount)
}

func TestValidateConfigRejectsUnsafeProbeInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeInterval = time.Second

	require.ErrorContains(t, ValidateConfig(cfg), "probe_interval")
}

func TestValidateConfigRejectsProbeTimeoutLongerThanInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeTimeout = cfg.ProbeInterval

	require.ErrorContains(t, ValidateConfig(cfg), "probe_timeout")
}

func TestRateWindowBasics(t *testing.T) {
	rw := newRateWindow(time.Second)

	for range 11 {
		rw.record(true)
	}
	require.Equal(t, 0.0, rw.errorRate())

	rw.record(false)
	require.Greater(t, rw.errorRate(), 0.0)
}

func TestRateWindowMinSamples(t *testing.T) {
	rw := newRateWindow(time.Minute)
	rw.record(false)
	require.Equal(t, 0.0, rw.errorRate())
}

func TestConcurrentAccessSafety(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				h.WeightedUpstreams(upstreamURLs(t, h))
				h.RecordResult("https://a.example.com", 200, 10*time.Millisecond)
				h.AggregateState()
				h.DashboardStatus()
			}
		}()
	}
	wg.Wait()
}

func TestUpstreamString(t *testing.T) {
	require.Equal(t, "closed", SClosed.String())
	require.Equal(t, "degraded", SDegraded.String())
	require.Equal(t, "open", SOpen.String())
	require.Equal(t, "halfopen", SHalfOpen.String())
}

func TestResourceStateString(t *testing.T) {
	require.Equal(t, "pending", RPending.String())
	require.Equal(t, "active", RActive.String())
	require.Equal(t, "suspect", RSuspect.String())
	require.Equal(t, "blocked", RBlocked.String())
	require.Equal(t, "removed", RRemoved.String())
}

func TestMetricsEmission(t *testing.T) {
	ts := &testStats{}
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, ts, "ua")

	h.RecordResult("https://a.example.com", 200, 50*time.Millisecond)

	ts.mu.Lock()
	require.Equal(t, 1.0, ts.health["weight"])
	require.Equal(t, 0.0, ts.health["error_rate"])
	require.Greater(t, ts.health["state"], -1.0)
	ts.mu.Unlock()
}

func TestNilStatsRecorderDoesNotPanic(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, nil, "ua")
	require.NotPanics(t, func() {
		h.RecordResult("https://a.example.com", 200, time.Millisecond)
		h.RecordFailure("https://a.example.com", errors.New("boom"))
	})
}

func TestAggregateStateString(t *testing.T) {
	require.Equal(t, "healthy", StateHealthy.String())
	require.Equal(t, "degraded", StateDegraded.String())
	require.Equal(t, "unhealthy", StateUnhealthy.String())
}

func TestRecordFailurePublishesUpstreamStateEvent(t *testing.T) {
	b := bus.New()
	h := New("repo", "deb", DefaultConfig(), []string{"https://a.example.com"}, nil, "test-agent")
	h.SetBus(b)
	ch := b.Subscribe(bus.EventUpstreamState)

	for i := 0; i < minSampleSize; i++ {
		h.RecordFailure("https://a.example.com", errors.New("boom"))
	}

	select {
	case evt := <-ch:
		payload, ok := evt.Payload.(bus.UpstreamStatePayload)
		require.True(t, ok)
		require.Equal(t, "repo", payload.Instance)
		require.Equal(t, "deb", payload.Mode)
		require.Equal(t, "https://a.example.com", payload.Upstream)
		require.Equal(t, "failure", payload.Reason)
		require.NotEmpty(t, payload.To)
	case <-time.After(time.Second):
		t.Fatal("expected upstream state event")
	}
}

func upstreamURLs(t *testing.T, h *ServiceHealth) []string {
	t.Helper()
	urls := make([]string, 0, len(h.upstreams))
	for url := range h.upstreams {
		urls = append(urls, url)
	}
	return urls
}

func fastProbeConfig() Config {
	cfg := DefaultConfig()
	cfg.ProbeInterval = 5 * time.Millisecond
	cfg.ProbeTimeout = 500 * time.Millisecond
	cfg.CanaryCooldown = 5 * time.Millisecond
	cfg.EvaluationWindow = time.Second
	return cfg
}

func addActiveProbeResource(h *ServiceHealth, rootID, targetPath, upstreamURL string) {
	targets := []ProbeTarget{{Path: targetPath}}
	h.AddResource(rootID, targets, []string{upstreamURL})
	h.MarkResourceActive(rootID, targets)
}

func collectProbeTimes(t *testing.T, ch <-chan time.Time, count int, timeout time.Duration) []time.Time {
	t.Helper()
	deadline := time.After(timeout)
	result := make([]time.Time, 0, count)
	for len(result) < count {
		select {
		case item := <-ch:
			result = append(result, item)
		case <-deadline:
			t.Fatalf("timed out waiting for %d probe requests, got %d", count, len(result))
		}
	}
	return result
}

func collectProbePaths(t *testing.T, ch <-chan string, count int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.After(timeout)
	result := make([]string, 0, count)
	for len(result) < count {
		select {
		case item := <-ch:
			if strings.HasSuffix(item, "/probe") {
				result = append(result, item)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %d probe requests, got %d", count, len(result))
		}
	}
	return result
}

func BenchmarkWeightedUpstreams(b *testing.B) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	urls := make([]string, 0, len(h.upstreams))
	for url := range h.upstreams {
		urls = append(urls, url)
	}
	b.ResetTimer()
	for b.Loop() {
		h.WeightedUpstreams(urls)
	}
}

func BenchmarkRecordResult(b *testing.B) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	b.ResetTimer()
	for b.Loop() {
		h.RecordResult("https://a.example.com", 200, time.Millisecond)
	}
}

func BenchmarkAggregateState(b *testing.B) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	b.ResetTimer()
	for b.Loop() {
		h.AggregateState()
	}
}
