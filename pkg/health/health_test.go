package health

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.NotNil(t, rh)
	require.Equal(t, RPending, rh.State)

	copy, cancel, ok := h.TryStartRefresh(rh.Path)
	require.True(t, ok)
	require.NotNil(t, cancel)
	require.Equal(t, copy.Path, rh.Path)

	h.FinishRefresh(rh.Path, rh.Generation, nil, []ProbeTarget{{Path: "dists/bookworm/InRelease"}})
	cancel()

	final, ok := h.ResourceHealth(rh.Path)
	require.True(t, ok)
	require.Equal(t, RActive, final.State)
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

	_, cancel1, ok1 := h.TryStartRefresh("dists/bookworm")
	require.True(t, ok1)

	_, _, ok2 := h.TryStartRefresh("dists/bookworm")
	require.False(t, ok2)

	cancel1()
	_, cancel3, ok3 := h.TryStartRefresh("dists/bookworm")
	require.True(t, ok3)
	cancel3()
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
	h.Start()
	require.NotNil(t, h.ctx)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h.Stop(ctx)
	require.NoError(t, err)
}

func TestStopWithDisabledProbing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")
	h.Start()
	require.Nil(t, h.ctx)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := h.Stop(ctx)
	require.NoError(t, err)
}

func TestProbeHeadSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
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
	h.probeOne(h.upstreams[server.URL])
	uh := h.upstreams[server.URL]
	require.Equal(t, SClosed, uh.State)
}

func TestProbeDirectConnectionFailure(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"http://127.0.0.1:1"}, &testStats{}, "ua")
	h.probeOne(h.upstreams["http://127.0.0.1:1"])
	uh := h.upstreams["http://127.0.0.1:1"]
	require.Equal(t, SClosed, uh.State)
	require.NotEmpty(t, uh.lastProbeErr)
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
	h.mu.Unlock()

	h.probeOne(h.upstreams[server.URL])

	state, _ = h.ResourceState(rh.Path)
	require.Equal(t, RPending, state, "successful probe should recover blocked resource to pending")
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

func TestAggregateStateString(t *testing.T) {
	require.Equal(t, "healthy", StateHealthy.String())
	require.Equal(t, "degraded", StateDegraded.String())
	require.Equal(t, "unhealthy", StateUnhealthy.String())
}

func upstreamURLs(t *testing.T, h *ServiceHealth) []string {
	t.Helper()
	urls := make([]string, 0, len(h.upstreams))
	for url := range h.upstreams {
		urls = append(urls, url)
	}
	return urls
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
