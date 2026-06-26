package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testStats struct{}

func (s *testStats) RecordUpstream(instance, mode, method string, status int)              {}
func (s *testStats) RecordMetadataRefresh(instance, mode, result string, duration time.Duration, ready bool) {
}
func (s *testStats) SetMetadataState(instance, mode, state string, ready bool) {}

func TestNewServiceHealth(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "test-ua")
	require.NotNil(t, h)
	require.Equal(t, StateHealthy, h.AggregateState())
	require.Equal(t, 2, h.UpstreamCount())
}

func TestWeightedUpstreamsAllHealthy(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com", "https://c.example.com"}, &testStats{}, "ua")
	result := h.WeightedUpstreams(h.upstreamURLs())
	require.Len(t, result, 3)
	for _, wu := range result {
		require.Equal(t, 1.0, wu.Weight)
	}
}

func TestWeightedUpstreamsByWeight(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	h.mu.Lock()
	h.upstreams["https://a.example.com"].Weight = 1.0
	h.upstreams["https://b.example.com"].Weight = 0.3
	h.mu.Unlock()

	result := h.WeightedUpstreams(h.upstreamURLs())
	require.Len(t, result, 2)
	require.Equal(t, "https://a.example.com", result[0].URL)
	require.Equal(t, "https://b.example.com", result[1].URL)
}

func TestWeightedUpstreamsBypassWhenAllDead(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	h.mu.Lock()
	h.upstreams["https://a.example.com"].Weight = 0
	h.upstreams["https://b.example.com"].Weight = 0
	h.mu.Unlock()

	result := h.WeightedUpstreams(h.upstreamURLs())
	require.Len(t, result, 2)
	for _, wu := range result {
		require.Equal(t, 1.0, wu.Weight)
	}
}

func TestUpstreamStateTransitions(t *testing.T) {
	cfg := DefaultConfig()
	uh := newUpstreamHealth("https://a.example.com")
	require.Equal(t, SHealthy, uh.State)
	require.Equal(t, 1.0, uh.Weight)

	uh.recordFailure(nil, 0, cfg.FailureThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SHealthy, uh.State, "one failure shouldn't degrade")

	uh.recordFailure(nil, 0, cfg.FailureThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordFailure(nil, 0, cfg.FailureThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SUnhealthy, uh.State, "3 consecutive failures should go unhealthy")
	require.Equal(t, 0.0, uh.Weight)

	uh.recordProbeResult(true, 10*time.Millisecond, cfg.EwmaAlpha, cfg.FailureThreshold, cfg.SuccessThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SHalfOpen, uh.State, "probe success should go half-open")
	require.Equal(t, 0.1, uh.Weight)

	uh.recordProbeResult(true, 10*time.Millisecond, cfg.EwmaAlpha, cfg.FailureThreshold, cfg.SuccessThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SHealthy, uh.State, "2 probe successes should go healthy")
	require.Equal(t, 1.0, uh.Weight)
}

func TestUpstreamDegradeByLatency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DegradeLatency = 100 * time.Millisecond
	uh := newUpstreamHealth("https://a.example.com")

	uh.recordSuccess(200*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SDegraded, uh.State, "high latency should degrade")
	require.Less(t, uh.Weight, 1.0)
	require.Greater(t, uh.Weight, 0.0)

	uh.recordSuccess(50*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordSuccess(50*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordSuccess(50*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordSuccess(50*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordSuccess(50*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SHealthy, uh.State, "low latency should recover to healthy with EWMA alpha=0.2")
}

func TestUpgradeDegradedToUnhealthy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DegradeLatency = 100 * time.Millisecond
	uh := newUpstreamHealth("https://a.example.com")

	uh.recordSuccess(200*time.Millisecond, cfg.EwmaAlpha, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SDegraded, uh.State)

	uh.recordFailure(nil, 0, cfg.FailureThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordFailure(nil, 0, cfg.FailureThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	uh.recordFailure(nil, 0, cfg.FailureThreshold, float64(cfg.DegradeLatency), cfg.MinWeight)
	require.Equal(t, SUnhealthy, uh.State, "degraded + consecutive fails → unhealthy")
}

func TestUpstreamProbeIndexes(t *testing.T) {
	uh := newUpstreamHealth("https://a.example.com")
	require.True(t, uh.shouldProbe(30*time.Second, 60*time.Second))

	uh.LastProbeAt = time.Now()
	require.False(t, uh.shouldProbe(30*time.Second, 60*time.Second))

	uh.LastProbeAt = time.Now().Add(-90 * time.Second)
	require.True(t, uh.shouldProbe(30*time.Second, 60*time.Second))
}

func TestShouldProbeRespectsUnhealthyInterval(t *testing.T) {
	uh := newUpstreamHealth("https://a.example.com")
	uh.State = SUnhealthy

	uh.LastProbeAt = time.Now().Add(-40 * time.Second)
	require.False(t, uh.shouldProbe(30*time.Second, 60*time.Second), "should use longer unhealthy interval")

	uh.LastProbeAt = time.Now().Add(-90 * time.Second)
	require.True(t, uh.shouldProbe(30*time.Second, 60*time.Second))
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
	cfg.RemovalThreshold = 2
	cfg.MinNotFoundAge = 0
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")

	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	gen := rh.Generation

	h.FinishRefresh(rh.Path, gen, ErrResourceNotFound, nil)
	state, ok := h.ResourceState(rh.Path)
	require.True(t, ok)
	require.Equal(t, RSuspect, state, "first 404 → suspect")

	h.FinishRefresh(rh.Path, gen, ErrResourceNotFound, nil)
	_, ok = h.ResourceState(rh.Path)
	require.False(t, ok, "second 404 → removed")
}

func TestResourceForbiddenBlocked(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceForbidden, nil)
	state, ok := h.ResourceState(rh.Path)
	require.True(t, ok)
	require.Equal(t, RBlocked, state)

	nextRefresh, _ := h.ResourceNextRefresh(rh.Path)
	require.True(t, nextRefresh.After(time.Now()))
	require.True(t, nextRefresh.Before(time.Now().Add(2*time.Hour)))
}

func TestResourceTransientSuspect(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceTransient, nil)
	state, ok := h.ResourceState(rh.Path)
	require.True(t, ok)
	require.Equal(t, RSuspect, state)
}

func TestResourceTransientToBlocked(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FailureThreshold = 2
	h := New("test", "apk", cfg, []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})

	h.FinishRefresh(rh.Path, rh.Generation, ErrResourceTransient, nil)
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
	require.False(t, ok2, "concurrent refresh should be rejected")

	cancel1()
	_, cancel3, ok3 := h.TryStartRefresh("dists/bookworm")
	require.True(t, ok3, "after cancel should allow refresh")
	cancel3()
}

func TestFinishRefreshRejectsStaleGeneration(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	rh := h.AddResource("dists/bookworm", []ProbeTarget{{Path: "dists/bookworm/InRelease"}}, []string{"https://a.example.com"})
	gen := rh.Generation

	h.FinishRefresh(rh.Path, gen+999, nil, nil)

	final, _ := h.ResourceHealth(rh.Path)
	require.Equal(t, RPending, final.State, "stale generation should not update state")
}

func TestAggregateStateTransitions(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")
	require.Equal(t, StateHealthy, h.AggregateState())

	h.mu.Lock()
	h.upstreams["https://a.example.com"].State = SUnhealthy
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
	color, label, extra := h.DashboardStatus()
	require.Equal(t, "green", color)
	require.Equal(t, "healthy", label)
	require.Empty(t, extra)

	h.mu.Lock()
	h.upstreams["https://a.example.com"].State = SUnhealthy
	h.recomputeAggregateLocked()
	h.mu.Unlock()
	color, label, extra = h.DashboardStatus()
	require.Equal(t, "red", color)
	require.Equal(t, "unhealthy", label)
}

func TestRecordResultUpdatesUpstream(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	h.RecordResult("https://a.example.com", 200, 50*time.Millisecond)
	uh, _ := h.upstreams["https://a.example.com"]
	require.Equal(t, SHealthy, uh.State)

	h.RecordResult("https://a.example.com", 500, 0)
	uh, _ = h.upstreams["https://a.example.com"]
	require.Equal(t, SHealthy, uh.State, "one 500 shouldn't kill")

	h.RecordResult("https://a.example.com", 500, 0)
	h.RecordResult("https://a.example.com", 500, 0)
	uh, _ = h.upstreams["https://a.example.com"]
	require.Equal(t, SUnhealthy, uh.State, "3 consecutive 500s → unhealthy")
}

func TestRecordFailure(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com"}, &testStats{}, "ua")

	h.RecordFailure("https://a.example.com", context.DeadlineExceeded)
	uh, _ := h.upstreams["https://a.example.com"]
	require.Equal(t, SHealthy, uh.State)

	h.RecordFailure("https://a.example.com", context.DeadlineExceeded)
	h.RecordFailure("https://a.example.com", context.DeadlineExceeded)
	uh, _ = h.upstreams["https://a.example.com"]
	require.Equal(t, SUnhealthy, uh.State)
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
	require.Equal(t, SHealthy, uh.State)
}

func TestProbeHead404IsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	h.probeOne(h.upstreams[server.URL])
	uh := h.upstreams[server.URL]
	require.Equal(t, SHealthy, uh.State, "404 on HEAD → upstream is reachable")
}

func TestProbe500IsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	h := New("test", "apk", DefaultConfig(), []string{server.URL}, &testStats{}, "ua")
	h.probeOne(h.upstreams[server.URL])
	uh := h.upstreams[server.URL]
	require.Equal(t, SHealthy, uh.State, "one 500 probe shouldn't kill")
	require.Equal(t, 1, uh.ConsecutiveFails)
}

func TestProbeDirectConnectionFailure(t *testing.T) {
	h := New("test", "apk", DefaultConfig(), []string{"http://127.0.0.1:1"}, &testStats{}, "ua")

	h.probeOne(h.upstreams["http://127.0.0.1:1"])
	uh := h.upstreams["http://127.0.0.1:1"]
	require.Equal(t, SHealthy, uh.State, "one connection failure shouldn't kill")
	require.NotEmpty(t, uh.LastProbeError)
}

func TestResourceSnapshotRoundtrip(t *testing.T) {
	rh := &ResourceHealth{
		Path:    "dists/bookworm",
		State:   RActive,
		LastTargets: []ProbeTarget{{Path: "dists/bookworm/InRelease"}},
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
	require.Equal(t, 30*time.Second, cfg.ProbeInterval)
	require.Equal(t, 5*time.Second, cfg.ProbeTimeout)
	require.Equal(t, 3, cfg.FailureThreshold)
	require.Equal(t, 2, cfg.SuccessThreshold)
	require.Equal(t, 2*time.Second, cfg.DegradeLatency)
	require.Equal(t, 0.1, cfg.MinWeight)
	require.Equal(t, 0.2, cfg.EwmaAlpha)
	require.Equal(t, 3, cfg.RemovalThreshold)
	require.Equal(t, time.Hour, cfg.BlockInterval)
	require.Equal(t, 10*time.Minute, cfg.MinNotFoundAge)
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
				h.WeightedUpstreams(h.upstreamURLs())
				h.RecordResult("https://a.example.com", 200, 10*time.Millisecond)
				h.AggregateState()
				h.DashboardStatus()
			}
		}()
	}
	wg.Wait()
}

func TestUpstreamString(t *testing.T) {
	require.Equal(t, "healthy", SHealthy.String())
	require.Equal(t, "degraded", SDegraded.String())
	require.Equal(t, "unhealthy", SUnhealthy.String())
	require.Equal(t, "halfopen", SHalfOpen.String())
}

func TestResourceStateString(t *testing.T) {
	require.Equal(t, "pending", RPending.String())
	require.Equal(t, "active", RActive.String())
	require.Equal(t, "suspect", RSuspect.String())
	require.Equal(t, "blocked", RBlocked.String())
	require.Equal(t, "removed", RRemoved.String())
}

func (h *ServiceHealth) upstreamURLs() []string {
	urls := make([]string, 0, len(h.upstreams))
	for url := range h.upstreams {
		urls = append(urls, url)
	}
	return urls
}

// benchmarks for latency-sensitive paths
func BenchmarkWeightedUpstreams(b *testing.B) {
	h := New("test", "apk", DefaultConfig(), []string{"https://a.example.com", "https://b.example.com"}, &testStats{}, "ua")
	urls := h.upstreamURLs()
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
