package health

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type observationStats struct {
	errorRate float64
	latency   float64
	calls     int
}

func (*observationStats) RecordUpstream(string, string, string, int) {}
func (*observationStats) RecordMetadataRefresh(string, string, string, time.Duration, bool) {
}
func (*observationStats) SetMetadataState(string, string, string, bool) {}
func (s *observationStats) SetUpstreamObservation(_, _, _ string, errorRate, latency float64) {
	s.errorRate = errorRate
	s.latency = latency
	s.calls++
}

func TestConfigOnlyExposesObservationalAndResourceRemovalSettings(t *testing.T) {
	defaults := DefaultConfig()
	disabled := false
	removeCount := 3
	removeAge := 0 * time.Second
	cfg := ApplyConfigPatch(defaults, &ConfigPatch{
		Enabled:             &disabled,
		ResourceRemoveCount: &removeCount,
		ResourceRemoveAge:   &removeAge,
	})
	require.False(t, cfg.Enabled)
	require.Equal(t, 3, cfg.ResourceRemoveCount)
	require.Zero(t, cfg.ResourceRemoveAge)
	require.NoError(t, ValidateConfig(cfg))
}

func TestPassiveObservationNeverRemovesOrWeightsUpstream(t *testing.T) {
	stats := &observationStats{}
	h := New("repo", "rpm", DefaultConfig(), []string{"https://repo.example"}, stats)

	h.RecordResult("https://repo.example", 200, 20*time.Millisecond)
	h.RecordResult("https://repo.example", 503, 40*time.Millisecond)

	require.Equal(t, 2, stats.calls)
	require.InDelta(t, 0.5, stats.errorRate, 0.001)
	require.Equal(t, 0.02, stats.latency)
	require.Contains(t, h.upstreams, "https://repo.example")
}

func TestCanceledRequestDoesNotAffectPassiveHealth(t *testing.T) {
	stats := &observationStats{}
	h := New("repo", "rpm", DefaultConfig(), []string{"https://repo.example"}, stats)
	h.RecordFailure("https://repo.example", context.Canceled)
	h.RecordFailure("https://repo.example", context.DeadlineExceeded)
	require.Zero(t, stats.calls)
	require.Zero(t, h.upstreams["https://repo.example"].window.totalSamples())
}

func TestDisabledPassiveObservationDoesNotRecord(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	stats := &observationStats{}
	h := New("repo", "rpm", cfg, []string{"https://repo.example"}, stats)
	h.RecordResult("https://repo.example", http.StatusServiceUnavailable, time.Millisecond)
	require.Zero(t, stats.calls)
}

func TestTransientRefreshFailuresStaySuspectWithoutSecondRetryClock(t *testing.T) {
	h := New("repo", "rpm", DefaultConfig(), nil, nil)
	rh := h.AddResource("root", nil, nil)
	for range 10 {
		h.FinishRefresh("root", rh.Generation, ErrResourceTransient, nil)
	}
	current, ok := h.ResourceHealth("root")
	require.True(t, ok)
	require.Equal(t, RSuspect, current.State)
	require.Equal(t, 10, current.ConsecutiveTransient)

	_, done, err := h.TryStartRefresh("root", time.Now())
	require.NoError(t, err)
	done()
}

func TestForbiddenResourceIsPermanentlyBlocked(t *testing.T) {
	h := New("repo", "rpm", DefaultConfig(), nil, nil)
	rh := h.AddResource("root", nil, nil)
	h.FinishRefresh("root", rh.Generation, ErrResourceForbidden, nil)

	current, ok := h.ResourceHealth("root")
	require.True(t, ok)
	require.Equal(t, RBlocked, current.State)
	_, _, err := h.TryStartRefresh("root", time.Now().Add(24*time.Hour))
	require.ErrorIs(t, err, ErrRefreshBlocked)
}

func TestRepeatedNotFoundRemovesResource(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ResourceRemoveAge = 0
	cfg.ResourceRemoveCount = 2
	h := New("repo", "rpm", cfg, nil, nil)
	rh := h.AddResource("root", nil, nil)
	h.FinishRefresh("root", rh.Generation, ErrResourceNotFound, nil)
	h.FinishRefresh("root", rh.Generation, ErrResourceNotFound, nil)
	_, ok := h.ResourceHealth("root")
	require.False(t, ok)
}

func TestSuccessfulRefreshClearsTransientState(t *testing.T) {
	h := New("repo", "rpm", DefaultConfig(), nil, nil)
	rh := h.AddResource("root", nil, nil)
	h.FinishRefresh("root", rh.Generation, ErrResourceTransient, nil)
	h.FinishRefresh("root", rh.Generation, nil, []ResourceTarget{{Path: "repomd.xml"}})

	current, ok := h.ResourceHealth("root")
	require.True(t, ok)
	require.Equal(t, RActive, current.State)
	require.Zero(t, current.ConsecutiveTransient)
	require.Equal(t, []ResourceTarget{{Path: "repomd.xml"}}, current.LastTargets)
}
