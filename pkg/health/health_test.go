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

func (s *observationStats) SetUpstreamObservation(_, _, _ string, errorRate, latency float64) {
	s.errorRate = errorRate
	s.latency = latency
	s.calls++
}

func TestApplyConfigPatchUpdatesObservationSetting(t *testing.T) {
	defaults := DefaultConfig()
	disabled := false
	cfg := ApplyConfigPatch(defaults, &ConfigPatch{Enabled: &disabled})
	require.False(t, cfg.Enabled)
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
}

func TestDisabledPassiveObservationDoesNotRecord(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	stats := &observationStats{}
	h := New("repo", "rpm", cfg, []string{"https://repo.example"}, stats)
	h.RecordResult("https://repo.example", http.StatusServiceUnavailable, time.Millisecond)
	require.Zero(t, stats.calls)
}
