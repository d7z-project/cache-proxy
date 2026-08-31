package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type StatsRecorder interface {
	SetUpstreamObservation(instance, mode, upstream string, errorRate, latency float64)
}

type AggregateState int

const (
	StateHealthy AggregateState = iota
	StateDegraded
	StateUnhealthy
)

func (s AggregateState) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateDegraded:
		return "degraded"
	case StateUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// ServiceHealth records passive upstream observations only. Repository
// lifecycle and refresh coordination belong to handlers and the scheduler.
type ServiceHealth struct {
	mu        sync.RWMutex
	name      string
	mode      string
	config    Config
	stats     StatsRecorder
	upstreams map[string]*UpstreamHealth
	aggregate AggregateState
}

func New(name, mode string, cfg Config, upstreams []string, stats StatsRecorder) *ServiceHealth {
	h := &ServiceHealth{
		name: name, mode: mode, config: cfg, stats: stats,
		upstreams: make(map[string]*UpstreamHealth, len(upstreams)),
	}
	for _, url := range upstreams {
		h.upstreams[url] = newUpstreamHealth(url, defaultEvaluationWindow)
	}
	return h
}

func (h *ServiceHealth) RecordResult(url string, status int, latency time.Duration) {
	if !h.config.Enabled {
		return
	}
	h.mu.Lock()
	upstream := h.upstreams[url]
	if upstream == nil {
		h.mu.Unlock()
		return
	}
	if status == 0 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 {
		upstream.recordFailure(fmt.Errorf("http %d", status))
	} else {
		upstream.recordSuccess(latency)
	}
	h.emitUpstreamMetrics(upstream)
	h.recomputeAggregateLocked()
	h.mu.Unlock()
}

func (h *ServiceHealth) RecordFailure(url string, err error) {
	if !h.config.Enabled || err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	h.mu.Lock()
	upstream := h.upstreams[url]
	if upstream == nil {
		h.mu.Unlock()
		return
	}
	upstream.recordFailure(err)
	h.emitUpstreamMetrics(upstream)
	h.recomputeAggregateLocked()
	h.mu.Unlock()
}

func (h *ServiceHealth) DashboardStatus() (color, label, extra string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	switch h.aggregate {
	case StateHealthy:
		return "green", "healthy", ""
	case StateDegraded:
		return "yellow", "degraded", ""
	case StateUnhealthy:
		return "red", "unhealthy", ""
	default:
		return "gray", "unknown", ""
	}
}

func (h *ServiceHealth) recomputeAggregateLocked() {
	worst := 0.0
	for _, upstream := range h.upstreams {
		worst = max(worst, upstream.window.errorRate())
	}
	switch {
	case worst >= 0.5:
		h.aggregate = StateUnhealthy
	case worst > 0:
		h.aggregate = StateDegraded
	default:
		h.aggregate = StateHealthy
	}
}

func (h *ServiceHealth) emitUpstreamMetrics(upstream *UpstreamHealth) {
	if h.stats != nil {
		h.stats.SetUpstreamObservation(h.name, h.mode, upstream.URL, upstream.window.errorRate(), upstream.ewmaLatency.Seconds())
	}
}
