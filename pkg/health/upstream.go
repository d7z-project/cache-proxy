package health

import (
	"log/slog"
	"math"
	"time"
)

type UpstreamState int

const (
	SClosed    UpstreamState = iota
	SDegraded
	SOpen
	SHalfOpen
)

func (s UpstreamState) String() string {
	switch s {
	case SClosed:
		return "closed"
	case SDegraded:
		return "degraded"
	case SOpen:
		return "open"
	case SHalfOpen:
		return "halfopen"
	default:
		return "unknown"
	}
}

type UpstreamHealth struct {
	URL   string
	State UpstreamState

	window         *rateWindow
	ewmaLatency    time.Duration
	latencySamples int64

	consecutiveOk int

	openedAt       time.Time
	lastSuccessAt  time.Time
	lastProbeAt    time.Time
	lastProbeErr   string

	totalRequests  int64
	failedRequests int64
	weight         float64
	probeIdx       int32
}

func newUpstreamHealth(url string, evalWindow time.Duration) *UpstreamHealth {
	return &UpstreamHealth{
		URL:    url,
		State:  SClosed,
		weight: 1.0,
		window: newRateWindow(evalWindow),
	}
}

func (uh *UpstreamHealth) recordSuccess(latency time.Duration, cfg Config) {
	wasState := uh.State

	if uh.latencySamples == 0 {
		uh.ewmaLatency = latency
	} else {
		old := float64(uh.ewmaLatency)
		uh.ewmaLatency = time.Duration(old*(1-ewmaAlpha) + float64(latency)*ewmaAlpha)
	}
	uh.latencySamples++
	uh.lastSuccessAt = time.Now()
	uh.totalRequests++
	uh.window.record(true)

	switch uh.State {
	case SOpen:
		if time.Since(uh.openedAt) >= cfg.CanaryCooldown {
			uh.transition(SHalfOpen)
			uh.consecutiveOk = 1
		}
	case SHalfOpen:
		uh.consecutiveOk++
		w := cfg.CanaryStep * float64(uh.consecutiveOk)
		if float64(uh.consecutiveOk) >= float64(canarySuccessMin) && w >= canaryCeiling {
			uh.transition(SClosed)
			uh.consecutiveOk = 0
		}
	default:
		uh.evaluateRate(cfg)
	}

	uh.computeWeight(cfg)
	if wasState != uh.State {
		uh.logChange(wasState, "success", "")
	}
}

func (uh *UpstreamHealth) recordFailure(err error, cfg Config) {
	wasState := uh.State

	uh.window.record(false)
	if err != nil {
		uh.lastProbeErr = err.Error()
	}
	uh.failedRequests++
	uh.totalRequests++

	switch uh.State {
	case SHalfOpen:
		uh.transition(SOpen)
		uh.openedAt = time.Now()
		uh.consecutiveOk = 0
	default:
		uh.evaluateRate(cfg)
	}

	uh.computeWeight(cfg)
	if wasState != uh.State {
		uh.logChange(wasState, "failure", err.Error())
	}
}

func (uh *UpstreamHealth) recordProbe(success bool, latency time.Duration, cfg Config) {
	uh.lastProbeAt = time.Now()
	if success {
		uh.recordSuccess(latency, cfg)
	} else {
		uh.recordFailure(nil, cfg)
	}
}

func (uh *UpstreamHealth) evaluateRate(cfg Config) {
	rate := uh.window.errorRate()
	samples := uh.window.totalSamples()

	if samples >= minSampleSize {
		if rate >= cfg.TripRate {
			uh.transition(SOpen)
			uh.openedAt = time.Now()
			return
		}
		if rate > cfg.DegradeRate {
			uh.transition(SDegraded)
			return
		}
	}

	if uh.latencySamples > 0 && float64(uh.ewmaLatency) > float64(cfg.DegradeLatency) {
		uh.transition(SDegraded)
	} else {
		uh.transition(SClosed)
	}
}

func (uh *UpstreamHealth) computeWeight(cfg Config) {
	switch uh.State {
	case SClosed:
		uh.weight = uh.latencyWeight(cfg)
	case SDegraded:
		lw := uh.latencyWeight(cfg)
		ew := uh.errorWeight(cfg)
		uh.weight = math.Max(cfg.MinWeight, lw*ew)
	case SOpen:
		uh.weight = 0
	case SHalfOpen:
		w := cfg.CanaryStep * float64(uh.consecutiveOk)
		uh.weight = math.Min(canaryCeiling, w)
	}
}

func (uh *UpstreamHealth) latencyWeight(cfg Config) float64 {
	if uh.latencySamples == 0 {
		return 1.0
	}
	if float64(uh.ewmaLatency) <= float64(cfg.DegradeLatency) {
		return 1.0
	}
	r := float64(cfg.DegradeLatency) / float64(uh.ewmaLatency)
	return math.Max(cfg.MinWeight, r)
}

func (uh *UpstreamHealth) errorWeight(cfg Config) float64 {
	rate := uh.window.errorRate()
	if rate <= cfg.DegradeRate {
		return 1.0
	}
	if rate >= cfg.TripRate {
		return cfg.MinWeight
	}
	r := (rate - cfg.DegradeRate) / (cfg.TripRate - cfg.DegradeRate)
	return 1.0 - r*(1.0-cfg.MinWeight)
}

func (uh *UpstreamHealth) transition(state UpstreamState) {
	uh.State = state
}

func (uh *UpstreamHealth) logChange(wasState UpstreamState, reason, detail string) {
	attrs := []any{
		"url", uh.URL,
		"from", wasState.String(),
		"to", uh.State.String(),
		"weight", uh.weight,
		"error_rate", uh.window.errorRate(),
		"ewma_latency", uh.ewmaLatency,
		"reason", reason,
	}
	if detail != "" {
		attrs = append(attrs, "detail", detail)
	}
	slog.Debug("upstream state change", attrs...)
}

func (uh *UpstreamHealth) shouldProbe(interval time.Duration) bool {
	return time.Since(uh.lastProbeAt) >= interval
}
