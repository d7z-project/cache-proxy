package health

import (
	"math"
	"time"
)

type UpstreamState int

const (
	SHealthy   UpstreamState = iota
	SDegraded
	SUnhealthy
	SHalfOpen
)

func (s UpstreamState) String() string {
	switch s {
	case SHealthy:
		return "healthy"
	case SDegraded:
		return "degraded"
	case SUnhealthy:
		return "unhealthy"
	case SHalfOpen:
		return "halfopen"
	default:
		return "unknown"
	}
}

type failureRecord struct {
	Time    time.Time
	Latency time.Duration
	Status  int
}

type UpstreamHealth struct {
	URL   string
	State UpstreamState

	ConsecutiveFails int
	ConsecutiveOk    int
	LastFailureAt    time.Time
	LastSuccessAt    time.Time

	Latency      time.Duration
	LatencySamples int64

	Failures []failureRecord

	LastProbeAt    time.Time
	LastProbeError string

	TotalRequests  int64
	FailedRequests int64

	Weight float64

	ProbeIdx int32
}

func newUpstreamHealth(url string) *UpstreamHealth {
	return &UpstreamHealth{
		URL:    url,
		State:  SHealthy,
		Weight: 1.0,
	}
}

func (uh *UpstreamHealth) recordSuccess(latency time.Duration, alpha, degradeLatency, minWeight float64) {
	uh.ConsecutiveFails = 0
	uh.ConsecutiveOk++
	uh.LastSuccessAt = time.Now()
	uh.TotalRequests++

	if uh.LatencySamples == 0 {
		uh.Latency = latency
	} else {
		uh.Latency = time.Duration(float64(uh.Latency)*(1-alpha) + float64(latency)*alpha)
	}
	uh.LatencySamples++

	uh.recalcState(degradeLatency)
	uh.computeWeight(degradeLatency, minWeight)
}

func (uh *UpstreamHealth) recordFailure(err error, latency time.Duration, failureThreshold int, degradeLatency, minWeight float64) {
	uh.ConsecutiveFails++
	uh.ConsecutiveOk = 0
	uh.LastFailureAt = time.Now()
	uh.TotalRequests++
	uh.FailedRequests++
	if err != nil {
		uh.LastProbeError = err.Error()
	} else {
		uh.LastProbeError = ""
	}
	if latency > 0 {
		uh.Latency = latency
		uh.LatencySamples++
	}

	now := time.Now()
	uh.Failures = append(uh.Failures, failureRecord{Time: now, Latency: latency})
	cutoff := now.Add(-5 * time.Minute)
	start := 0
	for i, f := range uh.Failures {
		if f.Time.After(cutoff) {
			start = i
			break
		}
	}
	uh.Failures = uh.Failures[start:]

	if uh.ConsecutiveFails >= failureThreshold {
		uh.State = SUnhealthy
		uh.Weight = 0
		return
	}
	uh.recalcState(degradeLatency)
	uh.computeWeight(degradeLatency, minWeight)
}

func (uh *UpstreamHealth) recordProbeResult(success bool, latency time.Duration, alpha float64, failureThreshold, successThreshold int, degradeLatency, minWeight float64) {
	uh.LastProbeAt = time.Now()

	if success {
		uh.LastProbeError = ""
		if uh.State == SHalfOpen {
			uh.ConsecutiveOk++
			if uh.ConsecutiveOk >= successThreshold {
				uh.State = SHealthy
				uh.ConsecutiveFails = 0
				uh.ConsecutiveOk = 0
			}
		} else if uh.State == SUnhealthy {
			uh.State = SHalfOpen
			uh.ConsecutiveOk = 1
			uh.ConsecutiveFails = 0
		} else {
			uh.ConsecutiveFails = 0
		}
		uh.recordSuccess(latency, alpha, degradeLatency, minWeight)
	} else {
		if uh.State == SHalfOpen {
			uh.State = SUnhealthy
			uh.ConsecutiveOk = 0
		}
		uh.recordFailure(nil, 0, failureThreshold, degradeLatency, minWeight)
	}
}

func (uh *UpstreamHealth) recalcState(degradeLatency float64) {
	if uh.State == SUnhealthy || uh.State == SHalfOpen {
		return
	}
	if uh.LatencySamples > 0 && float64(uh.Latency) > degradeLatency {
		uh.State = SDegraded
		return
	}
	uh.State = SHealthy
}

func (uh *UpstreamHealth) computeWeight(degradeLatency, minWeight float64) {
	switch uh.State {
	case SUnhealthy:
		uh.Weight = 0
	case SHalfOpen:
		uh.Weight = 0.1
	case SHealthy:
		uh.Weight = 1.0
	case SDegraded:
		if uh.LatencySamples == 0 || float64(uh.Latency) <= 0 {
			uh.Weight = 1.0
			return
		}
		ratio := degradeLatency / float64(uh.Latency)
		uh.Weight = math.Max(minWeight, ratio)
	}
}

func (uh *UpstreamHealth) shouldProbe(healthyInterval, unhealthyInterval time.Duration) bool {
	interval := healthyInterval
	if uh.State == SUnhealthy {
		interval = unhealthyInterval
	}
	return time.Since(uh.LastProbeAt) >= interval
}
