package health

import "time"

type UpstreamHealth struct {
	URL string

	window         *rateWindow
	ewmaLatency    time.Duration
	latencySamples int64
	lastSuccessAt  time.Time
	lastError      string
}

func newUpstreamHealth(url string, evaluationWindow time.Duration) *UpstreamHealth {
	return &UpstreamHealth{URL: url, window: newRateWindow(evaluationWindow)}
}

func (uh *UpstreamHealth) recordSuccess(latency time.Duration) {
	if uh.latencySamples == 0 {
		uh.ewmaLatency = latency
	} else {
		uh.ewmaLatency = time.Duration(float64(uh.ewmaLatency)*(1-ewmaAlpha) + float64(latency)*ewmaAlpha)
	}
	uh.latencySamples++
	uh.lastSuccessAt = time.Now()
	uh.lastError = ""
	uh.window.record(true)
}

func (uh *UpstreamHealth) recordFailure(err error) {
	uh.window.record(false)
	if err != nil {
		uh.lastError = err.Error()
	}
}
