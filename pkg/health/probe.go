package health

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

func (h *ServiceHealth) probeLoop() {
	defer h.wg.Done()
	ticker := time.NewTicker(h.config.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.probeAll()
		}
	}
}

func (h *ServiceHealth) probeAll() {
	h.mu.RLock()
	upstreams := make([]*UpstreamHealth, 0, len(h.upstreams))
	for _, uh := range h.upstreams {
		upstreams = append(upstreams, uh)
	}
	h.mu.RUnlock()

	for _, uh := range upstreams {
		if !uh.shouldProbe(h.config.ProbeInterval, h.config.BlockInterval) {
			continue
		}
		h.probeOne(uh)
	}
}

func (h *ServiceHealth) probeOne(uh *UpstreamHealth) {
	parent := h.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, h.config.ProbeTimeout)
	defer cancel()

	start := time.Now()
	resp, err := h.probeDo(ctx, uh)
	latency := time.Since(start)

	h.mu.Lock()
	defer h.mu.Unlock()

	if err != nil {
		uh.recordProbeResult(false, 0, h.config.EwmaAlpha, h.config.FailureThreshold, h.config.SuccessThreshold, float64(h.config.DegradeLatency), h.config.MinWeight)
		uh.LastProbeError = err.Error()
		return
	}

	switch {
	case resp.StatusCode >= 500:
		uh.recordProbeResult(false, 0, h.config.EwmaAlpha, h.config.FailureThreshold, h.config.SuccessThreshold, float64(h.config.DegradeLatency), h.config.MinWeight)
		uh.LastProbeError = fmt.Sprintf("HTTP %d", resp.StatusCode)
	case resp.StatusCode == 404 || resp.StatusCode == 403 || resp.StatusCode == 410:
		uh.recordProbeResult(true, latency, h.config.EwmaAlpha, h.config.FailureThreshold, h.config.SuccessThreshold, float64(h.config.DegradeLatency), h.config.MinWeight)
		if rh := h.findResourceForProbe(resp.Request.URL.Path); rh != nil {
			h.recordResourceResultLocked(rh, statusToResourceError(resp.StatusCode))
		}
	default:
		uh.recordProbeResult(true, latency, h.config.EwmaAlpha, h.config.FailureThreshold, h.config.SuccessThreshold, float64(h.config.DegradeLatency), h.config.MinWeight)
	}
}

func (h *ServiceHealth) probeDo(ctx context.Context, uh *UpstreamHealth) (*http.Response, error) {
	targets := h.probeTargetsForUpstream(uh)
	if len(targets) == 0 {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, uh.URL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", h.userAgent)
		return h.probeClient.Do(req)
	}

	idx := int(atomic.AddInt32(&uh.ProbeIdx, 1)) % len(targets)
	targetURL := uh.URL + "/" + targets[idx].Path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", h.userAgent)
	return h.probeClient.Do(req)
}

func (h *ServiceHealth) probeTargetsForUpstream(uh *UpstreamHealth) []ProbeTarget {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var targets []ProbeTarget
	for _, rh := range h.resources {
		switch rh.State {
		case RActive, RSuspect, RBlocked:
			if len(rh.UpstreamURLs) > 0 && rh.UpstreamURLs[0] == uh.URL {
				for _, t := range rh.LastTargets {
					targets = append(targets, t)
					if len(targets) >= h.config.MaxDynamicPaths {
						return targets
					}
				}
			}
		}
	}
	return targets
}

func (h *ServiceHealth) findResourceForProbe(path string) *ResourceHealth {
	for _, rh := range h.resources {
		for _, t := range rh.LastTargets {
			if t.Path == path || "/"+t.Path == path {
				return rh
			}
		}
	}
	return nil
}

func statusToResourceError(status int) error {
	switch status {
	case 404, 410:
		return ErrResourceNotFound
	case 403:
		return ErrResourceForbidden
	default:
		return ErrResourceTransient
	}
}
