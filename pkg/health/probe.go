package health

import (
	"context"
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
		h.mu.RLock()
		state := uh.State
		lastProbe := uh.lastProbeAt
		h.mu.RUnlock()

		switch state {
		case SOpen, SHalfOpen:
			if time.Since(lastProbe) < h.config.CanaryCooldown {
				continue
			}
		default:
			if time.Since(lastProbe) < h.config.ProbeInterval {
				continue
			}
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

	if err != nil {
		h.mu.Lock()
		transition := uh.recordFailure(err, h.config)
		h.emitUpstreamMetrics(uh)
		if transition != nil {
			h.recordCircuitEvent(uh.URL, transition)
		}
		h.recomputeAggregateLocked()
		h.mu.Unlock()
		return
	}
	defer resp.Body.Close()

	h.mu.Lock()
	defer h.mu.Unlock()

	var transition *stateTransition
	switch {
	case resp.StatusCode >= 500:
		transition = uh.recordFailure(formatStatusError(resp.StatusCode), h.config)
	case resp.StatusCode == 404 || resp.StatusCode == 403 || resp.StatusCode == 410:
		transition = uh.recordProbe(true, latency, h.config)
		if rh := h.findResourceForProbe(resp.Request.URL.Path); rh != nil {
			h.applyResourceErrorLocked(rh, statusToResourceError(resp.StatusCode))
		}
	default:
		transition = uh.recordProbe(true, latency, h.config)
		if rh := h.findResourceForProbe(resp.Request.URL.Path); rh != nil && rh.State == RBlocked {
			rh.State = RPending
			rh.NextRefreshAt = time.Time{}
			rh.ConsecutiveTransient = 0
			rh.ConsecutiveInvalid = 0
			rh.ConsecutiveNotFound = 0
		}
	}

	h.emitUpstreamMetrics(uh)
	if transition != nil {
		h.recordCircuitEvent(uh.URL, transition)
	}
	h.recomputeAggregateLocked()
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

	idx := int(atomic.AddInt32(&uh.probeIdx, 1)) % len(targets)
	targetURL := uh.URL + "/" + targets[idx].Path
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, targetURL, nil)
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
			if len(rh.UpstreamURLs) > 0 {
				matched := false
				for _, u := range rh.UpstreamURLs {
					if u == uh.URL {
						matched = true
						break
					}
				}
				if !matched {
					continue
				}
				for _, t := range rh.LastTargets {
					targets = append(targets, t)
					if len(targets) >= maxDynamicPaths {
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
