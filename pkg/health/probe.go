package health

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var errNoProbeTarget = errors.New("active probe has no target")

type probeRequest struct {
	upstreamURL string
	targetPath  string
}

func (h *ServiceHealth) probeOne(uh *UpstreamHealth) {
	parent := h.ctx
	if parent == nil {
		parent = context.Background()
	}
	h.probeOneContext(parent, uh.URL)
}

func (h *ServiceHealth) probeOneContext(parent context.Context, upstreamURL string) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, h.config.ProbeTimeout)
	defer cancel()

	start := time.Now()
	resp, targetPath, err := h.probeDo(ctx, upstreamURL)
	latency := time.Since(start)
	if err != nil {
		if errors.Is(err, errNoProbeTarget) || errors.Is(err, context.Canceled) {
			return
		}
		h.recordActiveProbeFailure(upstreamURL, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	h.recordActiveProbeResponse(upstreamURL, targetPath, resp.StatusCode, latency)
}

func (h *ServiceHealth) probeDo(ctx context.Context, upstreamURL string) (*http.Response, string, error) {
	reqInfo, useRange, err := h.nextProbeRequest(upstreamURL)
	if err != nil {
		return nil, "", err
	}
	if useRange {
		resp, err := h.doRangeProbe(ctx, reqInfo)
		return resp, reqInfo.targetPath, err
	}

	resp, err := h.doHeadProbe(ctx, reqInfo)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		return resp, reqInfo.targetPath, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	h.mu.Lock()
	if uh := h.upstreams[upstreamURL]; uh != nil {
		uh.rangeProbeOnly = true
	}
	h.mu.Unlock()

	resp, err = h.doRangeProbe(ctx, reqInfo)
	return resp, reqInfo.targetPath, err
}

func (h *ServiceHealth) doHeadProbe(ctx context.Context, reqInfo probeRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, probeURL(reqInfo.upstreamURL, reqInfo.targetPath), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", h.userAgent)
	return h.probeClient.Do(req)
}

func (h *ServiceHealth) doRangeProbe(ctx context.Context, reqInfo probeRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL(reqInfo.upstreamURL, reqInfo.targetPath), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", h.userAgent)
	req.Header.Set("Range", "bytes=0-0")
	return h.probeClient.Do(req)
}

func (h *ServiceHealth) nextProbeRequest(upstreamURL string) (probeRequest, bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	uh := h.upstreams[upstreamURL]
	if uh == nil {
		return probeRequest{}, false, errNoProbeTarget
	}
	targets := h.probeTargetsForUpstreamLocked(upstreamURL)
	if len(targets) == 0 {
		return probeRequest{}, false, errNoProbeTarget
	}
	idx := int(atomic.AddInt32(&uh.probeIdx, 1)-1) % len(targets)
	return probeRequest{upstreamURL: upstreamURL, targetPath: targets[idx].Path}, uh.rangeProbeOnly, nil
}

func (h *ServiceHealth) probeTargetsForUpstreamLocked(upstreamURL string) []ProbeTarget {
	var targets []ProbeTarget
	for _, rh := range h.resources {
		switch rh.State {
		case RActive, RSuspect, RBlocked:
			if !resourceUsesUpstream(rh, upstreamURL) {
				continue
			}
			for _, target := range rh.LastTargets {
				targets = append(targets, target)
				if len(targets) >= maxDynamicPaths {
					return targets
				}
			}
		}
	}
	return targets
}

func resourceUsesUpstream(rh *ResourceHealth, upstreamURL string) bool {
	for _, candidate := range rh.UpstreamURLs {
		if candidate == upstreamURL {
			return true
		}
	}
	return false
}

func probeURL(upstreamURL, targetPath string) string {
	if targetPath == "" {
		return upstreamURL
	}
	if strings.HasSuffix(upstreamURL, "/") {
		return upstreamURL + targetPath
	}
	return upstreamURL + "/" + targetPath
}

func (h *ServiceHealth) recordActiveProbeFailure(upstreamURL string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	uh := h.upstreams[upstreamURL]
	if uh == nil {
		return
	}
	uh.lastProbeAt = time.Now()
	transition := uh.recordFailure(err, h.config)
	h.emitUpstreamMetrics(uh)
	if transition != nil {
		h.recordCircuitEvent(upstreamURL, transition)
	}
	h.recomputeAggregateLocked()
}

func (h *ServiceHealth) recordActiveProbeResponse(
	upstreamURL string,
	targetPath string,
	status int,
	latency time.Duration,
) {
	h.mu.Lock()
	defer h.mu.Unlock()

	uh := h.upstreams[upstreamURL]
	if uh == nil {
		return
	}
	uh.lastProbeAt = time.Now()

	var transition *stateTransition
	if upstreamStatusIsFailure(status) {
		transition = uh.recordFailure(formatStatusError(status), h.config)
	} else {
		transition = uh.recordSuccess(latency, h.config)
	}

	if rh := h.findResourceForProbe(targetPath); rh != nil {
		h.applyProbeResourceStatusLocked(rh, status)
	}
	h.emitUpstreamMetrics(uh)
	if transition != nil {
		h.recordCircuitEvent(upstreamURL, transition)
	}
	h.recomputeAggregateLocked()
}

func (h *ServiceHealth) applyProbeResourceStatusLocked(rh *ResourceHealth, status int) {
	switch {
	case status == http.StatusNotFound || status == http.StatusGone || status == http.StatusForbidden:
		h.applyResourceErrorLocked(rh, statusToResourceError(status))
	case status >= 500 || (status >= 400 && status < 500):
		h.applyResourceErrorLocked(rh, ErrResourceTransient)
	case status >= 200 && status < 400 && rh.State == RBlocked:
		rh.State = RPending
		rh.NextRefreshAt = time.Time{}
		rh.ConsecutiveTransient = 0
		rh.ConsecutiveInvalid = 0
		rh.ConsecutiveNotFound = 0
		rh.LastError = ""
	}
}

func (h *ServiceHealth) findResourceForProbe(targetPath string) *ResourceHealth {
	for _, rh := range h.resources {
		for _, target := range rh.LastTargets {
			if target.Path == targetPath {
				return rh
			}
		}
	}
	return nil
}

func statusToResourceError(status int) error {
	switch status {
	case http.StatusNotFound, http.StatusGone:
		return ErrResourceNotFound
	case http.StatusForbidden:
		return ErrResourceForbidden
	default:
		return ErrResourceTransient
	}
}
