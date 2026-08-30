package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

type StatsRecorder interface {
	RecordUpstream(instance, mode, method string, status int)
	RecordMetadataRefresh(instance, mode, result string, duration time.Duration, ready bool)
	SetMetadataState(instance, mode, state string, ready bool)
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

type ServiceHealth struct {
	mu     sync.RWMutex
	name   string
	mode   string
	config Config
	stats  StatsRecorder
	bus    *bus.Bus

	upstreams map[string]*UpstreamHealth
	resources map[string]*ResourceHealth

	aggregate AggregateState
}

func New(name, mode string, cfg Config, upstreams []string, stats StatsRecorder) *ServiceHealth {
	h := &ServiceHealth{
		name:      name,
		mode:      mode,
		config:    cfg,
		stats:     stats,
		upstreams: map[string]*UpstreamHealth{},
		resources: map[string]*ResourceHealth{},
	}
	for _, url := range upstreams {
		h.upstreams[url] = newUpstreamHealth(url, cfg.evaluationWindow)
	}

	h.recomputeAggregateLocked()
	return h
}

func (h *ServiceHealth) SetBus(b *bus.Bus) { h.bus = b }

func (h *ServiceHealth) RecordResult(url string, status int, latency time.Duration) {
	if !h.config.Enabled {
		return
	}
	h.mu.Lock()
	uh, ok := h.upstreams[url]
	if !ok {
		h.mu.Unlock()
		return
	}
	if upstreamStatusIsFailure(status) {
		if status == 0 {
			uh.recordFailure(errors.New("network error"))
		} else {
			uh.recordFailure(fmt.Errorf("http %d", status))
		}
	} else {
		uh.recordSuccess(latency)
	}
	h.emitUpstreamMetrics(uh)
	h.mu.Unlock()
}

func upstreamStatusIsFailure(status int) bool {
	return status == 0 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func (h *ServiceHealth) RecordFailure(url string, err error) {
	if !h.config.Enabled {
		return
	}
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return
	}
	h.mu.Lock()
	uh, ok := h.upstreams[url]
	if !ok {
		h.mu.Unlock()
		return
	}
	uh.recordFailure(err)
	h.emitUpstreamMetrics(uh)
	h.mu.Unlock()
}

func (h *ServiceHealth) AddResource(path string, targets []ResourceTarget, upstreams []string) ResourceHealth {
	h.mu.Lock()
	existing, ok := h.resources[path]
	if ok && existing.State != RRemoved {
		if len(targets) > 0 {
			existing.LastTargets = append([]ResourceTarget(nil), targets...)
		}
		if len(upstreams) > 0 {
			existing.UpstreamURLs = append([]string(nil), upstreams...)
		}
		snapshot := existing.snapshot()
		h.mu.Unlock()
		return snapshot
	}

	rh := &ResourceHealth{
		Path:         path,
		State:        RPending,
		DiscoveredAt: time.Now(),
		LastTargets:  append([]ResourceTarget(nil), targets...),
		UpstreamURLs: append([]string(nil), upstreams...),
	}
	if existing != nil {
		rh.Generation = existing.Generation + 1
	}
	h.resources[path] = rh
	h.recomputeAggregateLocked()
	snapshot := rh.snapshot()
	h.mu.Unlock()
	return snapshot
}

func (h *ServiceHealth) RestoreResources(snapshots []ResourceSnapshot) {
	h.mu.Lock()
	for _, snapshot := range snapshots {
		if snapshot.Path == "" {
			continue
		}
		h.resources[snapshot.Path] = ResourceFromSnapshot(snapshot)
	}
	h.recomputeAggregateLocked()
	h.mu.Unlock()
}

func (h *ServiceHealth) TryStartRefresh(path string, now time.Time) (ResourceHealth, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	rh := h.resources[path]
	if rh == nil || rh.State == RRemoved {
		return ResourceHealth{}, nil, ErrRefreshResourceRemoved
	}
	if rh.Refreshing {
		return ResourceHealth{}, nil, ErrRefreshAlreadyRunning
	}
	if rh.State == RBlocked {
		return ResourceHealth{}, nil, ErrRefreshBlocked
	}
	rh.Refreshing = true
	rh.LastRefreshAt = now
	rh.Generation++
	resourceCopy := *rh
	return resourceCopy, func() {
		h.mu.Lock()
		if cur := h.resources[path]; cur != nil && cur.Generation == resourceCopy.Generation {
			cur.Refreshing = false
		}
		h.mu.Unlock()
	}, nil
}

func (h *ServiceHealth) FinishRefresh(path string, gen uint64, err error, targets []ResourceTarget) {
	h.mu.Lock()
	rh := h.resources[path]
	if rh == nil || rh.Generation != gen {
		h.mu.Unlock()
		return
	}

	if err == nil {
		rh.State = RActive
		rh.ConsecutiveNotFound = 0
		rh.ConsecutiveInvalid = 0
		rh.ConsecutiveTransient = 0
		rh.FirstNotFoundAt = time.Time{}
		rh.LastSuccessAt = time.Now()
		rh.LastError = ""
		if len(targets) > 0 {
			rh.LastTargets = append([]ResourceTarget(nil), targets...)
		}
		h.recomputeAggregateLocked()
		h.mu.Unlock()
		return
	}

	h.applyResourceErrorLocked(rh, err)
	h.mu.Unlock()
}

func (h *ServiceHealth) ResourceState(path string) (ResourceState, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rh, ok := h.resources[path]
	if !ok {
		return RRemoved, false
	}
	if rh.State == RRemoved {
		return RRemoved, false
	}
	return rh.State, true
}

func (h *ServiceHealth) SnapshotResources() []ResourceSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([]ResourceSnapshot, 0, len(h.resources))
	for _, rh := range h.resources {
		result = append(result, rh.Snapshot())
	}
	return result
}

func (h *ServiceHealth) AggregateState() AggregateState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.aggregate
}

func (h *ServiceHealth) DashboardStatus() (color, label, extra string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	switch h.aggregate {
	case StateHealthy:
		return "green", "healthy", ""
	case StateDegraded:
		return "yellow", degradedLabel(h.resources), ""
	case StateUnhealthy:
		return "red", "unhealthy", ""
	default:
		return "gray", "unknown", ""
	}
}

func (h *ServiceHealth) recomputeAggregateLocked() {
	healthy, degraded, unhealthy := 0, 0, 0
	for _, rh := range h.resources {
		switch rh.State {
		case RActive:
			healthy++
		case RSuspect:
			degraded++
		case RBlocked, RRemoved, RPending:
			unhealthy++
		}
	}
	switch {
	case unhealthy > 0 && healthy == 0:
		h.aggregate = StateUnhealthy
	case unhealthy > 0 || degraded > 0:
		h.aggregate = StateDegraded
	default:
		h.aggregate = StateHealthy
	}
}

func (h *ServiceHealth) applyResourceErrorLocked(rh *ResourceHealth, err error) {
	rh.LastError = err.Error()
	switch {
	case errors.Is(err, ErrResourceNotFound):
		rh.ConsecutiveNotFound++
		if rh.FirstNotFoundAt.IsZero() {
			rh.FirstNotFoundAt = time.Now()
		}
		now := time.Now()
		removeResource := rh.ConsecutiveNotFound >= h.config.ResourceRemoveCount &&
			now.Sub(rh.FirstNotFoundAt) >= h.config.ResourceRemoveAge
		if removeResource {
			path := rh.Path
			rh.State = RRemoved
			rh.Generation++
			delete(h.resources, rh.Path)
			if h.bus != nil {
				h.bus.Publish(bus.Event{
					Type:    bus.EventMetadataRemoved,
					Payload: bus.MetadataRemovedPayload{Instance: h.name, RootID: path},
				})
			}
		} else {
			rh.State = RSuspect
		}
	case errors.Is(err, ErrResourceForbidden):
		rh.ConsecutiveInvalid++
		rh.State = RBlocked
	case errors.Is(err, ErrResourceTransient):
		rh.ConsecutiveTransient++
		rh.State = RSuspect
	default:
		rh.ConsecutiveTransient++
		rh.State = RSuspect
	}
	h.recomputeAggregateLocked()
}

func (h *ServiceHealth) ResourceHealth(path string) (ResourceHealth, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rh, ok := h.resources[path]
	if !ok {
		return ResourceHealth{}, false
	}
	return rh.snapshot(), true
}

func (h *ServiceHealth) RemoveResource(path string) {
	h.mu.Lock()
	_, existed := h.resources[path]
	delete(h.resources, path)
	h.recomputeAggregateLocked()
	h.mu.Unlock()
	if existed && h.bus != nil {
		h.bus.Publish(bus.Event{
			Type:    bus.EventMetadataRemoved,
			Payload: bus.MetadataRemovedPayload{Instance: h.name, RootID: path},
		})
	}
}

func (h *ServiceHealth) MarkResourceActive(path string, targets []ResourceTarget) {
	h.mu.Lock()
	rh := h.resources[path]
	if rh == nil || rh.State == RRemoved {
		h.mu.Unlock()
		return
	}
	rh.State = RActive
	rh.Refreshing = false
	rh.ConsecutiveNotFound = 0
	rh.ConsecutiveInvalid = 0
	rh.ConsecutiveTransient = 0
	rh.FirstNotFoundAt = time.Time{}
	rh.LastError = ""
	if rh.LastSuccessAt.IsZero() {
		rh.LastSuccessAt = time.Now()
	}
	if len(targets) > 0 {
		rh.LastTargets = append([]ResourceTarget(nil), targets...)
	}
	h.recomputeAggregateLocked()
	h.mu.Unlock()
}

func (h *ServiceHealth) emitUpstreamMetrics(uh *UpstreamHealth) {
	if h.stats == nil {
		return
	}
	h.stats.SetUpstreamObservation(h.name, h.mode, uh.URL, uh.window.errorRate(), uh.ewmaLatency.Seconds())
}

func degradedLabel(resources map[string]*ResourceHealth) string {
	n := 0
	for _, rh := range resources {
		if rh.State != RActive {
			n++
		}
	}
	if n == 0 {
		return "degraded"
	}
	return fmt.Sprintf("%d degraded", n)
}
