package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

type StatsRecorder interface {
	RecordUpstream(instance, mode, method string, status int)
	RecordMetadataRefresh(instance, mode, result string, duration time.Duration, ready bool)
	SetMetadataState(instance, mode, state string, ready bool)
	SetUpstreamHealth(instance, mode, upstream string, state int, weight, errorRate, latency float64)
	RecordCircuitEvent(instance, mode, upstream, event string)
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

type WeightedUpstream struct {
	URL    string
	Weight float64
}

type ServiceHealth struct {
	mu        sync.RWMutex
	name      string
	mode      string
	config    Config
	stats     StatsRecorder
	bus       *bus.Bus
	userAgent string

	upstreams map[string]*UpstreamHealth
	resources map[string]*ResourceHealth

	aggregate AggregateState

	probeClient    *http.Client
	probeScheduler *ProbeScheduler
	ctx            context.Context
	cancel         context.CancelFunc
	lifecycleMu    sync.Mutex
	running        bool
	stopping       bool
	activeProbes   int
	activeDone     chan struct{}
}

func New(name, mode string, cfg Config, upstreams []string, stats StatsRecorder, userAgent string) *ServiceHealth {
	h := &ServiceHealth{
		name:      name,
		mode:      mode,
		config:    cfg,
		stats:     stats,
		userAgent: userAgent,
		upstreams: map[string]*UpstreamHealth{},
		resources: map[string]*ResourceHealth{},
	}
	for _, url := range upstreams {
		h.upstreams[url] = newUpstreamHealth(url, cfg.EvaluationWindow)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = cfg.ProbeTimeout
	h.probeClient = &http.Client{Transport: transport}

	h.recomputeAggregateLocked()
	return h
}

func (h *ServiceHealth) SetBus(b *bus.Bus) { h.bus = b }

// SetProbeScheduler attaches the shared active probe scheduler.
func (h *ServiceHealth) SetProbeScheduler(s *ProbeScheduler) { h.probeScheduler = s }

func (h *ServiceHealth) notifyProbeScheduler() {
	if h.probeScheduler != nil {
		h.probeScheduler.notify()
	}
}

func (h *ServiceHealth) Start(parent context.Context) {
	if !h.config.Enabled {
		return
	}
	h.lifecycleMu.Lock()
	if h.running {
		h.lifecycleMu.Unlock()
		return
	}
	h.ctx, h.cancel = context.WithCancel(parent)
	h.running = true
	h.stopping = false
	h.activeDone = make(chan struct{})
	scheduler := h.probeScheduler
	h.lifecycleMu.Unlock()
	if scheduler != nil {
		scheduler.register(h)
	}
}

func (h *ServiceHealth) Stop(ctx context.Context) error {
	h.lifecycleMu.Lock()
	cancel := h.cancel
	if h.running {
		h.stopping = true
	}
	scheduler := h.probeScheduler
	h.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if scheduler != nil {
		scheduler.unregister(h)
	}
	done := make(chan struct{})
	go func() {
		h.lifecycleMu.Lock()
		if h.activeProbes > 0 && h.activeDone == nil {
			h.activeDone = make(chan struct{})
		}
		activeDone := h.activeDone
		for h.activeProbes > 0 && activeDone != nil {
			h.lifecycleMu.Unlock()
			<-activeDone
			h.lifecycleMu.Lock()
			activeDone = h.activeDone
		}
		h.lifecycleMu.Unlock()
		close(done)
	}()
	select {
	case <-done:
		h.lifecycleMu.Lock()
		h.ctx = nil
		h.cancel = nil
		h.running = false
		h.lifecycleMu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *ServiceHealth) beginActiveProbe() bool {
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	if h.stopping {
		return false
	}
	if h.activeProbes == 0 && h.activeDone == nil {
		h.activeDone = make(chan struct{})
	}
	h.activeProbes++
	return true
}

func (h *ServiceHealth) finishActiveProbe() {
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	if h.activeProbes > 0 {
		h.activeProbes--
	}
	if h.activeProbes == 0 && h.activeDone != nil {
		close(h.activeDone)
		h.activeDone = nil
	}
}

func (h *ServiceHealth) WeightedUpstreams(upstreams []string) []WeightedUpstream {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]WeightedUpstream, 0, len(upstreams))
	usable := false
	for _, url := range upstreams {
		w := 1.0
		if uh, ok := h.upstreams[url]; ok {
			w = uh.weight
		}
		if w > 0 {
			usable = true
			result = append(result, WeightedUpstream{URL: url, Weight: w})
		}
	}
	if !usable {
		for _, url := range upstreams {
			result = append(result, WeightedUpstream{URL: url, Weight: 1.0})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Weight > result[j].Weight
	})
	return result
}

func (h *ServiceHealth) RecordResult(url string, status int, latency time.Duration) {
	h.mu.Lock()
	uh, ok := h.upstreams[url]
	if !ok {
		h.mu.Unlock()
		return
	}
	var transition *stateTransition
	if upstreamStatusIsFailure(status) {
		transition = uh.recordFailure(formatStatusError(status), h.config)
	} else {
		transition = uh.recordSuccess(latency, h.config)
	}
	h.emitUpstreamMetrics(uh)
	if transition != nil {
		h.recordCircuitEvent(url, transition)
		h.recomputeAggregateLocked()
	}
	h.mu.Unlock()
	if transition != nil {
		h.notifyProbeScheduler()
	}
}

func upstreamStatusIsFailure(status int) bool {
	if status == http.StatusNotFound {
		return false
	}
	return status == 0 || status >= 500 || (status >= 400 && status < 500)
}

func (h *ServiceHealth) RecordFailure(url string, err error) {
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	h.mu.Lock()
	uh, ok := h.upstreams[url]
	if !ok {
		h.mu.Unlock()
		return
	}
	transition := uh.recordFailure(err, h.config)
	h.emitUpstreamMetrics(uh)
	if transition != nil {
		h.recordCircuitEvent(url, transition)
		h.recomputeAggregateLocked()
	}
	h.mu.Unlock()
	if transition != nil {
		h.notifyProbeScheduler()
	}
}

func (h *ServiceHealth) AddResource(path string, targets []ProbeTarget, upstreams []string) ResourceHealth {
	h.mu.Lock()
	existing, ok := h.resources[path]
	if ok && existing.State != RRemoved {
		if len(targets) > 0 {
			existing.LastTargets = append([]ProbeTarget(nil), targets...)
		}
		if len(upstreams) > 0 {
			existing.UpstreamURLs = append([]string(nil), upstreams...)
		}
		snapshot := existing.snapshot()
		h.mu.Unlock()
		h.notifyProbeScheduler()
		return snapshot
	}

	rh := &ResourceHealth{
		Path:         path,
		State:        RPending,
		DiscoveredAt: time.Now(),
		LastTargets:  append([]ProbeTarget(nil), targets...),
		UpstreamURLs: append([]string(nil), upstreams...),
	}
	if existing != nil {
		rh.Generation = existing.Generation + 1
	}
	h.resources[path] = rh
	h.recomputeAggregateLocked()
	snapshot := rh.snapshot()
	h.mu.Unlock()
	h.notifyProbeScheduler()
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
	h.notifyProbeScheduler()
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
	if rh.State == RBlocked && !rh.NextRefreshAt.IsZero() && now.Before(rh.NextRefreshAt) {
		return ResourceHealth{}, nil, ErrRefreshBlockedUntil
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

func (h *ServiceHealth) FinishRefresh(path string, gen uint64, err error, targets []ProbeTarget) {
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
			rh.LastTargets = append([]ProbeTarget(nil), targets...)
		}
		rh.NextRefreshAt = time.Time{}
		h.recomputeAggregateLocked()
		h.mu.Unlock()
		h.notifyProbeScheduler()
		return
	}

	h.applyResourceErrorLocked(rh, err)
	h.mu.Unlock()
	h.notifyProbeScheduler()
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

func (h *ServiceHealth) RefreshBlockedUntil(path string) (time.Time, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rh, ok := h.resources[path]
	if !ok {
		return time.Time{}, false
	}
	return rh.NextRefreshAt, true
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
		return "yellow", degradedLabel(h.upstreams, h.resources), ""
	case StateUnhealthy:
		return "red", "unhealthy", ""
	default:
		return "gray", "unknown", ""
	}
}

func (h *ServiceHealth) recomputeAggregateLocked() {
	healthy, degraded, unhealthy := 0, 0, 0
	for _, uh := range h.upstreams {
		switch uh.State {
		case SClosed:
			healthy++
		case SDegraded:
			degraded++
		case SOpen, SHalfOpen:
			unhealthy++
		}
	}
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
	case isResourceNotFound(err):
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
	case isResourceForbidden(err):
		rh.ConsecutiveInvalid++
		rh.State = RBlocked
		rh.NextRefreshAt = time.Now().Add(h.config.ResourceBlockInterval)
	case isResourceTransient(err):
		rh.ConsecutiveTransient++
		if rh.ConsecutiveTransient >= resourceFailCount {
			rh.State = RBlocked
			rh.NextRefreshAt = time.Now().Add(h.config.ResourceBlockInterval)
		} else {
			rh.State = RSuspect
		}
	default:
		rh.ConsecutiveTransient++
		if rh.ConsecutiveTransient >= resourceFailCount {
			rh.State = RBlocked
			rh.NextRefreshAt = time.Now().Add(h.config.ResourceBlockInterval)
		} else {
			rh.State = RSuspect
		}
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

func (h *ServiceHealth) MarkResourceActive(path string, targets []ProbeTarget) {
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
	rh.NextRefreshAt = time.Time{}
	if len(targets) > 0 {
		rh.LastTargets = append([]ProbeTarget(nil), targets...)
	}
	h.recomputeAggregateLocked()
	h.mu.Unlock()
	h.notifyProbeScheduler()
}

func (h *ServiceHealth) emitUpstreamMetrics(uh *UpstreamHealth) {
	if h.stats == nil {
		return
	}
	h.stats.SetUpstreamHealth(h.name, h.mode, uh.URL,
		int(uh.State), uh.weight, uh.window.errorRate(),
		uh.ewmaLatency.Seconds())
}

func (h *ServiceHealth) recordCircuitEvent(upstream string, transition *stateTransition) {
	if transition == nil {
		return
	}
	event := transition.From + "->" + transition.To
	if h.stats != nil {
		h.stats.RecordCircuitEvent(h.name, h.mode, upstream, event)
	}
	if h.bus != nil {
		h.bus.Publish(bus.Event{
			Type: bus.EventUpstreamState,
			Payload: bus.UpstreamStatePayload{
				Instance: h.name,
				Mode:     h.mode,
				Upstream: upstream,
				From:     transition.From,
				To:       transition.To,
				Reason:   transition.Reason,
				Detail:   transition.Detail,
			},
		})
	}
}

func degradedLabel(upstreams map[string]*UpstreamHealth, resources map[string]*ResourceHealth) string {
	n := 0
	for _, uh := range upstreams {
		if uh.State != SClosed {
			n++
		}
	}
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

func formatStatusError(status int) error {
	if status == 0 {
		return fmt.Errorf("network error")
	}
	return fmt.Errorf("HTTP %d", status)
}

func isResourceNotFound(err error) bool  { return err == ErrResourceNotFound }
func isResourceForbidden(err error) bool { return err == ErrResourceForbidden }
func isResourceTransient(err error) bool { return err == ErrResourceTransient }
