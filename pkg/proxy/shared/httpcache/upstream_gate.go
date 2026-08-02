package httpcache

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var ErrAdmissionWaitTimeout = errors.New("upstream admission wait timed out")

type AdmissionClass uint8

const (
	AdmissionForeground AdmissionClass = iota
	AdmissionRefresh
)

type AdmissionWaitError struct {
	Host string
}

func (e *AdmissionWaitError) Error() string {
	if e.Host == "" {
		return ErrAdmissionWaitTimeout.Error()
	}
	return "upstream " + e.Host + " admission wait timed out"
}

func (e *AdmissionWaitError) Unwrap() error { return ErrAdmissionWaitTimeout }

type UpstreamRateLimitError struct {
	Host       string
	RetryAfter time.Time
}

func (e *UpstreamRateLimitError) Error() string { return "upstream " + e.Host + " is rate limited" }

func AdmissionRetryAfterSeconds(err error) (int, bool) {
	var limited *UpstreamRateLimitError
	if errors.As(err, &limited) {
		if limited.RetryAfter.IsZero() {
			return 5, true
		}
		remaining := time.Until(limited.RetryAfter)
		return max(int((remaining+time.Second-1)/time.Second), 1), true
	}
	if errors.Is(err, ErrAdmissionWaitTimeout) {
		return 5, true
	}
	return 0, false
}

type UpstreamGateConfig struct {
	MaxActive           int
	MaxActivePerHost    int
	RequestInterval     time.Duration
	ForegroundQueueWait time.Duration
	Hosts               map[string]UpstreamHostGateConfig
}

type UpstreamHostGateConfig struct {
	MaxActive       int
	RequestInterval time.Duration
}

type gateWaiter struct {
	host    string
	ready   chan struct{}
	granted bool
	err     error
	element *list.Element
}

type upstreamHostGate struct {
	active          int
	queued          int
	maxActive       int
	requestInterval time.Duration
	nextRequest     time.Time
	cooldownUntil   time.Time
}

type UpstreamGate struct {
	mu                  sync.Mutex
	maxActive           int
	maxActivePerHost    int
	requestInterval     time.Duration
	foregroundQueueWait time.Duration
	hostConfigs         map[string]UpstreamHostGateConfig
	active              int
	waiters             list.List
	hosts               map[string]*upstreamHostGate
	wakeTimer           *time.Timer
	wakeAt              time.Time
	wakeSequence        uint64
}

type UpstreamGateSnapshot struct {
	Active           int
	Queued           int
	MaxActive        int
	MaxActivePerHost int
	RequestInterval  time.Duration
	Hosts            map[string]UpstreamHostGateSnapshot
}

type UpstreamHostGateSnapshot struct {
	Active          int
	Queued          int
	MaxActive       int
	RequestInterval time.Duration
	NextRequest     time.Time
	CooldownUntil   time.Time
}

func NewUpstreamGate(cfg UpstreamGateConfig) *UpstreamGate {
	if cfg.MaxActive <= 0 || cfg.MaxActivePerHost <= 0 {
		return nil
	}
	hostConfigs := make(map[string]UpstreamHostGateConfig, len(cfg.Hosts))
	for host, hostConfig := range cfg.Hosts {
		hostConfigs[host] = hostConfig
	}
	gate := &UpstreamGate{
		maxActive:           cfg.MaxActive,
		maxActivePerHost:    cfg.MaxActivePerHost,
		requestInterval:     max(cfg.RequestInterval, 0),
		foregroundQueueWait: max(cfg.ForegroundQueueWait, 0),
		hostConfigs:         hostConfigs,
		hosts:               map[string]*upstreamHostGate{},
	}
	for host := range hostConfigs {
		gate.hosts[host] = gate.newHostState(host)
	}
	return gate
}

func (g *UpstreamGate) Acquire(ctx context.Context, upstream string, class AdmissionClass) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host := normalizeUpstreamHost(upstream)
	waiter := &gateWaiter{host: host, ready: make(chan struct{})}

	g.mu.Lock()
	state := g.hostStateLocked(host)
	if time.Now().Before(state.cooldownUntil) {
		err := &UpstreamRateLimitError{Host: host, RetryAfter: state.cooldownUntil}
		g.mu.Unlock()
		return nil, err
	}
	waiter.element = g.waiters.PushBack(waiter)
	state.queued++
	g.grantWaitersLocked()
	g.mu.Unlock()

	if class == AdmissionRefresh || g.foregroundQueueWait <= 0 {
		select {
		case <-waiter.ready:
			return g.waiterResult(waiter)
		case <-ctx.Done():
			g.cancelOrReleaseWaiter(waiter)
			return nil, ctx.Err()
		}
	}

	timer := time.NewTimer(g.foregroundQueueWait)
	defer timer.Stop()
	select {
	case <-waiter.ready:
		return g.waiterResult(waiter)
	case <-ctx.Done():
		g.cancelOrReleaseWaiter(waiter)
		return nil, ctx.Err()
	case <-timer.C:
		if g.cancelWaiter(waiter) {
			return nil, &AdmissionWaitError{Host: host}
		}
		return g.waiterResult(waiter)
	}
}

func (g *UpstreamGate) TryAcquireProbe(upstream string) (func(), bool) {
	if g == nil {
		return func() {}, true
	}
	host := normalizeUpstreamHost(upstream)
	now := time.Now()
	g.mu.Lock()
	state := g.hostStateLocked(host)
	if g.active >= g.maxActive || state.active >= state.maxActive || state.queued > 0 ||
		now.Before(state.nextRequest) || now.Before(state.cooldownUntil) {
		g.mu.Unlock()
		return nil, false
	}
	g.activateLocked(state, now)
	g.mu.Unlock()
	return g.releaseFunc(host), true
}

func (g *UpstreamGate) RateLimited(upstream, retryAfter string) *UpstreamRateLimitError {
	host := normalizeUpstreamHost(upstream)
	if g == nil {
		return &UpstreamRateLimitError{Host: host}
	}
	now := time.Now()
	delay := parseRetryAfter(retryAfter, now)
	if delay <= 0 {
		delay = 30 * time.Second
	}

	g.mu.Lock()
	state := g.hostStateLocked(host)
	if candidate := now.Add(delay); candidate.After(state.cooldownUntil) {
		state.cooldownUntil = candidate
	}
	limited := &UpstreamRateLimitError{Host: host, RetryAfter: state.cooldownUntil}
	for element := g.waiters.Front(); element != nil; {
		next := element.Next()
		waiter := element.Value.(*gateWaiter)
		if waiter.host == host {
			g.removeWaiterLocked(waiter)
			waiter.err = limited
			close(waiter.ready)
		}
		element = next
	}
	g.grantWaitersLocked()
	g.mu.Unlock()
	slog.Warn("upstream rate limit activated", "host", host, "retry_at", limited.RetryAfter, "backoff", time.Until(limited.RetryAfter))
	return limited
}

// ObserveRateLimit records a real upstream 429 for callers that do not need
// the resulting typed error, such as active health probes.
func (g *UpstreamGate) ObserveRateLimit(upstream, retryAfter string) {
	g.RateLimited(upstream, retryAfter)
}

func (g *UpstreamGate) Snapshot() UpstreamGateSnapshot {
	snapshot := UpstreamGateSnapshot{Hosts: map[string]UpstreamHostGateSnapshot{}}
	if g == nil {
		return snapshot
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	snapshot.Active = g.active
	snapshot.Queued = g.waiters.Len()
	snapshot.MaxActive = g.maxActive
	snapshot.MaxActivePerHost = g.maxActivePerHost
	snapshot.RequestInterval = g.requestInterval
	for host, state := range g.hosts {
		_, configured := g.hostConfigs[host]
		if !configured && state.active == 0 && state.queued == 0 && !now.Before(state.nextRequest) && !now.Before(state.cooldownUntil) {
			delete(g.hosts, host)
			continue
		}
		hostSnapshot := UpstreamHostGateSnapshot{
			Active:          state.active,
			Queued:          state.queued,
			MaxActive:       state.maxActive,
			RequestInterval: state.requestInterval,
		}
		if now.Before(state.nextRequest) {
			hostSnapshot.NextRequest = state.nextRequest
		}
		if now.Before(state.cooldownUntil) {
			hostSnapshot.CooldownUntil = state.cooldownUntil
		}
		snapshot.Hosts[host] = hostSnapshot
	}
	return snapshot
}

func normalizeUpstreamHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Host)
}

func (g *UpstreamGate) waiterResult(waiter *gateWaiter) (func(), error) {
	if waiter.err != nil {
		return nil, waiter.err
	}
	return g.releaseFunc(waiter.host), nil
}

func (g *UpstreamGate) cancelWaiter(waiter *gateWaiter) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if waiter.granted || waiter.err != nil {
		return false
	}
	g.removeWaiterLocked(waiter)
	g.grantWaitersLocked()
	return true
}

func (g *UpstreamGate) cancelOrReleaseWaiter(waiter *gateWaiter) {
	if g.cancelWaiter(waiter) {
		return
	}
	release, err := g.waiterResult(waiter)
	if err == nil {
		release()
	}
}

func (g *UpstreamGate) hostStateLocked(host string) *upstreamHostGate {
	state := g.hosts[host]
	if state != nil {
		return state
	}
	state = g.newHostState(host)
	g.hosts[host] = state
	return state
}

func (g *UpstreamGate) newHostState(host string) *upstreamHostGate {
	state := &upstreamHostGate{maxActive: g.maxActivePerHost, requestInterval: g.requestInterval}
	if override, ok := g.hostConfigs[host]; ok {
		if override.MaxActive > 0 {
			state.maxActive = override.MaxActive
		}
		state.requestInterval = max(override.RequestInterval, 0)
	}
	return state
}

func (g *UpstreamGate) removeWaiterLocked(waiter *gateWaiter) {
	if waiter.element == nil {
		return
	}
	g.waiters.Remove(waiter.element)
	waiter.element = nil
	state := g.hostStateLocked(waiter.host)
	state.queued--
}

func (g *UpstreamGate) grantWaitersLocked() {
	for g.active < g.maxActive {
		now := time.Now()
		var selected *gateWaiter
		seenHosts := map[string]struct{}{}
		var nextWake time.Time
		for element := g.waiters.Front(); element != nil; element = element.Next() {
			waiter := element.Value.(*gateWaiter)
			if _, seen := seenHosts[waiter.host]; seen {
				continue
			}
			seenHosts[waiter.host] = struct{}{}
			state := g.hostStateLocked(waiter.host)
			if now.Before(state.cooldownUntil) || state.active >= state.maxActive {
				continue
			}
			if now.Before(state.nextRequest) {
				if nextWake.IsZero() || state.nextRequest.Before(nextWake) {
					nextWake = state.nextRequest
				}
				continue
			}
			selected = waiter
			break
		}
		if selected == nil {
			g.scheduleWakeLocked(nextWake)
			return
		}
		g.removeWaiterLocked(selected)
		state := g.hostStateLocked(selected.host)
		g.activateLocked(state, now)
		selected.granted = true
		close(selected.ready)
	}
}

func (g *UpstreamGate) activateLocked(state *upstreamHostGate, now time.Time) {
	g.active++
	state.active++
	state.nextRequest = now.Add(state.requestInterval)
}

func (g *UpstreamGate) scheduleWakeLocked(at time.Time) {
	if at.IsZero() {
		return
	}
	if g.wakeTimer != nil && !at.Before(g.wakeAt) {
		return
	}
	if g.wakeTimer != nil {
		g.wakeTimer.Stop()
	}
	g.wakeAt = at
	g.wakeSequence++
	sequence := g.wakeSequence
	g.wakeTimer = time.AfterFunc(max(time.Until(at), time.Millisecond), func() {
		g.mu.Lock()
		if sequence != g.wakeSequence {
			g.mu.Unlock()
			return
		}
		g.wakeTimer = nil
		g.wakeAt = time.Time{}
		g.grantWaitersLocked()
		g.mu.Unlock()
	})
}

func (g *UpstreamGate) releaseFunc(host string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.active--
			g.hostStateLocked(host).active--
			g.grantWaitersLocked()
			g.mu.Unlock()
		})
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if when, err := http.ParseTime(value); err == nil {
		return when.Sub(now)
	}
	seconds, err := time.ParseDuration(value + "s")
	if err != nil || seconds <= 0 {
		return 0
	}
	return seconds
}
