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

const maxAdmissionWaiters = 4096
const maxAdmissionWaitersPerHost = 512
const maxForegroundBurst = 8
const refreshAdmissionMaxWait = time.Second

var ErrAdmissionOverloaded = errors.New("upstream admission queue is full")

type AdmissionClass uint8

const (
	AdmissionForeground AdmissionClass = iota
	AdmissionRefresh
)

type AdmissionOverloadError struct{}

func (*AdmissionOverloadError) Error() string { return ErrAdmissionOverloaded.Error() }
func (*AdmissionOverloadError) Unwrap() error { return ErrAdmissionOverloaded }

type UpstreamRateLimitError struct {
	Host       string
	RetryAfter time.Time
}

func (e *UpstreamRateLimitError) Error() string { return "upstream " + e.Host + " is rate limited" }

func AdmissionRetryAfterSeconds(err error) (int, bool) {
	var overloaded *AdmissionOverloadError
	if errors.As(err, &overloaded) {
		return 1, true
	}
	var limited *UpstreamRateLimitError
	if errors.As(err, &limited) && !limited.RetryAfter.IsZero() {
		remaining := time.Until(limited.RetryAfter)
		return max(int((remaining+time.Second-1)/time.Second), 1), true
	}
	return 0, false
}

type UpstreamGateConfig struct {
	MaxActive        int
	MaxActivePerHost int
	MinInterval      time.Duration
	Hosts            map[string]UpstreamHostGateConfig
}

type UpstreamHostGateConfig struct {
	MaxActive   int
	MinInterval time.Duration
}

type gateWaiter struct {
	host      string
	class     AdmissionClass
	queuedAt  time.Time
	ready     chan struct{}
	granted   bool
	element   *list.Element
	waitQueue *list.List
}

type upstreamHostGate struct {
	active        int
	queued        int
	maxActive     int
	minInterval   time.Duration
	lastStarted   time.Time
	cooldownUntil time.Time
}

type UpstreamGate struct {
	mu               sync.Mutex
	maxActive        int
	maxActivePerHost int
	minInterval      time.Duration
	hostConfigs      map[string]UpstreamHostGateConfig
	active           int
	foreground       list.List
	refresh          list.List
	hosts            map[string]*upstreamHostGate
	wakeTimer        *time.Timer
	wakeAt           time.Time
	wakeSequence     uint64
	lastGrantedHost  [2]string
	foregroundBurst  int
}

type UpstreamGateSnapshot struct {
	Active           int
	Queued           int
	ForegroundQueued int
	RefreshQueued    int
	OldestWait       time.Duration
	MaxActive        int
	MaxActivePerHost int
	MinInterval      time.Duration
	Hosts            map[string]UpstreamHostGateSnapshot
}

type UpstreamHostGateSnapshot struct {
	Active        int
	Queued        int
	MaxActive     int
	MinInterval   time.Duration
	CooldownUntil time.Time
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
		maxActive:        cfg.MaxActive,
		maxActivePerHost: cfg.MaxActivePerHost,
		minInterval:      max(cfg.MinInterval, 0),
		hostConfigs:      hostConfigs,
		hosts:            map[string]*upstreamHostGate{},
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
	waiter := &gateWaiter{host: host, class: class, queuedAt: time.Now(), ready: make(chan struct{})}

	g.mu.Lock()
	state := g.hostStateLocked(host)
	if class == AdmissionRefresh && time.Now().Before(state.cooldownUntil) {
		err := &UpstreamRateLimitError{Host: host, RetryAfter: state.cooldownUntil}
		g.mu.Unlock()
		return nil, err
	}
	if g.foreground.Len()+g.refresh.Len() >= maxAdmissionWaiters || state.queued >= maxAdmissionWaitersPerHost {
		g.mu.Unlock()
		return nil, &AdmissionOverloadError{}
	}
	if class == AdmissionForeground {
		waiter.waitQueue = &g.foreground
	} else {
		waiter.waitQueue = &g.refresh
	}
	waiter.element = waiter.waitQueue.PushBack(waiter)
	state.queued++
	g.grantWaitersLocked()
	g.mu.Unlock()

	select {
	case <-waiter.ready:
		return g.releaseFunc(waiter.host), nil
	case <-ctx.Done():
		g.cancelOrReleaseWaiter(waiter)
		return nil, ctx.Err()
	}
}

func (g *UpstreamGate) RateLimited(upstream, retryAfter string) *UpstreamRateLimitError {
	host := normalizeUpstreamHost(upstream)
	now := time.Now()
	delay := parseRetryAfter(retryAfter, now)
	if g == nil || delay <= 0 {
		return &UpstreamRateLimitError{Host: host}
	}

	g.mu.Lock()
	state := g.hostStateLocked(host)
	if candidate := now.Add(delay); candidate.After(state.cooldownUntil) {
		state.cooldownUntil = candidate
	}
	limited := &UpstreamRateLimitError{Host: host, RetryAfter: state.cooldownUntil}
	g.scheduleWakeLocked(state.cooldownUntil)
	g.grantWaitersLocked()
	g.mu.Unlock()
	slog.Warn("upstream rate limit activated", "host", host, "retry_at", limited.RetryAfter, "backoff", time.Until(limited.RetryAfter))
	return limited
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
	snapshot.ForegroundQueued = g.foreground.Len()
	snapshot.RefreshQueued = g.refresh.Len()
	snapshot.Queued = snapshot.ForegroundQueued + snapshot.RefreshQueued
	snapshot.MaxActive = g.maxActive
	snapshot.MaxActivePerHost = g.maxActivePerHost
	snapshot.MinInterval = g.minInterval
	for _, queue := range []*list.List{&g.foreground, &g.refresh} {
		if first := queue.Front(); first != nil {
			wait := now.Sub(first.Value.(*gateWaiter).queuedAt)
			if wait > snapshot.OldestWait {
				snapshot.OldestWait = wait
			}
		}
	}
	for host, state := range g.hosts {
		_, configured := g.hostConfigs[host]
		if !configured && state.active == 0 && state.queued == 0 && !now.Before(state.cooldownUntil) {
			delete(g.hosts, host)
			continue
		}
		hostSnapshot := UpstreamHostGateSnapshot{
			Active:      state.active,
			Queued:      state.queued,
			MaxActive:   state.maxActive,
			MinInterval: state.minInterval,
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

func (g *UpstreamGate) cancelOrReleaseWaiter(waiter *gateWaiter) {
	g.mu.Lock()
	if waiter.granted {
		g.active--
		g.hostStateLocked(waiter.host).active--
	} else {
		g.removeWaiterLocked(waiter)
	}
	g.grantWaitersLocked()
	g.mu.Unlock()
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
	state := &upstreamHostGate{maxActive: g.maxActivePerHost, minInterval: g.minInterval}
	if override, ok := g.hostConfigs[host]; ok {
		if override.MaxActive > 0 {
			state.maxActive = override.MaxActive
		}
		if override.MinInterval > 0 {
			state.minInterval = override.MinInterval
		}
	}
	return state
}

func (g *UpstreamGate) removeWaiterLocked(waiter *gateWaiter) {
	if waiter.element == nil {
		return
	}
	waiter.waitQueue.Remove(waiter.element)
	waiter.element = nil
	g.hostStateLocked(waiter.host).queued--
}

func (g *UpstreamGate) grantWaitersLocked() {
	for g.active < g.maxActive {
		selected, nextWake := g.selectWaiterLocked(time.Now())
		if selected == nil {
			g.scheduleWakeLocked(nextWake)
			return
		}
		g.removeWaiterLocked(selected)
		state := g.hostStateLocked(selected.host)
		g.active++
		state.active++
		state.lastStarted = time.Now()
		g.lastGrantedHost[selected.class] = selected.host
		if selected.class == AdmissionRefresh {
			g.foregroundBurst = 0
		} else {
			g.foregroundBurst++
		}
		selected.granted = true
		close(selected.ready)
	}
}

func (g *UpstreamGate) selectWaiterLocked(now time.Time) (*gateWaiter, time.Time) {
	var nextWake time.Time
	queues := []*list.List{&g.foreground, &g.refresh}
	classes := []AdmissionClass{AdmissionForeground, AdmissionRefresh}
	if firstRefresh := g.refresh.Front(); firstRefresh != nil &&
		(g.foregroundBurst >= maxForegroundBurst || now.Sub(firstRefresh.Value.(*gateWaiter).queuedAt) >= refreshAdmissionMaxWait) {
		queues[0], queues[1] = queues[1], queues[0]
		classes[0], classes[1] = classes[1], classes[0]
	}
	for i, queue := range queues {
		class := classes[i]
		seenHosts := map[string]struct{}{}
		var firstEligible *gateWaiter
		for element := queue.Front(); element != nil; element = element.Next() {
			waiter := element.Value.(*gateWaiter)
			if _, seen := seenHosts[waiter.host]; seen {
				continue
			}
			seenHosts[waiter.host] = struct{}{}
			state := g.hostStateLocked(waiter.host)
			if now.Before(state.cooldownUntil) {
				if nextWake.IsZero() || state.cooldownUntil.Before(nextWake) {
					nextWake = state.cooldownUntil
				}
				continue
			}
			nextStart := state.lastStarted.Add(state.minInterval)
			if state.minInterval > 0 && now.Before(nextStart) {
				if nextWake.IsZero() || nextStart.Before(nextWake) {
					nextWake = nextStart
				}
				continue
			}
			if state.active < state.maxActive {
				if firstEligible == nil {
					firstEligible = waiter
				}
				if waiter.host != g.lastGrantedHost[class] {
					return waiter, nextWake
				}
			}
		}
		if firstEligible != nil {
			return firstEligible, nextWake
		}
	}
	return nil, nextWake
}

func (g *UpstreamGate) scheduleWakeLocked(at time.Time) {
	if at.IsZero() || (g.wakeTimer != nil && !at.Before(g.wakeAt)) {
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
		if sequence == g.wakeSequence {
			g.wakeTimer = nil
			g.wakeAt = time.Time{}
			g.grantWaitersLocked()
		}
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
		return max(when.Sub(now), 0)
	}
	seconds, err := time.ParseDuration(value + "s")
	if err != nil || seconds <= 0 {
		return 0
	}
	return seconds
}
