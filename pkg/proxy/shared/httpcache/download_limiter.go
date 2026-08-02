package httpcache

import (
	"container/list"
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var ErrDownloadLimit = errors.New("download limit reached")

type RateLimitError struct {
	Host       string
	RetryAfter time.Time
}

func (e *RateLimitError) Error() string { return "upstream " + e.Host + " is rate limited" }
func (e *RateLimitError) Unwrap() error { return ErrDownloadLimit }

type downloadWaiter struct {
	instance   string
	host       string
	background bool
	ready      chan struct{}
	granted    bool
	err        error
	element    *list.Element
}

type hostLimit struct {
	until       time.Time
	lastLimited time.Time
	strikes     int
	active      int
	tokens      float64
	lastRefill  time.Time
	rateScale   float64
}

type DownloadLimiter struct {
	mu                sync.Mutex
	max               int
	perMax            int
	perHostMax        int
	backgroundMax     int
	active            int
	background        int
	perActive         map[string]int
	hosts             map[string]*hostLimit
	waiters           list.List
	perWait           map[string]int
	requestsPerSecond float64
	requestBurst      int
	foregroundWait    time.Duration
	wakeTimer         *time.Timer
}

type DownloadLimiterSnapshot struct {
	Active     int
	Background int
	Queued     int
	Hosts      map[string]DownloadHostSnapshot
}

type DownloadHostSnapshot struct {
	Active        int
	CooldownUntil time.Time
}

func NewDownloadLimiter(maxActive, maxPerInstance int) *DownloadLimiter {
	if maxActive <= 0 || maxPerInstance <= 0 {
		return nil
	}
	return &DownloadLimiter{
		max:               maxActive,
		perMax:            maxPerInstance,
		perHostMax:        maxPerInstance,
		backgroundMax:     1,
		perActive:         map[string]int{},
		hosts:             map[string]*hostLimit{},
		perWait:           map[string]int{},
		requestsPerSecond: 8,
		requestBurst:      4,
		foregroundWait:    3 * time.Second,
	}
}

func (l *DownloadLimiter) Configure(maxActive, maxPerInstance, maxPerHost, maxBackground int) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if maxActive > 0 {
		l.max = maxActive
	}
	if maxPerInstance > 0 {
		l.perMax = maxPerInstance
	}
	if maxPerHost > 0 {
		l.perHostMax = maxPerHost
	}
	if maxBackground > 0 {
		l.backgroundMax = maxBackground
	}
	l.grantWaitersLocked()
	l.mu.Unlock()
}

func (l *DownloadLimiter) ConfigurePacing(requestsPerSecond float64, burst int, foregroundWait time.Duration) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if requestsPerSecond > 0 {
		l.requestsPerSecond = requestsPerSecond
	}
	if burst > 0 {
		l.requestBurst = burst
	}
	if foregroundWait > 0 {
		l.foregroundWait = foregroundWait
	}
	for _, state := range l.hosts {
		if state.tokens > float64(l.requestBurst) {
			state.tokens = float64(l.requestBurst)
		}
	}
	l.grantWaitersLocked()
	l.mu.Unlock()
}

func (l *DownloadLimiter) Acquire(ctx context.Context, instance string) (func(), error) {
	return l.AcquireUpstream(ctx, instance, "", false)
}

func (l *DownloadLimiter) AcquireUpstream(ctx context.Context, instance, upstream string, background bool) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host := upstreamHost(upstream)

	l.mu.Lock()
	if limited := l.rateLimitErrorLocked(host, time.Now()); limited != nil {
		l.mu.Unlock()
		return nil, limited
	}
	now := time.Now()
	if l.waiters.Len() == 0 && l.hasCapacityLocked(instance, host, background) && l.takeTokenLocked(host, now) {
		l.activateLocked(instance, host, background)
		l.mu.Unlock()
		return l.releaseFunc(instance, host, background), nil
	}
	if background {
		l.mu.Unlock()
		return nil, ErrDownloadLimit
	}
	if l.waiters.Len() >= l.max || l.perWait[instance] >= l.perMax {
		l.mu.Unlock()
		return nil, ErrDownloadLimit
	}
	waiter := &downloadWaiter{instance: instance, host: host, background: background, ready: make(chan struct{})}
	waiter.element = l.waiters.PushBack(waiter)
	l.perWait[instance]++
	slog.Debug("upstream admission queued", "instance", instance, "host", host, "background", background, "active", l.active, "queued", l.waiters.Len())
	l.grantWaitersLocked()
	l.mu.Unlock()
	timer := time.NewTimer(l.foregroundWait)
	defer timer.Stop()

	select {
	case <-waiter.ready:
		if waiter.err != nil {
			return nil, waiter.err
		}
		return l.releaseFunc(instance, host, background), nil
	case <-ctx.Done():
		l.mu.Lock()
		if waiter.granted {
			l.mu.Unlock()
			l.release(instance, host, background)
		} else {
			l.removeWaiterLocked(waiter)
			l.mu.Unlock()
		}
		return nil, errors.Join(ErrDownloadLimit, ctx.Err())
	case <-timer.C:
		l.mu.Lock()
		if waiter.granted {
			l.mu.Unlock()
			l.release(instance, host, background)
		} else {
			l.removeWaiterLocked(waiter)
			l.mu.Unlock()
		}
		return nil, ErrDownloadLimit
	}
}

func (l *DownloadLimiter) ObserveResponse(upstream string, status int, retryAfter string) time.Time {
	if l == nil || status != http.StatusTooManyRequests {
		return time.Time{}
	}
	host := upstreamHost(upstream)
	if host == "" {
		return time.Time{}
	}
	now := time.Now()
	l.mu.Lock()
	state := l.hostStateLocked(host)
	if !state.lastLimited.IsZero() && now.Sub(state.lastLimited) > 30*time.Minute {
		state.strikes = 0
	}
	state.strikes++
	state.lastLimited = now
	currentScale := l.effectiveRateScaleLocked(state, now)
	state.rateScale = max(currentScale/2, 0.125)
	state.tokens = 0
	delay := parseRetryAfter(retryAfter, now)
	if delay <= 0 {
		delay = fallbackRateLimitBackoff(host, state.strikes)
	}
	if delay > 30*time.Minute {
		delay = 30 * time.Minute
	}
	if candidate := now.Add(delay); candidate.After(state.until) {
		state.until = candidate
	}
	until := state.until
	slog.Warn("upstream rate limit activated", "host", host, "retry_at", until, "backoff", until.Sub(now), "strikes", state.strikes)
	for element := l.waiters.Front(); element != nil; {
		next := element.Next()
		waiter := element.Value.(*downloadWaiter)
		if waiter.host == host {
			l.removeWaiterLocked(waiter)
			waiter.err = &RateLimitError{Host: host, RetryAfter: until}
			close(waiter.ready)
		}
		element = next
	}
	l.grantWaitersLocked()
	l.mu.Unlock()
	return until
}

func (l *DownloadLimiter) Cooldown(upstream string) (time.Time, bool) {
	if l == nil {
		return time.Time{}, false
	}
	host := upstreamHost(upstream)
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.rateLimitErrorLocked(host, time.Now())
	if err == nil {
		return time.Time{}, false
	}
	return err.RetryAfter, true
}

func (l *DownloadLimiter) Snapshot() DownloadLimiterSnapshot {
	snapshot := DownloadLimiterSnapshot{Hosts: map[string]DownloadHostSnapshot{}}
	if l == nil {
		return snapshot
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	snapshot.Active = l.active
	snapshot.Background = l.background
	snapshot.Queued = l.waiters.Len()
	for host, state := range l.hosts {
		if state.active == 0 && now.Sub(state.lastRefill) > 30*time.Minute &&
			(state.lastLimited.IsZero() || now.Sub(state.lastLimited) > 30*time.Minute) {
			delete(l.hosts, host)
			continue
		}
		hostSnapshot := DownloadHostSnapshot{Active: state.active}
		if now.Before(state.until) {
			hostSnapshot.CooldownUntil = state.until
		}
		if hostSnapshot.Active > 0 || !hostSnapshot.CooldownUntil.IsZero() {
			snapshot.Hosts[host] = hostSnapshot
		}
	}
	return snapshot
}

func (l *DownloadLimiter) hasCapacityLocked(instance, host string, background bool) bool {
	if l.active >= l.max || l.perActive[instance] >= l.perMax {
		return false
	}
	if background && l.background >= l.backgroundMax {
		return false
	}
	return host == "" || l.hostStateLocked(host).active < l.perHostMax
}

func (l *DownloadLimiter) activateLocked(instance, host string, background bool) {
	l.active++
	l.perActive[instance]++
	if host != "" {
		state := l.hostStateLocked(host)
		state.active++
	}
	if background {
		l.background++
	}
}

func (l *DownloadLimiter) hostStateLocked(host string) *hostLimit {
	state := l.hosts[host]
	if state == nil {
		state = &hostLimit{tokens: float64(l.requestBurst), lastRefill: time.Now(), rateScale: 1}
		l.hosts[host] = state
	}
	return state
}

func (l *DownloadLimiter) rateLimitErrorLocked(host string, now time.Time) *RateLimitError {
	if host == "" {
		return nil
	}
	state := l.hosts[host]
	if state == nil || state.until.IsZero() || !now.Before(state.until) {
		return nil
	}
	return &RateLimitError{Host: host, RetryAfter: state.until}
}

func (l *DownloadLimiter) removeWaiterLocked(waiter *downloadWaiter) {
	if waiter.element == nil {
		return
	}
	l.waiters.Remove(waiter.element)
	waiter.element = nil
	l.perWait[waiter.instance]--
	if l.perWait[waiter.instance] == 0 {
		delete(l.perWait, waiter.instance)
	}
}

func (l *DownloadLimiter) grantWaitersLocked() {
	now := time.Now()
	for l.active < l.max {
		var selected *downloadWaiter
		for element := l.waiters.Front(); element != nil; element = element.Next() {
			waiter := element.Value.(*downloadWaiter)
			if l.rateLimitErrorLocked(waiter.host, now) == nil && l.hasCapacityLocked(waiter.instance, waiter.host, waiter.background) && l.tokenAvailableLocked(waiter.host, now) {
				selected = waiter
				break
			}
		}
		if selected == nil {
			l.scheduleWakeLocked(now)
			return
		}
		_ = l.takeTokenLocked(selected.host, now)
		l.removeWaiterLocked(selected)
		l.activateLocked(selected.instance, selected.host, selected.background)
		selected.granted = true
		close(selected.ready)
	}
}

func (l *DownloadLimiter) takeTokenLocked(host string, now time.Time) bool {
	if host == "" || l.requestsPerSecond <= 0 {
		return true
	}
	state := l.hostStateLocked(host)
	l.refillTokensLocked(state, now)
	if state.tokens < 1 {
		return false
	}
	state.tokens--
	return true
}

func (l *DownloadLimiter) tokenAvailableLocked(host string, now time.Time) bool {
	if host == "" || l.requestsPerSecond <= 0 {
		return true
	}
	state := l.hostStateLocked(host)
	l.refillTokensLocked(state, now)
	return state.tokens >= 1
}

func (l *DownloadLimiter) refillTokensLocked(state *hostLimit, now time.Time) {
	if state.lastRefill.IsZero() {
		state.lastRefill = now
		state.tokens = float64(l.requestBurst)
		return
	}
	rate := l.requestsPerSecond * l.effectiveRateScaleLocked(state, now)
	state.tokens = min(float64(l.requestBurst), state.tokens+now.Sub(state.lastRefill).Seconds()*rate)
	state.lastRefill = now
}

func (l *DownloadLimiter) effectiveRateScaleLocked(state *hostLimit, now time.Time) float64 {
	if state.rateScale <= 0 {
		return 1
	}
	if state.rateScale >= 1 || state.lastLimited.IsZero() {
		return state.rateScale
	}
	recovered := now.Sub(state.lastLimited).Seconds() / (30 * time.Minute).Seconds()
	return min(1, state.rateScale+(1-state.rateScale)*max(recovered, 0))
}

func (l *DownloadLimiter) scheduleWakeLocked(now time.Time) {
	if l.waiters.Len() == 0 || l.requestsPerSecond <= 0 || l.wakeTimer != nil {
		return
	}
	delay := time.Second / time.Duration(max(l.requestsPerSecond, 1))
	for element := l.waiters.Front(); element != nil; element = element.Next() {
		waiter := element.Value.(*downloadWaiter)
		if waiter.host == "" || !l.hasCapacityLocked(waiter.instance, waiter.host, waiter.background) {
			continue
		}
		state := l.hostStateLocked(waiter.host)
		l.refillTokensLocked(state, now)
		rate := l.requestsPerSecond * l.effectiveRateScaleLocked(state, now)
		if rate > 0 {
			candidate := time.Duration(max((1-state.tokens)/rate, 0) * float64(time.Second))
			if candidate < delay {
				delay = candidate
			}
		}
	}
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	l.wakeTimer = time.AfterFunc(delay, func() {
		l.mu.Lock()
		l.wakeTimer = nil
		l.grantWaitersLocked()
		l.mu.Unlock()
	})
}

func (l *DownloadLimiter) releaseFunc(instance, host string, background bool) func() {
	var once sync.Once
	return func() { once.Do(func() { l.release(instance, host, background) }) }
}

func (l *DownloadLimiter) release(instance, host string, background bool) {
	l.mu.Lock()
	l.active--
	l.perActive[instance]--
	if l.perActive[instance] == 0 {
		delete(l.perActive, instance)
	}
	if host != "" {
		state := l.hostStateLocked(host)
		state.active--
	}
	if background {
		l.background--
	}
	l.grantWaitersLocked()
	l.mu.Unlock()
}

func (l *DownloadLimiter) Update(maxActive, maxPerInstance int) {
	l.Configure(maxActive, maxPerInstance, 0, 0)
}

func upstreamHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Host)
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

func fallbackRateLimitBackoff(host string, strikes int) time.Duration {
	base := 30 * time.Second * time.Duration(1<<min(max(strikes-1, 0), 5))
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(host))
	_, _ = hash.Write([]byte{byte(strikes)})
	percent := 80 + int(hash.Sum32()%41)
	return base * time.Duration(percent) / 100
}
