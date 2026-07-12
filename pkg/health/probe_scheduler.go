package health

import (
	"context"
	"hash/fnv"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultMinHostProbeInterval = 30 * time.Second

type probeSchedulerConfig struct {
	MinHostInterval time.Duration
	JitterFraction  float64
}

// ProbeScheduler coordinates active health probes across services by upstream host.
type ProbeScheduler struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	wake     chan struct{}
	finished chan probeFinished

	config   probeSchedulerConfig
	services map[*ServiceHealth]struct{}
	jobs     map[probeJobKey]*probeJob
	hosts    map[string]*probeHost
}

type probeJobKey struct {
	service     *ServiceHealth
	upstreamURL string
}

type probeJob struct {
	key     probeJobKey
	hostKey string
	nextAt  time.Time
}

type probeHost struct {
	nextAt   time.Time
	inflight bool
	cursor   int
}

type probeCandidate struct {
	service     *ServiceHealth
	upstreamURL string
	hostKey     string
	interval    time.Duration
	dueAt       time.Time
	lastProbeAt time.Time
}

type probeFinished struct {
	hostKey string
}

// NewProbeScheduler starts an in-memory active probe scheduler.
func NewProbeScheduler(parent context.Context) *ProbeScheduler {
	return newProbeScheduler(parent, probeSchedulerConfig{
		MinHostInterval: defaultMinHostProbeInterval,
		JitterFraction:  0.2,
	})
}

func newProbeScheduler(parent context.Context, cfg probeSchedulerConfig) *ProbeScheduler {
	if parent == nil {
		parent = context.Background()
	}
	if cfg.MinHostInterval <= 0 {
		cfg.MinHostInterval = defaultMinHostProbeInterval
	}
	ctx, cancel := context.WithCancel(parent)
	s := &ProbeScheduler{
		ctx:      ctx,
		cancel:   cancel,
		wake:     make(chan struct{}, 1),
		finished: make(chan probeFinished, 128),
		config:   cfg,
		services: map[*ServiceHealth]struct{}{},
		jobs:     map[probeJobKey]*probeJob{},
		hosts:    map[string]*probeHost{},
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

func (s *ProbeScheduler) register(h *ServiceHealth) {
	if s == nil || h == nil {
		return
	}
	s.mu.Lock()
	s.services[h] = struct{}{}
	s.mu.Unlock()
	s.notify()
}

func (s *ProbeScheduler) unregister(h *ServiceHealth) {
	if s == nil || h == nil {
		return
	}
	s.mu.Lock()
	delete(s.services, h)
	for key := range s.jobs {
		if key.service == h {
			delete(s.jobs, key)
		}
	}
	s.pruneHostsLocked()
	s.mu.Unlock()
	s.notify()
}

// Stop cancels the scheduler and waits for its goroutines to exit.
func (s *ProbeScheduler) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ProbeScheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *ProbeScheduler) loop() {
	defer s.wg.Done()

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		wait := s.dispatchDue()
		resetTimer(timer, wait)
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		case <-s.wake:
		case finished := <-s.finished:
			s.markFinished(finished.hostKey)
		}
	}
}

func resetTimer(timer *time.Timer, wait time.Duration) {
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(wait)
}

func (s *ProbeScheduler) dispatchDue() time.Duration {
	now := time.Now()
	var ready []*probeJob

	s.mu.Lock()
	s.reconcileLocked(now)
	nextWake := time.Minute
	for _, job := range s.jobs {
		host := s.hostLocked(job.hostKey)
		dueAt := maxTime(job.nextAt, host.nextAt)
		if host.inflight {
			continue
		}
		if dueAt.After(now) {
			nextWake = minDuration(nextWake, dueAt.Sub(now))
			continue
		}
		ready = append(ready, job)
	}
	ready = s.selectReadyByHostLocked(ready)
	for _, job := range ready {
		host := s.hostLocked(job.hostKey)
		host.inflight = true
		job.nextAt = now.Add(s.serviceProbeInterval(job.key.service, job.key.upstreamURL)).Add(s.hostJitter(job.hostKey))
		s.wg.Add(1)
		go s.runProbe(job.key.service, job.key.upstreamURL, job.hostKey)
	}
	s.mu.Unlock()

	if len(ready) > 0 {
		return time.Millisecond
	}
	return nextWake
}

func (s *ProbeScheduler) reconcileLocked(now time.Time) {
	active := map[probeJobKey]probeCandidate{}
	for service := range s.services {
		for _, candidate := range service.probeCandidates(now) {
			active[candidate.key()] = candidate
		}
	}
	for key := range s.jobs {
		if _, ok := active[key]; !ok {
			delete(s.jobs, key)
		}
	}
	for key, candidate := range active {
		job := s.jobs[key]
		if job == nil {
			s.jobs[key] = &probeJob{
				key:     key,
				hostKey: candidate.hostKey,
				nextAt:  now.Add(candidate.interval).Add(s.hostJitter(candidate.hostKey)),
			}
			continue
		}
		job.hostKey = candidate.hostKey
		if !candidate.lastProbeAt.IsZero() {
			job.nextAt = candidate.dueAt
			continue
		}
		if candidate.dueAt.Before(job.nextAt) {
			job.nextAt = candidate.dueAt
		}
	}
	s.pruneHostsLocked()
}

func (c probeCandidate) key() probeJobKey {
	return probeJobKey{service: c.service, upstreamURL: c.upstreamURL}
}

func (s *ProbeScheduler) selectReadyByHostLocked(jobs []*probeJob) []*probeJob {
	byHost := map[string][]*probeJob{}
	for _, job := range jobs {
		byHost[job.hostKey] = append(byHost[job.hostKey], job)
	}
	selected := make([]*probeJob, 0, len(byHost))
	for hostKey, hostJobs := range byHost {
		sort.Slice(hostJobs, func(i, j int) bool {
			if hostJobs[i].key.service.name == hostJobs[j].key.service.name {
				return hostJobs[i].key.upstreamURL < hostJobs[j].key.upstreamURL
			}
			return hostJobs[i].key.service.name < hostJobs[j].key.service.name
		})
		host := s.hostLocked(hostKey)
		idx := host.cursor % len(hostJobs)
		host.cursor++
		selected = append(selected, hostJobs[idx])
	}
	return selected
}

func (s *ProbeScheduler) serviceProbeInterval(service *ServiceHealth, upstreamURL string) time.Duration {
	interval := service.probeInterval(upstreamURL)
	if interval < s.config.MinHostInterval {
		return s.config.MinHostInterval
	}
	return interval
}

func (s *ProbeScheduler) runProbe(service *ServiceHealth, upstreamURL, hostKey string) {
	defer s.wg.Done()
	defer func() {
		select {
		case s.finished <- probeFinished{hostKey: hostKey}:
		case <-s.ctx.Done():
		}
	}()
	if !service.beginActiveProbe() {
		return
	}
	defer service.finishActiveProbe()
	service.probeOneContext(service.context(), upstreamURL)
}

func (s *ProbeScheduler) markFinished(hostKey string) {
	s.mu.Lock()
	host := s.hostLocked(hostKey)
	host.inflight = false
	host.nextAt = time.Now().Add(s.config.MinHostInterval).Add(s.hostJitter(hostKey))
	s.pruneHostsLocked()
	s.mu.Unlock()
}

func (s *ProbeScheduler) hostLocked(hostKey string) *probeHost {
	host := s.hosts[hostKey]
	if host == nil {
		host = &probeHost{}
		s.hosts[hostKey] = host
	}
	return host
}

func (s *ProbeScheduler) pruneHostsLocked() {
	used := map[string]struct{}{}
	for _, job := range s.jobs {
		used[job.hostKey] = struct{}{}
	}
	for hostKey, host := range s.hosts {
		if host.inflight {
			continue
		}
		if _, ok := used[hostKey]; !ok {
			delete(s.hosts, hostKey)
		}
	}
}

func (s *ProbeScheduler) hostJitter(hostKey string) time.Duration {
	if s.config.JitterFraction <= 0 {
		return 0
	}
	window := int64(float64(s.config.MinHostInterval) * s.config.JitterFraction)
	if window <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(hostKey))
	return time.Duration(int64(hash.Sum64() % uint64(window)))
}

func (h *ServiceHealth) probeCandidates(now time.Time) []probeCandidate {
	h.mu.RLock()
	defer h.mu.RUnlock()

	candidates := make([]probeCandidate, 0, len(h.upstreams))
	for upstreamURL, uh := range h.upstreams {
		if len(h.probeTargetsForUpstreamLocked(upstreamURL)) == 0 {
			continue
		}
		hostKey, ok := normalizeProbeHost(upstreamURL)
		if !ok {
			continue
		}
		interval := h.probeIntervalLocked(uh)
		dueAt := now.Add(interval)
		if !uh.lastProbeAt.IsZero() {
			dueAt = uh.lastProbeAt.Add(interval)
		}
		candidates = append(candidates, probeCandidate{
			service:     h,
			upstreamURL: upstreamURL,
			hostKey:     hostKey,
			interval:    interval,
			dueAt:       dueAt,
			lastProbeAt: uh.lastProbeAt,
		})
	}
	return candidates
}

func (h *ServiceHealth) probeInterval(upstreamURL string) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()

	uh := h.upstreams[upstreamURL]
	if uh == nil {
		return h.config.ProbeInterval
	}
	return h.probeIntervalLocked(uh)
}

func (h *ServiceHealth) probeIntervalLocked(uh *UpstreamHealth) time.Duration {
	switch uh.State {
	case SOpen, SHalfOpen:
		return h.config.CanaryCooldown
	default:
		return h.config.ProbeInterval
	}
}

func (h *ServiceHealth) context() context.Context {
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	if h.ctx != nil {
		return h.ctx
	}
	return context.Background()
}

func normalizeProbeHost(upstreamURL string) (string, bool) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", false
	}
	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	switch {
	case port == "" || parsed.Scheme == "https" && port == "443" || parsed.Scheme == "http" && port == "80":
		return parsed.Scheme + "://" + host, true
	default:
		return parsed.Scheme + "://" + net.JoinHostPort(host, port), true
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
