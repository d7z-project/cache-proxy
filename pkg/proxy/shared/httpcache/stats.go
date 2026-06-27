package httpcache

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metricsCollector struct {
	requestsTotal         *prometheus.CounterVec
	responseBytesTotal    *prometheus.CounterVec
	upstreamRequestsTotal *prometheus.CounterVec
	activeDownloads       *prometheus.GaugeVec
	metadataRefreshTotal  *prometheus.CounterVec
	metadataRefreshTime   *prometheus.HistogramVec
	metadataSnapshotReady *prometheus.GaugeVec
	upstreamHealth        *prometheus.GaugeVec
	upstreamWeight        *prometheus.GaugeVec
	upstreamErrorRate     *prometheus.GaugeVec
	upstreamLatency       *prometheus.GaugeVec
	circuitEvents         *prometheus.CounterVec
}

func newMetricsCollector(reg prometheus.Registerer) *metricsCollector {
	mc := &metricsCollector{
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_requests_total",
			Help: "Total proxy requests by instance, mode, method, cache result and status.",
		}, []string{"instance", "mode", "method", "cache", "status"}),
		responseBytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_response_bytes_total",
			Help: "Total proxy response bytes estimated from Content-Length.",
		}, []string{"instance", "mode", "cache"}),
		upstreamRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_upstream_requests_total",
			Help: "Total upstream requests by instance, mode, method and status.",
		}, []string{"instance", "mode", "method", "status"}),
		activeDownloads: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_active_downloads",
			Help: "Active cache downloads by instance and mode.",
		}, []string{"instance", "mode"}),
		metadataRefreshTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_metadata_refresh_total",
			Help: "Total metadata refresh cycles by instance, mode, and result.",
		}, []string{"instance", "mode", "result"}),
		metadataRefreshTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_proxy_metadata_refresh_duration_seconds",
			Help:    "Metadata refresh cycle duration by instance and mode.",
			Buckets: prometheus.DefBuckets,
		}, []string{"instance", "mode"}),
		metadataSnapshotReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_metadata_snapshot_ready",
			Help: "Whether the instance currently has at least one successfully loaded metadata snapshot.",
		}, []string{"instance", "mode"}),
		upstreamHealth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_upstream_health",
			Help: "Upstream health state: 0=closed, 1=degraded, 2=open, 3=halfopen.",
		}, []string{"instance", "mode", "upstream"}),
		upstreamWeight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_upstream_weight",
			Help: "Current upstream traffic weight (0-1).",
		}, []string{"instance", "mode", "upstream"}),
		upstreamErrorRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_upstream_error_rate",
			Help: "Sliding-window error rate for the upstream (0-1).",
		}, []string{"instance", "mode", "upstream"}),
		upstreamLatency: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_upstream_latency_seconds",
			Help: "EWMA latency for the upstream in seconds.",
		}, []string{"instance", "mode", "upstream"}),
		circuitEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_circuit_breaker_events_total",
			Help: "Circuit breaker events by transition type.",
		}, []string{"instance", "mode", "upstream", "event"}),
	}
	reg.MustRegister(mc.requestsTotal, mc.responseBytesTotal, mc.upstreamRequestsTotal, mc.activeDownloads, mc.metadataRefreshTotal, mc.metadataRefreshTime, mc.metadataSnapshotReady, mc.upstreamHealth, mc.upstreamWeight, mc.upstreamErrorRate, mc.upstreamLatency, mc.circuitEvents)
	return mc
}

type instanceEntry struct {
	mu   sync.Mutex
	data InstanceStats
}

type Stats struct {
	instances sync.Map // string -> *instanceEntry
	mc        *metricsCollector

	totalRequests      atomic.Uint64
	totalErrors        atomic.Uint64
	totalResponseBytes atomic.Uint64
	totalUpstreamReqs  atomic.Uint64
	totalUpstreamErrs  atomic.Uint64
	totalActiveDown    atomic.Int64
	totalRefreshes     atomic.Uint64
	totalRefreshFails  atomic.Uint64
	totalSnapshotReady atomic.Bool

	totalMu          sync.Mutex
	totalMetadataState string
	totalLastRefresh   string
	totalLastRefreshAt time.Time
	totalCache         map[string]uint64
	totalUpstreamSt    map[string]uint64
}

type StatsSnapshot struct {
	Total     InstanceStats            `json:"total"`
	Instances map[string]InstanceStats `json:"instances"`
}

type InstanceStats struct {
	Mode             string            `json:"mode,omitempty"`
	Requests         uint64            `json:"requests"`
	Errors           uint64            `json:"errors"`
	ResponseBytes    uint64            `json:"responseBytes"`
	Cache            map[string]uint64 `json:"cache"`
	UpstreamRequests uint64            `json:"upstreamRequests"`
	UpstreamErrors   uint64            `json:"upstreamErrors"`
	UpstreamStatus   map[string]uint64 `json:"upstreamStatus"`
	ActiveDownloads  int64             `json:"activeDownloads"`
	MetadataState    string            `json:"metadataState,omitempty"`
	SnapshotReady    bool              `json:"snapshotReady"`
	Refreshes        uint64            `json:"refreshes"`
	RefreshFailures  uint64            `json:"refreshFailures"`
	LastRefresh      string            `json:"lastRefresh,omitempty"`
	LastRefreshAt    time.Time         `json:"lastRefreshAt,omitempty"`
}

func NewStats(reg prometheus.Registerer) *Stats {
	return &Stats{
		mc:              newMetricsCollector(reg),
		totalCache:      map[string]uint64{},
		totalUpstreamSt: map[string]uint64{},
	}
}

func (s *Stats) RecordRequest(instance, mode, method, cache string, status int, bytes uint64) {
	if s == nil {
		return
	}
	if cache == "" {
		cache = "UNKNOWN"
	}
	statusText := strconv.Itoa(status)
	s.mc.requestsTotal.WithLabelValues(instance, mode, method, cache, statusText).Inc()
	if bytes > 0 {
		s.mc.responseBytesTotal.WithLabelValues(instance, mode, cache).Add(float64(bytes))
	}

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.Requests++
	entry.data.ResponseBytes += bytes
	entry.data.Cache[cache]++
	if status >= 500 {
		entry.data.Errors++
	}
	entry.mu.Unlock()

	s.totalRequests.Add(1)
	s.totalResponseBytes.Add(bytes)
	s.incrTotalCache(cache)
	if status >= 500 {
		s.totalErrors.Add(1)
	}
}

func (s *Stats) RecordUpstream(instance, mode, method string, status int) {
	if s == nil {
		return
	}
	statusText := strconv.Itoa(status)
	if status == 0 {
		statusText = "error"
	}
	s.mc.upstreamRequestsTotal.WithLabelValues(instance, mode, method, statusText).Inc()

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.UpstreamRequests++
	entry.data.UpstreamStatus[statusText]++
	if status == 0 || status >= 500 {
		entry.data.UpstreamErrors++
	}
	entry.mu.Unlock()

	s.totalUpstreamReqs.Add(1)
	s.incrTotalUpstreamStatus(statusText)
	if status == 0 || status >= 500 {
		s.totalUpstreamErrs.Add(1)
	}
}

func (s *Stats) AddActiveDownload(instance, mode string, delta int64) {
	if s == nil {
		return
	}
	s.mc.activeDownloads.WithLabelValues(instance, mode).Add(float64(delta))

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.ActiveDownloads += delta
	entry.mu.Unlock()

	s.totalActiveDown.Add(delta)
}

func (s *Stats) RecordMetadataRefresh(instance, mode, result string, duration time.Duration, ready bool) {
	if s == nil {
		return
	}
	if result == "" {
		result = "unknown"
	}
	s.mc.metadataRefreshTotal.WithLabelValues(instance, mode, result).Inc()
	s.mc.metadataRefreshTime.WithLabelValues(instance, mode).Observe(duration.Seconds())
	readyVal := float64(0)
	if ready {
		readyVal = 1
	}
	s.mc.metadataSnapshotReady.WithLabelValues(instance, mode).Set(readyVal)

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.Refreshes++
	entry.data.LastRefresh = result
	entry.data.LastRefreshAt = time.Now()
	entry.data.SnapshotReady = ready
	if result != "success" {
		entry.data.RefreshFailures++
	}
	entry.mu.Unlock()

	s.totalRefreshes.Add(1)
	s.totalMu.Lock()
	s.totalLastRefresh = result
	s.totalLastRefreshAt = time.Now()
	if result != "success" {
		s.totalRefreshFails.Add(1)
	}
	if ready {
		s.totalSnapshotReady.Store(true)
	}
	s.totalMu.Unlock()
}

func (s *Stats) SetMetadataState(instance, mode, state string, ready bool) {
	if s == nil {
		return
	}
	readyVal := float64(0)
	if ready {
		readyVal = 1
	}
	s.mc.metadataSnapshotReady.WithLabelValues(instance, mode).Set(readyVal)

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.MetadataState = state
	entry.data.LastRefreshAt = time.Now()
	entry.data.SnapshotReady = ready
	entry.mu.Unlock()

	s.totalMu.Lock()
	s.totalMetadataState = state
	if ready {
		s.totalSnapshotReady.Store(true)
	}
	s.totalMu.Unlock()
}

func (s *Stats) SetUpstreamHealth(instance, mode, upstream string, state int, weight, errorRate, latencySecs float64) {
	if s == nil {
		return
	}
	s.mc.upstreamHealth.WithLabelValues(instance, mode, upstream).Set(float64(state))
	s.mc.upstreamWeight.WithLabelValues(instance, mode, upstream).Set(weight)
	s.mc.upstreamErrorRate.WithLabelValues(instance, mode, upstream).Set(errorRate)
	s.mc.upstreamLatency.WithLabelValues(instance, mode, upstream).Set(latencySecs)
}

func (s *Stats) RecordCircuitEvent(instance, mode, upstream, event string) {
	if s == nil {
		return
	}
	s.mc.circuitEvents.WithLabelValues(instance, mode, upstream, event).Inc()
}

func (s *Stats) Snapshot() StatsSnapshot {
	if s == nil {
		return StatsSnapshot{Total: emptyInstanceStats(""), Instances: map[string]InstanceStats{}}
	}
	total := InstanceStats{
		Requests:         s.totalRequests.Load(),
		Errors:           s.totalErrors.Load(),
		ResponseBytes:    s.totalResponseBytes.Load(),
		UpstreamRequests: s.totalUpstreamReqs.Load(),
		UpstreamErrors:   s.totalUpstreamErrs.Load(),
		ActiveDownloads:  s.totalActiveDown.Load(),
		Refreshes:        s.totalRefreshes.Load(),
		RefreshFailures:  s.totalRefreshFails.Load(),
		SnapshotReady:    s.totalSnapshotReady.Load(),
	}
	s.totalMu.Lock()
	total.MetadataState = s.totalMetadataState
	total.LastRefresh = s.totalLastRefresh
	total.LastRefreshAt = s.totalLastRefreshAt
	total.Cache = cloneMap(s.totalCache)
	total.UpstreamStatus = cloneMap(s.totalUpstreamSt)
	s.totalMu.Unlock()

	result := StatsSnapshot{Total: total, Instances: map[string]InstanceStats{}}
	s.instances.Range(func(key, value interface{}) bool {
		entry := value.(*instanceEntry)
		entry.mu.Lock()
		result.Instances[key.(string)] = cloneInstanceStats(entry.data)
		entry.mu.Unlock()
		return true
	})
	return result
}

func (s *Stats) getOrCreateEntry(name, mode string) *instanceEntry {
	if entry, ok := s.instances.Load(name); ok {
		e := entry.(*instanceEntry)
		e.mu.Lock()
		if e.data.Mode == "" {
			e.data.Mode = mode
		}
		e.mu.Unlock()
		return e
	}
	entry := &instanceEntry{data: emptyInstanceStats(mode)}
	actual, _ := s.instances.LoadOrStore(name, entry)
	return actual.(*instanceEntry)
}

func (s *Stats) incrTotalCache(cache string) {
	s.totalMu.Lock()
	s.totalCache[cache]++
	s.totalMu.Unlock()
}

func (s *Stats) incrTotalUpstreamStatus(status string) {
	s.totalMu.Lock()
	s.totalUpstreamSt[status]++
	s.totalMu.Unlock()
}

func emptyInstanceStats(mode string) InstanceStats {
	return InstanceStats{Mode: mode, Cache: map[string]uint64{}, UpstreamStatus: map[string]uint64{}}
}

func cloneInstanceStats(item InstanceStats) InstanceStats {
	clone := item
	clone.Cache = cloneMap(item.Cache)
	clone.UpstreamStatus = cloneMap(item.UpstreamStatus)
	return clone
}

func cloneMap(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return map[string]uint64{}
	}
	dst := make(map[string]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
