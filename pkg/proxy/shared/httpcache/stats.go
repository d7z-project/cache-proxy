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
	repositoryMaintenance *prometheus.CounterVec
	repositoryObjects     *prometheus.CounterVec
	upstreamErrorRate     *prometheus.GaugeVec
	upstreamLatency       *prometheus.GaugeVec
}

func newMetricsCollector(reg prometheus.Registerer) *metricsCollector {
	metrics := &metricsCollector{
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
		repositoryMaintenance: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_repository_maintenance_total",
			Help: "Total repository maintenance operations by instance, mode, operation, and result.",
		}, []string{"instance", "mode", "operation", "result"}),
		repositoryObjects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_repository_maintenance_objects_total",
			Help: "Repository objects processed by maintenance operation and disposition.",
		}, []string{"instance", "mode", "operation", "disposition"}),
		upstreamErrorRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_upstream_error_rate",
			Help: "Sliding-window error rate for the upstream (0-1).",
		}, []string{"instance", "mode", "upstream"}),
		upstreamLatency: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_upstream_latency_seconds",
			Help: "EWMA latency for the upstream in seconds.",
		}, []string{"instance", "mode", "upstream"}),
	}
	reg.MustRegister(metrics.requestsTotal, metrics.responseBytesTotal, metrics.upstreamRequestsTotal, metrics.activeDownloads, metrics.metadataRefreshTotal, metrics.metadataRefreshTime, metrics.metadataSnapshotReady, metrics.repositoryMaintenance, metrics.repositoryObjects, metrics.upstreamErrorRate, metrics.upstreamLatency)
	return metrics
}

type instanceEntry struct {
	mu   sync.Mutex
	data InstanceStats
}

type Stats struct {
	instances sync.Map // string -> *instanceEntry
	metrics   *metricsCollector

	totalRequests      atomic.Uint64
	totalErrors        atomic.Uint64
	totalResponseBytes atomic.Uint64
	totalUpstreamReqs  atomic.Uint64
	totalUpstreamErrs  atomic.Uint64
	totalUpstreamBytes atomic.Uint64
	totalActiveDown    atomic.Int64
	totalActiveUp      atomic.Int64
	totalRefreshes     atomic.Uint64
	totalRefreshFails  atomic.Uint64
	totalSnapshotReady atomic.Bool

	totalMu            sync.Mutex
	totalMetadataState string
	totalLastRefresh   string
	totalLastRefreshAt time.Time
	totalCache         map[string]uint64
	totalUpstreamSt    map[string]uint64
	totalMaintenance   map[string]uint64
}

type StatsSnapshot struct {
	Total     InstanceStats            `json:"total"`
	Instances map[string]InstanceStats `json:"instances"`
}

type InstanceStats struct {
	Mode              string                   `json:"mode,omitempty"`
	Requests          uint64                   `json:"requests"`
	Errors            uint64                   `json:"errors"`
	ResponseBytes     uint64                   `json:"responseBytes"`
	Cache             map[string]uint64        `json:"cache"`
	UpstreamRequests  uint64                   `json:"upstreamRequests"`
	UpstreamErrors    uint64                   `json:"upstreamErrors"`
	UpstreamBytes     uint64                   `json:"upstreamBytes"`
	UpstreamStatus    map[string]uint64        `json:"upstreamStatus"`
	Upstreams         map[string]UpstreamStats `json:"upstreams,omitempty"`
	ActiveDownloads   int64                    `json:"activeDownloads"`
	ActiveUpstreams   int64                    `json:"activeUpstreams"`
	MetadataState     string                   `json:"metadataState,omitempty"`
	SnapshotReady     bool                     `json:"snapshotReady"`
	Refreshes         uint64                   `json:"refreshes"`
	RefreshFailures   uint64                   `json:"refreshFailures"`
	LastRefresh       string                   `json:"lastRefresh,omitempty"`
	LastRefreshAt     time.Time                `json:"lastRefreshAt,omitempty"`
	LastRefreshOKAt   time.Time                `json:"lastRefreshOkAt,omitempty"`
	LastStateChangeAt time.Time                `json:"lastStateChangeAt,omitempty"`
	Maintenance       map[string]uint64        `json:"maintenance,omitempty"`
}

type UpstreamStats struct {
	URL            string            `json:"url"`
	Requests       uint64            `json:"requests"`
	Errors         uint64            `json:"errors"`
	ResponseBytes  uint64            `json:"responseBytes"`
	Status         map[string]uint64 `json:"status"`
	ActiveRequests int64             `json:"activeRequests"`
	LastStatus     string            `json:"lastStatus,omitempty"`
	LastUsedAt     time.Time         `json:"lastUsedAt,omitempty"`
	LastError      string            `json:"lastError,omitempty"`
	ErrorRate      float64           `json:"errorRate"`
	LatencySeconds float64           `json:"latencySeconds"`
}

func NewStats(reg prometheus.Registerer) *Stats {
	return &Stats{
		metrics:          newMetricsCollector(reg),
		totalCache:       map[string]uint64{},
		totalUpstreamSt:  map[string]uint64{},
		totalMaintenance: map[string]uint64{},
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
	s.metrics.requestsTotal.WithLabelValues(instance, mode, method, cache, statusText).Inc()
	if bytes > 0 {
		s.metrics.responseBytesTotal.WithLabelValues(instance, mode, cache).Add(float64(bytes))
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
	s.RecordUpstreamRequest(instance, mode, "", method, status, 0, 0)
}

func (s *Stats) RecordUpstreamRequest(
	instance string,
	mode string,
	upstream string,
	method string,
	status int,
	latency time.Duration,
	bytes uint64,
) {
	if s == nil {
		return
	}
	statusText := strconv.Itoa(status)
	if status == 0 {
		statusText = "error"
	}
	failed := upstreamStatusIsFailure(status)
	s.metrics.upstreamRequestsTotal.WithLabelValues(instance, mode, method, statusText).Inc()

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	ensureStatsMaps(&entry.data)
	entry.data.UpstreamRequests++
	entry.data.UpstreamStatus[statusText]++
	entry.data.UpstreamBytes += bytes
	if failed {
		entry.data.UpstreamErrors++
	}
	if upstream != "" {
		upstreamStats := entry.data.Upstreams[upstream]
		if upstreamStats.URL == "" {
			upstreamStats.URL = upstream
		}
		if upstreamStats.Status == nil {
			upstreamStats.Status = map[string]uint64{}
		}
		upstreamStats.Requests++
		upstreamStats.Status[statusText]++
		upstreamStats.ResponseBytes += bytes
		upstreamStats.LastStatus = statusText
		upstreamStats.LastUsedAt = time.Now()
		upstreamStats.LatencySeconds = latency.Seconds()
		if failed {
			upstreamStats.Errors++
			upstreamStats.LastError = statusText
		} else {
			upstreamStats.LastError = ""
		}
		entry.data.Upstreams[upstream] = upstreamStats
	}
	entry.mu.Unlock()

	s.totalUpstreamReqs.Add(1)
	s.totalUpstreamBytes.Add(bytes)
	s.incrTotalUpstreamStatus(statusText)
	if failed {
		s.totalUpstreamErrs.Add(1)
	}
}

func (s *Stats) BeginUpstreamRequest(instance, mode, upstream string) func() {
	if s == nil || upstream == "" {
		return func() {}
	}
	s.addActiveUpstream(instance, mode, upstream, 1)
	var released atomic.Bool
	return func() {
		if released.CompareAndSwap(false, true) {
			s.addActiveUpstream(instance, mode, upstream, -1)
		}
	}
}

func (s *Stats) addActiveUpstream(instance, mode, upstream string, delta int64) {
	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	ensureStatsMaps(&entry.data)
	entry.data.ActiveUpstreams += delta
	if entry.data.ActiveUpstreams < 0 {
		entry.data.ActiveUpstreams = 0
	}
	upstreamStats := entry.data.Upstreams[upstream]
	if upstreamStats.URL == "" {
		upstreamStats.URL = upstream
	}
	if upstreamStats.Status == nil {
		upstreamStats.Status = map[string]uint64{}
	}
	upstreamStats.ActiveRequests += delta
	if upstreamStats.ActiveRequests < 0 {
		upstreamStats.ActiveRequests = 0
	}
	entry.data.Upstreams[upstream] = upstreamStats
	entry.mu.Unlock()

	s.totalActiveUp.Add(delta)
	if s.totalActiveUp.Load() < 0 {
		s.totalActiveUp.Store(0)
	}
}

func (s *Stats) AddActiveDownload(instance, mode string, delta int64) {
	if s == nil {
		return
	}
	s.metrics.activeDownloads.WithLabelValues(instance, mode).Add(float64(delta))

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
	s.metrics.metadataRefreshTotal.WithLabelValues(instance, mode, result).Inc()
	s.metrics.metadataRefreshTime.WithLabelValues(instance, mode).Observe(duration.Seconds())
	readyVal := float64(0)
	if ready {
		readyVal = 1
	}
	s.metrics.metadataSnapshotReady.WithLabelValues(instance, mode).Set(readyVal)

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.Refreshes++
	entry.data.LastRefresh = result
	entry.data.LastRefreshAt = time.Now()
	if result == "success" {
		entry.data.LastRefreshOKAt = entry.data.LastRefreshAt
	}
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
	s.metrics.metadataSnapshotReady.WithLabelValues(instance, mode).Set(readyVal)

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	entry.data.MetadataState = state
	entry.data.LastStateChangeAt = time.Now()
	entry.data.SnapshotReady = ready
	entry.mu.Unlock()

	s.totalMu.Lock()
	s.totalMetadataState = state
	if ready {
		s.totalSnapshotReady.Store(true)
	}
	s.totalMu.Unlock()
}

func (s *Stats) RecordRepositoryMaintenance(instance, mode, operation, result string, counts map[string]int) {
	if s == nil {
		return
	}
	if result == "" {
		result = "unknown"
	}
	s.metrics.repositoryMaintenance.WithLabelValues(instance, mode, operation, result).Inc()
	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	ensureStatsMaps(&entry.data)
	entry.data.Maintenance[operation+"."+result]++
	for disposition, count := range counts {
		if count <= 0 {
			continue
		}
		s.metrics.repositoryObjects.WithLabelValues(instance, mode, operation, disposition).Add(float64(count))
		entry.data.Maintenance[operation+"."+disposition] += uint64(count)
	}
	entry.mu.Unlock()

	s.totalMu.Lock()
	s.totalMaintenance[operation+"."+result]++
	for disposition, count := range counts {
		if count > 0 {
			s.totalMaintenance[operation+"."+disposition] += uint64(count)
		}
	}
	s.totalMu.Unlock()
}

func (s *Stats) SetUpstreamObservation(instance, mode, upstream string, errorRate, latencySecs float64) {
	if s == nil {
		return
	}
	s.metrics.upstreamErrorRate.WithLabelValues(instance, mode, upstream).Set(errorRate)
	s.metrics.upstreamLatency.WithLabelValues(instance, mode, upstream).Set(latencySecs)

	entry := s.getOrCreateEntry(instance, mode)
	entry.mu.Lock()
	ensureStatsMaps(&entry.data)
	upstreamStats := entry.data.Upstreams[upstream]
	if upstreamStats.URL == "" {
		upstreamStats.URL = upstream
	}
	if upstreamStats.Status == nil {
		upstreamStats.Status = map[string]uint64{}
	}
	upstreamStats.ErrorRate = errorRate
	upstreamStats.LatencySeconds = latencySecs
	entry.data.Upstreams[upstream] = upstreamStats
	entry.mu.Unlock()
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
		UpstreamBytes:    s.totalUpstreamBytes.Load(),
		ActiveDownloads:  s.totalActiveDown.Load(),
		ActiveUpstreams:  s.totalActiveUp.Load(),
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
	total.Maintenance = cloneMap(s.totalMaintenance)
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

func (s *Stats) RemoveInstance(name string) {
	if s == nil {
		return
	}
	s.instances.Delete(name)
	label := prometheus.Labels{"instance": name}
	s.metrics.requestsTotal.DeletePartialMatch(label)
	s.metrics.responseBytesTotal.DeletePartialMatch(label)
	s.metrics.upstreamRequestsTotal.DeletePartialMatch(label)
	s.metrics.activeDownloads.DeletePartialMatch(label)
	s.metrics.metadataRefreshTotal.DeletePartialMatch(label)
	s.metrics.metadataRefreshTime.DeletePartialMatch(label)
	s.metrics.metadataSnapshotReady.DeletePartialMatch(label)
	s.metrics.repositoryMaintenance.DeletePartialMatch(label)
	s.metrics.repositoryObjects.DeletePartialMatch(label)
	s.metrics.upstreamErrorRate.DeletePartialMatch(label)
	s.metrics.upstreamLatency.DeletePartialMatch(label)
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

func cloneInstanceStats(item InstanceStats) InstanceStats {
	clone := item
	clone.Cache = cloneMap(item.Cache)
	clone.UpstreamStatus = cloneMap(item.UpstreamStatus)
	clone.Upstreams = cloneUpstreams(item.Upstreams)
	clone.Maintenance = cloneMap(item.Maintenance)
	return clone
}

func emptyInstanceStats(mode string) InstanceStats {
	return InstanceStats{
		Mode:           mode,
		Cache:          map[string]uint64{},
		UpstreamStatus: map[string]uint64{},
		Upstreams:      map[string]UpstreamStats{},
		Maintenance:    map[string]uint64{},
	}
}

func ensureStatsMaps(item *InstanceStats) {
	if item.Cache == nil {
		item.Cache = map[string]uint64{}
	}
	if item.UpstreamStatus == nil {
		item.UpstreamStatus = map[string]uint64{}
	}
	if item.Upstreams == nil {
		item.Upstreams = map[string]UpstreamStats{}
	}
	if item.Maintenance == nil {
		item.Maintenance = map[string]uint64{}
	}
}

func cloneUpstreams(src map[string]UpstreamStats) map[string]UpstreamStats {
	if len(src) == 0 {
		return map[string]UpstreamStats{}
	}
	dst := make(map[string]UpstreamStats, len(src))
	for key, value := range src {
		value.Status = cloneMap(value.Status)
		dst[key] = value
	}
	return dst
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
