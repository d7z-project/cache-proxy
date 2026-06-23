package httpcache

import (
	"strconv"
	"sync"
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
	}
	reg.MustRegister(mc.requestsTotal, mc.responseBytesTotal, mc.upstreamRequestsTotal, mc.activeDownloads, mc.metadataRefreshTotal, mc.metadataRefreshTime, mc.metadataSnapshotReady)
	return mc
}

type Stats struct {
	mu        sync.RWMutex
	total     InstanceStats
	instances map[string]InstanceStats
	mc        *metricsCollector
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
		total:     emptyInstanceStats(""),
		instances: map[string]InstanceStats{},
		mc:        newMetricsCollector(reg),
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

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.instance(instance, mode)
	item.Requests++
	item.ResponseBytes += bytes
	item.Cache[cache]++
	if status >= 500 {
		item.Errors++
	}
	s.total.Requests++
	s.total.ResponseBytes += bytes
	s.total.Cache[cache]++
	if status >= 500 {
		s.total.Errors++
	}
	s.instances[instance] = item
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

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.instance(instance, mode)
	item.UpstreamRequests++
	item.UpstreamStatus[statusText]++
	if status == 0 || status >= 500 {
		item.UpstreamErrors++
	}
	s.total.UpstreamRequests++
	s.total.UpstreamStatus[statusText]++
	if status == 0 || status >= 500 {
		s.total.UpstreamErrors++
	}
	s.instances[instance] = item
}

func (s *Stats) AddActiveDownload(instance, mode string, delta int64) {
	if s == nil {
		return
	}
	s.mc.activeDownloads.WithLabelValues(instance, mode).Add(float64(delta))
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.instance(instance, mode)
	item.ActiveDownloads += delta
	s.total.ActiveDownloads += delta
	s.instances[instance] = item
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
	if ready {
		s.mc.metadataSnapshotReady.WithLabelValues(instance, mode).Set(1)
	} else {
		s.mc.metadataSnapshotReady.WithLabelValues(instance, mode).Set(0)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.instance(instance, mode)
	item.Refreshes++
	item.LastRefresh = result
	item.LastRefreshAt = time.Now()
	item.SnapshotReady = ready
	if result != "success" {
		item.RefreshFailures++
	}
	s.total.Refreshes++
	s.total.LastRefresh = result
	if result != "success" {
		s.total.RefreshFailures++
	}
	s.instances[instance] = item
}

func (s *Stats) SetMetadataState(instance, mode, state string, ready bool) {
	if s == nil {
		return
	}
	if ready {
		s.mc.metadataSnapshotReady.WithLabelValues(instance, mode).Set(1)
	} else {
		s.mc.metadataSnapshotReady.WithLabelValues(instance, mode).Set(0)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.instance(instance, mode)
	item.MetadataState = state
	item.LastRefreshAt = time.Now()
	item.SnapshotReady = ready
	s.total.MetadataState = state
	s.total.SnapshotReady = ready
	s.instances[instance] = item
}

func (s *Stats) Snapshot() StatsSnapshot {
	if s == nil {
		return StatsSnapshot{Total: emptyInstanceStats(""), Instances: map[string]InstanceStats{}}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := StatsSnapshot{Total: cloneInstanceStats(s.total), Instances: map[string]InstanceStats{}}
	for name, item := range s.instances {
		snapshot.Instances[name] = cloneInstanceStats(item)
	}
	return snapshot
}

func (s *Stats) instance(name, mode string) InstanceStats {
	item, ok := s.instances[name]
	if !ok {
		item = emptyInstanceStats(mode)
	}
	if item.Mode == "" {
		item.Mode = mode
	}
	return item
}

func emptyInstanceStats(mode string) InstanceStats {
	return InstanceStats{Mode: mode, Cache: map[string]uint64{}, UpstreamStatus: map[string]uint64{}}
}

func cloneInstanceStats(item InstanceStats) InstanceStats {
	clone := item
	clone.Cache = map[string]uint64{}
	for key, value := range item.Cache {
		clone.Cache[key] = value
	}
	clone.UpstreamStatus = map[string]uint64{}
	for key, value := range item.UpstreamStatus {
		clone.UpstreamStatus[key] = value
	}
	return clone
}
