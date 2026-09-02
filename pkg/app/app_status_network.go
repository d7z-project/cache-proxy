package app

import (
	"net/url"
	"sort"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type networkStatus struct {
	GeneratedAt string            `json:"generated_at"`
	Summary     networkSummary    `json:"summary"`
	Instances   []networkInstance `json:"instances"`
	Upstreams   []networkUpstream `json:"upstreams"`
	Edges       []networkEdge     `json:"edges"`
}

type networkSummary struct {
	Requests               uint64  `json:"requests"`
	ResponseBytes          uint64  `json:"response_bytes"`
	UpstreamRequests       uint64  `json:"upstream_requests"`
	UpstreamErrors         uint64  `json:"upstream_errors"`
	UpstreamBytes          uint64  `json:"upstream_bytes"`
	ActiveDownloads        int64   `json:"active_downloads"`
	ActiveUpstreamRequests int64   `json:"active_upstream_requests"`
	HitRate                float64 `json:"hit_rate"`
	UpstreamErrorRate      float64 `json:"upstream_error_rate"`
	QueuedUpstreamRequests int     `json:"queued_upstream_requests"`
	ForegroundQueued       int     `json:"foreground_queued"`
	RefreshQueued          int     `json:"refresh_queued"`
	OldestAdmissionWaitMS  int64   `json:"oldest_admission_wait_ms"`
	AdmissionActive        int     `json:"admission_active"`
	AdmissionMaxActive     int     `json:"admission_max_active"`
	AdmissionMinIntervalMS int64   `json:"admission_min_interval_ms"`
	RateLimitedUpstreams   int     `json:"rate_limited_upstreams"`
}

type networkInstance struct {
	ID                     string  `json:"id"`
	Name                   string  `json:"name"`
	Mode                   string  `json:"mode"`
	Route                  string  `json:"route"`
	Requests               uint64  `json:"requests"`
	ResponseBytes          uint64  `json:"response_bytes"`
	UpstreamRequests       uint64  `json:"upstream_requests"`
	UpstreamErrors         uint64  `json:"upstream_errors"`
	ActiveDownloads        int64   `json:"active_downloads"`
	ActiveUpstreamRequests int64   `json:"active_upstream_requests"`
	HitRate                float64 `json:"hit_rate"`
}

type networkUpstream struct {
	ID                     string  `json:"id"`
	Host                   string  `json:"host"`
	Requests               uint64  `json:"requests"`
	Errors                 uint64  `json:"errors"`
	ResponseBytes          uint64  `json:"response_bytes"`
	ActiveUpstreamRequests int64   `json:"active_upstream_requests"`
	ErrorRate              float64 `json:"error_rate"`
	LatencyMS              float64 `json:"latency_ms"`
	AdmissionActive        int     `json:"admission_active"`
	AdmissionQueued        int     `json:"admission_queued"`
	AdmissionMaxActive     int     `json:"admission_max_active"`
	AdmissionMinIntervalMS int64   `json:"admission_min_interval_ms"`
	CooldownUntil          string  `json:"cooldown_until,omitempty"`
}

type networkEdge struct {
	ID                     string  `json:"id"`
	From                   string  `json:"from"`
	To                     string  `json:"to"`
	Instance               string  `json:"instance"`
	Mode                   string  `json:"mode"`
	UpstreamURL            string  `json:"upstream_url"`
	UpstreamHost           string  `json:"upstream_host"`
	Requests               uint64  `json:"requests"`
	Errors                 uint64  `json:"errors"`
	ResponseBytes          uint64  `json:"response_bytes"`
	ActiveUpstreamRequests int64   `json:"active_upstream_requests"`
	ErrorRate              float64 `json:"error_rate"`
	LatencyMS              float64 `json:"latency_ms"`
	LastStatus             string  `json:"last_status,omitempty"`
	LastUsedAt             string  `json:"last_used_at,omitempty"`
	LastError              string  `json:"last_error,omitempty"`
}

func (s *appStatus) network(app *App) networkStatus {
	snapshot := metrics.StatsSnapshot{Instances: map[string]metrics.InstanceStats{}}
	if app.stats != nil {
		snapshot = app.stats.Snapshot()
	}
	status := networkStatus{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Summary: networkSummary{
			Requests:               snapshot.Total.Requests,
			ResponseBytes:          snapshot.Total.ResponseBytes,
			UpstreamRequests:       snapshot.Total.UpstreamRequests,
			UpstreamErrors:         snapshot.Total.UpstreamErrors,
			UpstreamBytes:          snapshot.Total.UpstreamBytes,
			ActiveDownloads:        snapshot.Total.ActiveDownloads,
			ActiveUpstreamRequests: snapshot.Total.ActiveUpstreams,
			HitRate:                networkHitRate(snapshot.Total.Cache),
			UpstreamErrorRate:      errorRate(snapshot.Total.UpstreamRequests, snapshot.Total.UpstreamErrors),
		},
	}

	upstreamNodes := map[string]*networkUpstream{}
	for _, entry := range sortedEntries(app.entries) {
		if !entry.Enabled {
			continue
		}
		stats := snapshot.Instances[entry.Name]
		route := entry.Path
		if route == "" {
			route = entry.Bind
		}
		if route == "" {
			route = "/"
		}
		instance := networkInstance{
			ID:                     "instance:" + entry.Name,
			Name:                   entry.Name,
			Mode:                   entry.Mode,
			Route:                  route,
			Requests:               stats.Requests,
			ResponseBytes:          stats.ResponseBytes,
			UpstreamRequests:       stats.UpstreamRequests,
			UpstreamErrors:         stats.UpstreamErrors,
			ActiveDownloads:        stats.ActiveDownloads,
			ActiveUpstreamRequests: stats.ActiveUpstreams,
			HitRate:                networkHitRate(stats.Cache),
		}
		status.Instances = append(status.Instances, instance)
		for upstreamURL, upstream := range stats.Upstreams {
			status.addNetworkEdge(instance, entry, upstreamNodes, upstreamURL, upstream)
		}
	}
	admission := app.upstreamGate.Snapshot()
	status.Summary.QueuedUpstreamRequests = admission.Queued
	status.Summary.ForegroundQueued = admission.ForegroundQueued
	status.Summary.RefreshQueued = admission.RefreshQueued
	status.Summary.OldestAdmissionWaitMS = admission.OldestWait.Milliseconds()
	status.Summary.AdmissionActive = admission.Active
	status.Summary.AdmissionMaxActive = admission.MaxActive
	status.Summary.AdmissionMinIntervalMS = admission.MinInterval.Milliseconds()
	for _, node := range upstreamNodes {
		node.AdmissionMaxActive = admission.MaxActivePerHost
		node.AdmissionMinIntervalMS = admission.MinInterval.Milliseconds()
	}
	for host, hostAdmission := range admission.Hosts {
		key := "upstream:" + host
		node := upstreamNodes[key]
		if node == nil {
			node = &networkUpstream{ID: key, Host: host}
			upstreamNodes[key] = node
		}
		node.AdmissionActive = hostAdmission.Active
		node.AdmissionQueued = hostAdmission.Queued
		node.AdmissionMaxActive = hostAdmission.MaxActive
		node.AdmissionMinIntervalMS = hostAdmission.MinInterval.Milliseconds()
		if !hostAdmission.CooldownUntil.IsZero() {
			node.CooldownUntil = hostAdmission.CooldownUntil.Format(time.RFC3339)
			status.Summary.RateLimitedUpstreams++
		}
	}
	upstreamKeys := make([]string, 0, len(upstreamNodes))
	for key := range upstreamNodes {
		upstreamKeys = append(upstreamKeys, key)
	}
	sort.Strings(upstreamKeys)
	for _, key := range upstreamKeys {
		upstream := *upstreamNodes[key]
		status.Upstreams = append(status.Upstreams, upstream)
	}
	sort.Slice(status.Edges, func(i, j int) bool {
		if status.Edges[i].Instance == status.Edges[j].Instance {
			return status.Edges[i].UpstreamURL < status.Edges[j].UpstreamURL
		}
		return status.Edges[i].Instance < status.Edges[j].Instance
	})
	return status
}

func (s *networkStatus) addNetworkEdge(
	instance networkInstance,
	entry *proxyruntime.Entry,
	upstreamNodes map[string]*networkUpstream,
	upstreamURL string,
	upstream metrics.UpstreamStats,
) {
	if upstreamURL == "" {
		return
	}
	parsed, err := url.Parse(upstreamURL)
	host := upstreamURL
	if err == nil && parsed.Host != "" {
		host = strings.ToLower(parsed.Host)
	}
	upstreamID := "upstream:" + host
	upstreamErrorRate := errorRate(upstream.Requests, upstream.Errors)
	edge := networkEdge{
		ID:                     instance.ID + "->" + upstreamID + ":" + upstreamURL,
		From:                   instance.ID,
		To:                     upstreamID,
		Instance:               entry.Name,
		Mode:                   entry.Mode,
		UpstreamURL:            upstreamURL,
		UpstreamHost:           host,
		Requests:               upstream.Requests,
		Errors:                 upstream.Errors,
		ResponseBytes:          upstream.ResponseBytes,
		ActiveUpstreamRequests: upstream.ActiveRequests,
		ErrorRate:              upstreamErrorRate,
		LatencyMS:              upstream.LatencySeconds * 1000,
		LastStatus:             upstream.LastStatus,
		LastError:              upstream.LastError,
	}
	if !upstream.LastUsedAt.IsZero() {
		edge.LastUsedAt = upstream.LastUsedAt.Format(time.RFC3339)
	}
	s.Edges = append(s.Edges, edge)

	node := upstreamNodes[upstreamID]
	if node == nil {
		node = &networkUpstream{ID: upstreamID, Host: host}
		upstreamNodes[upstreamID] = node
	}
	node.Requests += upstream.Requests
	node.Errors += upstream.Errors
	node.ResponseBytes += upstream.ResponseBytes
	node.ActiveUpstreamRequests += upstream.ActiveRequests
	if upstreamErrorRate > node.ErrorRate {
		node.ErrorRate = upstreamErrorRate
	}
	if latencyMS := upstream.LatencySeconds * 1000; latencyMS > node.LatencyMS {
		node.LatencyMS = latencyMS
	}
}

func networkHitRate(cache map[string]uint64) float64 {
	rate, ok := cacheHitRate(cache)
	if !ok {
		return 0
	}
	return rate
}

func errorRate(requests, errors uint64) float64 {
	if requests == 0 {
		return 0
	}
	return float64(errors) / float64(requests)
}
