package app

import (
	"net/url"
	"sort"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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
	DegradedUpstreams      int     `json:"degraded_upstreams"`
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
	State                  string  `json:"state"`
	Weight                 float64 `json:"weight"`
	ErrorRate              float64 `json:"error_rate"`
	LatencyMS              float64 `json:"latency_ms"`
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
	State                  string  `json:"state"`
	Weight                 float64 `json:"weight"`
	ErrorRate              float64 `json:"error_rate"`
	LatencyMS              float64 `json:"latency_ms"`
	LastStatus             string  `json:"last_status,omitempty"`
	LastUsedAt             string  `json:"last_used_at,omitempty"`
	LastError              string  `json:"last_error,omitempty"`
}

func (s *appStatus) network(app *App) networkStatus {
	snapshot := httpcache.StatsSnapshot{Instances: map[string]httpcache.InstanceStats{}}
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
			HitRate:                hitRate(snapshot.Total.Cache),
			UpstreamErrorRate:      errorRate(snapshot.Total.UpstreamRequests, snapshot.Total.UpstreamErrors),
		},
	}

	upstreamNodes := map[string]*networkUpstream{}
	for _, entry := range sortedEntries(app.entries) {
		if !entry.Enabled {
			continue
		}
		stats := snapshot.Instances[entry.Name]
		instance := networkInstance{
			ID:                     "instance:" + entry.Name,
			Name:                   entry.Name,
			Mode:                   entry.Mode,
			Route:                  entryRoute(entry),
			Requests:               stats.Requests,
			ResponseBytes:          stats.ResponseBytes,
			UpstreamRequests:       stats.UpstreamRequests,
			UpstreamErrors:         stats.UpstreamErrors,
			ActiveDownloads:        stats.ActiveDownloads,
			ActiveUpstreamRequests: stats.ActiveUpstreams,
			HitRate:                hitRate(stats.Cache),
		}
		status.Instances = append(status.Instances, instance)
		for upstreamURL, upstream := range stats.Upstreams {
			status.addNetworkEdge(instance, entry, upstreamNodes, upstreamURL, upstream)
		}
	}
	for _, key := range sortedNetworkKeys(upstreamNodes) {
		upstream := *upstreamNodes[key]
		if upstream.State != "closed" && upstream.State != "unknown" {
			status.Summary.DegradedUpstreams++
		}
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
	upstream httpcache.UpstreamStats,
) {
	if upstreamURL == "" {
		return
	}
	host := upstreamHost(upstreamURL)
	upstreamID := "upstream:" + host
	state := upstream.StateText
	if state == "" {
		state = "unknown"
	}
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
		State:                  state,
		Weight:                 upstream.Weight,
		ErrorRate:              upstream.ErrorRate,
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
		node = &networkUpstream{ID: upstreamID, Host: host, State: state}
		upstreamNodes[upstreamID] = node
	}
	node.Requests += upstream.Requests
	node.Errors += upstream.Errors
	node.ResponseBytes += upstream.ResponseBytes
	node.ActiveUpstreamRequests += upstream.ActiveRequests
	if networkStateRank(state) > networkStateRank(node.State) {
		node.State = state
	}
	if upstream.Weight > node.Weight {
		node.Weight = upstream.Weight
	}
	if upstream.ErrorRate > node.ErrorRate {
		node.ErrorRate = upstream.ErrorRate
	}
	if latencyMS := upstream.LatencySeconds * 1000; latencyMS > node.LatencyMS {
		node.LatencyMS = latencyMS
	}
}

func sortedNetworkKeys(items map[string]*networkUpstream) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func entryRoute(entry *proxyruntime.Entry) string {
	if entry.Path != "" {
		return entry.Path
	}
	if entry.Bind != "" {
		return entry.Bind
	}
	return "/"
}

func upstreamHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return rawURL
	}
	return strings.ToLower(parsed.Host)
}

func hitRate(cache map[string]uint64) float64 {
	if len(cache) == 0 {
		return 0
	}
	var total uint64
	var hits uint64
	for key, value := range cache {
		total += value
		switch strings.ToUpper(key) {
		case "HIT", "STALE", "GENERATION":
			hits += value
		}
	}
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

func errorRate(requests, errors uint64) float64 {
	if requests == 0 {
		return 0
	}
	return float64(errors) / float64(requests)
}

func networkStateRank(state string) int {
	switch state {
	case "open":
		return 4
	case "halfopen":
		return 3
	case "degraded":
		return 2
	case "closed":
		return 1
	default:
		return 0
	}
}
