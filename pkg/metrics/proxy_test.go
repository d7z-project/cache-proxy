package metrics

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestUpstreamStatisticsNormalizeAndBoundOrigins(t *testing.T) {
	stats := NewStats(prometheus.NewRegistry())
	for i := range 100 {
		u := fmt.Sprintf("https://user:secret@EXAMPLE.test:443/packages/%d?token=secret#fragment", i)
		release := stats.BeginUpstreamRequest("files", "file", u)
		stats.RecordUpstreamRequest("files", "file", u, http.MethodGet, http.StatusOK, time.Millisecond, 1)
		release()
	}
	snapshot := stats.Snapshot()
	require.Len(t, snapshot.Instances["files"].Upstreams, 1)
	require.EqualValues(t, 100, snapshot.Instances["files"].Upstreams["https://example.test"].Requests)
	var releases []func()
	for i := range 200 {
		u := fmt.Sprintf("https://cdn%d.example/download", i)
		releases = append(releases, stats.BeginUpstreamRequest("files", "file", u))
		stats.RecordUpstreamRequest("files", "file", u, http.MethodGet, http.StatusOK, time.Millisecond, 1)
	}
	for _, release := range releases {
		release()
		release()
	}
	snapshot = stats.Snapshot()
	require.Len(t, snapshot.Instances["files"].Upstreams, maxUpstreamOrigins+1)
	require.Positive(t, snapshot.Instances["files"].Upstreams["other"].Requests)
	require.EqualValues(t, 300, snapshot.Total.UpstreamRequests)
	require.Zero(t, snapshot.Total.ActiveUpstreams)
	for _, upstream := range snapshot.Instances["files"].Upstreams {
		require.Zero(t, upstream.ActiveRequests)
	}
	for raw, expected := range map[string]string{
		"http://EXAMPLE.test:80/x":    "http://example.test",
		"http://[::1]:80/x":           "http://[::1]",
		"https://[::1]:8443/x":        "https://[::1]:8443",
		"https://example.test:8443/x": "https://example.test:8443",
	} {
		require.Equal(t, expected, upstreamOrigin(raw))
	}
}

func TestRequestMetricsBoundMethodLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	stats := NewStats(registry)
	for i := range 100 {
		stats.RecordRequest("files", "file", fmt.Sprintf("CUSTOM%d", i), "REJECTED", 405, 0)
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		if family.GetName() == "cache_proxy_requests_total" {
			found = true
			require.Len(t, family.Metric, 1)
			require.Equal(t, float64(100), family.Metric[0].GetCounter().GetValue())
			method := ""
			for _, label := range family.Metric[0].Label {
				if label.GetName() == "method" {
					method = label.GetValue()
				}
			}
			require.Equal(t, "OTHER", method)
		}
	}
	require.True(t, found, "request counter was not collected")
}

func FuzzUpstreamOrigin(f *testing.F) {
	f.Add("https://user:password@EXAMPLE.test:443/package?token=secret")
	f.Add("http://[::1]:8080/path")
	f.Add("http://[fe80::1%25eth0]:80/path")
	f.Add("invalid")
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 4096 {
			t.Skip()
		}
		origin := upstreamOrigin(raw)
		require.Equal(t, origin, upstreamOrigin(origin))
		if origin == "other" {
			return
		}
		u, err := url.Parse(origin)
		require.NoError(t, err)
		require.Contains(t, []string{"http", "https"}, u.Scheme)
		require.Nil(t, u.User)
		require.Empty(t, u.Path)
		require.Empty(t, u.RawQuery)
		require.Empty(t, u.Fragment)
	})
}

func TestSnapshotOwnsNestedCounters(t *testing.T) {
	stats := NewStats(prometheus.NewRegistry())
	const upstream = "https://upstream.example"
	stats.RecordRequest("files", "file", http.MethodGet, "HIT", http.StatusOK, 10)
	stats.RecordUpstreamRequest("files", "file", upstream, http.MethodGet, http.StatusOK, time.Millisecond, 10)
	first := stats.Snapshot()
	first.Total.Cache["HIT"] = 99
	first.Total.UpstreamStatus["200"] = 99
	instance := first.Instances["files"]
	instance.Cache["HIT"] = 99
	instance.UpstreamStatus["200"] = 99
	instance.Upstreams[upstream].Status["200"] = 99
	delete(instance.Upstreams, upstream)

	current := stats.Snapshot()
	require.Equal(t, uint64(1), current.Total.Cache["HIT"])
	require.Equal(t, uint64(1), current.Total.UpstreamStatus["200"])
	require.Equal(t, uint64(1), current.Instances["files"].Cache["HIT"])
	require.Equal(t, uint64(1), current.Instances["files"].UpstreamStatus["200"])
	require.Equal(t, uint64(1), current.Instances["files"].Upstreams[upstream].Status["200"])
}

func TestConcurrentRecordingAndSnapshots(t *testing.T) {
	stats := NewStats(prometheus.NewRegistry())
	const upstream = "https://upstream.example"
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 30 {
				release := stats.BeginUpstreamRequest("files", "file", upstream)
				stats.RecordRequest("files", "file", http.MethodGet, "MISS", http.StatusOK, 10)
				stats.RecordUpstreamRequest("files", "file", upstream, http.MethodGet, http.StatusOK, time.Millisecond, 10)
				snapshot := stats.Snapshot()
				delete(snapshot.Instances["files"].Upstreams[upstream].Status, "200")
				release()
				release()
			}
		})
	}
	workers.Wait()
	snapshot := stats.Snapshot()
	require.Equal(t, uint64(240), snapshot.Total.Requests)
	require.Equal(t, uint64(240), snapshot.Total.UpstreamRequests)
	require.Equal(t, uint64(240), snapshot.Instances["files"].Upstreams[upstream].Status["200"])
	require.Zero(t, snapshot.Total.ActiveUpstreams)
	require.Zero(t, snapshot.Instances["files"].ActiveUpstreams)
	require.Zero(t, snapshot.Instances["files"].Upstreams[upstream].ActiveRequests)
}
