package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	httpproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	fileproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestHomePageShowsStatsAfterRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, fileproxy.Policy{}),
	})
	app, err := Open(ctx, doc)
	require.NoError(t, err)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/files/test.txt", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "card-stats")
	require.Contains(t, body, "1 req")
}

func TestHomePageWithEmptyStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{}),
	})
	app, err := Open(ctx, doc)
	require.NoError(t, err)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "0 req")
	require.Contains(t, body, "\u2014")
}

func TestHomePageWithNilStats(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "test",
		Mode:    "file",
		Enabled: true,
		Path:    "/files",
		Home: proxyruntime.HomeEntry{
			Name: "test",
			Mode: "file",
		},
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		entries:      map[string]*proxyruntime.Entry{"test": entry},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		stats:        nil,
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "0 req")
	require.Contains(t, body, "test")
}

func TestFormatHitRate(t *testing.T) {
	require.Equal(t, "\u2014", formatHitRate(map[string]uint64{}))

	cache := map[string]uint64{
		"HIT": 80, "FRESH": 10, "REFRESH": 5, "STALE": 1,
		"MISS": 3, "BYPASS": 1,
	}
	require.Equal(t, "96.0%", formatHitRate(cache))

	cache = map[string]uint64{"HIT": 0, "MISS": 10}
	require.Equal(t, "0.0%", formatHitRate(cache))

	cache = map[string]uint64{"HIT": 10, "MISS": 0}
	require.Equal(t, "100.0%", formatHitRate(cache))
}

func TestInstanceStatusNonIndexed(t *testing.T) {
	s := httpproxy.InstanceStats{UpstreamRequests: 100, UpstreamErrors: 0}
	color, label, extra := instanceStatus(s, i18nMaps["en"])
	require.Equal(t, "green", color)
	require.Equal(t, "upstream OK", label)
	require.Empty(t, extra)

	s = httpproxy.InstanceStats{UpstreamRequests: 100, UpstreamErrors: 10}
	color, label, extra = instanceStatus(s, i18nMaps["en"])
	require.Equal(t, "yellow", color)
	require.Equal(t, "10 err", label)
	require.Empty(t, extra)

	s = httpproxy.InstanceStats{}
	color, label, extra = instanceStatus(s, i18nMaps["en"])
	require.Empty(t, color)
	require.Equal(t, "\u2014", label)
	require.Empty(t, extra)
}

func TestInstanceStatusIndexed(t *testing.T) {
	now := time.Now()
	s := httpproxy.InstanceStats{
		MetadataState: "ready",
		LastRefreshAt: now.Add(-2 * time.Minute),
	}
	color, label, extra := instanceStatus(s, i18nMaps["en"])
	require.Equal(t, "green", color)
	require.Equal(t, "ready", label)
	require.NotEmpty(t, extra)

	s = httpproxy.InstanceStats{
		MetadataState: "refreshing",
		LastRefreshAt: now.Add(-30 * time.Second),
	}
	color, label, extra = instanceStatus(s, i18nMaps["en"])
	require.Equal(t, "blue", color)
	require.Equal(t, "refreshing", label)

	s = httpproxy.InstanceStats{MetadataState: "degraded"}
	color, label, extra = instanceStatus(s, i18nMaps["en"])
	require.Equal(t, "red", color)
	require.Equal(t, "degraded", label)
	require.Empty(t, extra)

	s = httpproxy.InstanceStats{MetadataState: "booting"}
	color, label, _ = instanceStatus(s, i18nMaps["en"])
	require.Equal(t, "gray", color)
	require.Equal(t, "loading", label)
}

func TestRelativeTime(t *testing.T) {
	require.Equal(t, "just now", relativeTime(500*time.Millisecond, i18nMaps["en"]))
	require.Equal(t, "30s ago", relativeTime(30*time.Second, i18nMaps["en"]))
	require.Equal(t, "5m ago", relativeTime(5*time.Minute, i18nMaps["en"]))
	require.Equal(t, "2h ago", relativeTime(2*time.Hour, i18nMaps["en"]))
}

func TestRelativeTimeZH(t *testing.T) {
	require.Equal(t, "刚刚", relativeTime(500*time.Millisecond, i18nMaps["zh"]))
	require.Equal(t, "30秒前", relativeTime(30*time.Second, i18nMaps["zh"]))
	require.Equal(t, "5分钟前", relativeTime(5*time.Minute, i18nMaps["zh"]))
	require.Equal(t, "2小时前", relativeTime(2*time.Hour, i18nMaps["zh"]))
}

func TestFormatCompact(t *testing.T) {
	require.Equal(t, "0", formatCompact(0))
	require.Equal(t, "999", formatCompact(999))
	require.Equal(t, "1.2k", formatCompact(1234))
	require.Equal(t, "150k", formatCompact(150000))
	require.Equal(t, "1.5M", formatCompact(1500000))
}

func TestStatsLastRefreshAtIsSet(t *testing.T) {
	reg := prometheus.NewRegistry()
	stats := httpproxy.NewStats(reg)

	stats.SetMetadataState("test", "apk", "booting", false)
	snap := stats.Snapshot()
	inst := snap.Instances["test"]
	require.Equal(t, "booting", inst.MetadataState)
	require.False(t, inst.LastRefreshAt.IsZero())

	stats.RecordMetadataRefresh("test", "apk", "success", time.Second, true)
	snap = stats.Snapshot()
	inst = snap.Instances["test"]
	require.Equal(t, "success", inst.LastRefresh)
	require.True(t, inst.SnapshotReady)
	require.False(t, inst.LastRefreshAt.IsZero())
}
