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
	"gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type repositoryRuntime struct {
	repositories []proxyruntime.RepositoryStatus
}

func (repositoryRuntime) ServeHTTP(http.ResponseWriter, *http.Request)        {}
func (repositoryRuntime) Start(context.Context) error                         { return nil }
func (repositoryRuntime) Stop(context.Context) error                          { return nil }
func (repositoryRuntime) Cleanup(context.Context, config.CleanupConfig) error { return nil }
func (r repositoryRuntime) RepositoryStatuses() []proxyruntime.RepositoryStatus {
	return append([]proxyruntime.RepositoryStatus(nil), r.repositories...)
}

func TestHomePageShowsStatsAfterRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
	})
	app := openApp(t, ctx, doc)
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
	require.Contains(t, body, "Requests")
	require.Contains(t, body, "<div class=\"stat-val\">1</div>")
}

func TestHomePageShowsZeroStatsWhenSnapshotEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})
	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "<div class=\"stat-val\">0</div>")
	require.Contains(t, body, "\u2014")
}

func TestHomePageHandlesNilStatsSource(t *testing.T) {
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
	app.ready.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "<div class=\"stat-val\">0</div>")
	require.Contains(t, body, "test")
}

func TestBindHomePageShowsSingleInstanceView(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "registry",
		Mode:    "oci",
		Enabled: true,
		Bind:    "127.0.0.1:5000",
		Home: proxyruntime.HomeEntry{
			Name: "registry",
			Mode: "oci",
		},
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		entries: map[string]*proxyruntime.Entry{
			"registry": entry,
		},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
	}
	app.ready.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "proxy.example.test"
	rec := httptest.NewRecorder()
	app.serveBindHome(rec, req, entry)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "single-hero")
	require.Contains(t, body, "registry")
	require.Contains(t, body, "http://proxy.example.test:5000")
	require.NotContains(t, body, "<section class=\"toolbar\">")
	require.NotContains(t, body, `id="status-btn"`)
	require.NotContains(t, body, `id="status-modal"`)
}

func TestHomePageRepositoryGenerationUsesReadableLabel(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "packages",
		Mode:    "pacman",
		Enabled: true,
		Path:    "/pacman",
		Runtime: repositoryRuntime{
			repositories: []proxyruntime.RepositoryStatus{{
				ID:            "core/os/x86_64",
				Path:          "core/os/x86_64",
				DisplayName:   "core/os/x86_64",
				Layout:        "pacman",
				Generation:    "djmsm2bdzupd",
				HasCurrent:    true,
				State:         "active",
				ArtifactCount: 12,
			}},
		},
		Home: proxyruntime.HomeEntry{
			Name: "packages",
			Mode: "pacman",
		},
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		entries:      map[string]*proxyruntime.Entry{"packages": entry},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
	}
	app.ready.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/?lang=zh", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "代次 djmsm2bdzupd")
	require.Contains(t, body, "title=\"代次 djmsm2bdzupd\"")
	require.Contains(t, body, "根路径")
	require.Contains(t, body, "Pacman 仓库")
}

func TestHomePageShowsFlatpakRepositoryLayout(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "flathub",
		Mode:    config.ModeFlatpak,
		Enabled: true,
		Path:    "/flathub",
		Runtime: repositoryRuntime{
			repositories: []proxyruntime.RepositoryStatus{{
				ID:              "/",
				Path:            "/",
				DisplayName:     "flathub",
				Layout:          "flatpak",
				PrimaryMetadata: []string{"summary"},
				Generation:      "djmsm2bdzupd",
				HasCurrent:      true,
				State:           "active",
				MetadataCount:   2,
			}},
		},
		Home: proxyruntime.HomeEntry{
			Name: "flathub",
			Mode: config.ModeFlatpak,
		},
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		entries:      map[string]*proxyruntime.Entry{"flathub": entry},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
	}
	app.ready.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, `class="badge badge-flatpak"`)
	require.Contains(t, body, "flatpak remote-add --if-not-exists")
	require.Contains(t, body, "Flatpak Repository")
	require.Contains(t, body, "summary")
}

func TestHomePageRendersStatusModalControls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app := openApp(t, ctx, testDocument(t.TempDir(), nil))
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/?lang=zh", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, `id="status-btn"`)
	require.Contains(t, body, `id="status-modal"`)
	require.Contains(t, body, `data-status-tab="disk"`)
	require.Contains(t, body, `data-status-tab="events"`)
	require.Contains(t, body, `id="refresh-badge"`)
	require.Contains(t, body, `id="status-modal-backdrop"`)
}

func TestHomePageDefaultsToHealthyStatusWithoutStoreStats(t *testing.T) {
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		entries:      map[string]*proxyruntime.Entry{},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		lifecycleCtx: context.Background(),
	}
	app.ready.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "healthy")
	require.NotContains(t, rec.Body.String(), "0 degraded")
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
	now := time.Now()
	s := httpcache.InstanceStats{UpstreamRequests: 100, UpstreamErrors: 0}
	color, label, extra, extraTitle := instanceStatus(s, i18nMaps["en"], now)
	require.Equal(t, "green", color)
	require.Equal(t, "Upstream OK", label)
	require.Empty(t, extra)
	require.Empty(t, extraTitle)

	s = httpcache.InstanceStats{UpstreamRequests: 100, UpstreamErrors: 10}
	color, label, extra, extraTitle = instanceStatus(s, i18nMaps["en"], now)
	require.Equal(t, "yellow", color)
	require.Equal(t, "10 err", label)
	require.Empty(t, extra)
	require.Empty(t, extraTitle)

	s = httpcache.InstanceStats{}
	color, label, extra, extraTitle = instanceStatus(s, i18nMaps["en"], now)
	require.Empty(t, color)
	require.Equal(t, "\u2014", label)
	require.Empty(t, extra)
	require.Empty(t, extraTitle)
}

func TestInstanceStatusIndexed(t *testing.T) {
	now := time.Now()
	s := httpcache.InstanceStats{
		MetadataState: "ready",
		LastRefreshAt: now.Add(-2 * time.Minute),
	}
	color, label, extra, extraTitle := instanceStatus(s, i18nMaps["en"], now)
	require.Equal(t, "green", color)
	require.Equal(t, "Ready", label)
	require.NotEmpty(t, extra)
	require.NotEmpty(t, extraTitle)

	s = httpcache.InstanceStats{
		MetadataState: "refreshing",
		LastRefreshAt: now.Add(-30 * time.Second),
	}
	color, label, extra, extraTitle = instanceStatus(s, i18nMaps["en"], now)
	require.Equal(t, "blue", color)
	require.Equal(t, "Refreshing", label)
	require.Equal(t, "30s ago", extra)
	require.NotEmpty(t, extraTitle)

	s = httpcache.InstanceStats{MetadataState: "degraded"}
	color, label, extra, extraTitle = instanceStatus(s, i18nMaps["en"], now)
	require.Equal(t, "yellow", color)
	require.Equal(t, "Degraded", label)
	require.Empty(t, extra)
	require.Empty(t, extraTitle)

	s = httpcache.InstanceStats{MetadataState: "booting"}
	color, label, _, _ = instanceStatus(s, i18nMaps["en"], now)
	require.Equal(t, "gray", color)
	require.Equal(t, "Loading", label)
}

func TestFormatRecentTime(t *testing.T) {
	now := time.Date(2026, 7, 2, 16, 4, 5, 0, time.Local)
	recent := now.Add(-30 * time.Minute)
	display, exact := formatRecentTime(recent, i18nMaps["en"], now)
	require.Equal(t, "30m ago", display)
	require.Equal(t, recent.Format(preciseTimeLayout), exact)

	old := now.Add(-48 * time.Hour)
	display, exact = formatRecentTime(old, i18nMaps["en"], now)
	require.Equal(t, old.Format(preciseTimeLayout), display)
	require.Equal(t, old.Format(preciseTimeLayout), exact)

	future := now.Add(2 * time.Hour)
	display, exact = formatRecentTime(future, i18nMaps["en"], now)
	require.Equal(t, future.Format(preciseTimeLayout), display)
	require.Equal(t, future.Format(preciseTimeLayout), exact)
}

func TestRelativeTime(t *testing.T) {
	require.Equal(t, "just now", relativeTime(500*time.Millisecond, i18nMaps["en"]))
	require.Equal(t, "30s ago", relativeTime(30*time.Second, i18nMaps["en"]))
	require.Equal(t, "5m ago", relativeTime(5*time.Minute, i18nMaps["en"]))
	require.Equal(t, "2h ago", relativeTime(2*time.Hour, i18nMaps["en"]))
}

func TestFormatCompact(t *testing.T) {
	require.Equal(t, "0", formatCompact(0))
	require.Equal(t, "999", formatCompact(999))
	require.Equal(t, "1.2k", formatCompact(1234))
	require.Equal(t, "150k", formatCompact(150000))
	require.Equal(t, "1.5M", formatCompact(1500000))
}

func TestStatsMetadataStateDoesNotOverwriteLastRefreshAt(t *testing.T) {
	reg := prometheus.NewRegistry()
	stats := httpcache.NewStats(reg)

	stats.SetMetadataState("test", "apk", "booting", false)
	snap := stats.Snapshot()
	inst := snap.Instances["test"]
	require.Equal(t, "booting", inst.MetadataState)
	require.False(t, inst.LastStateChangeAt.IsZero())
	require.True(t, inst.LastRefreshAt.IsZero())

	stats.RecordMetadataRefresh("test", "apk", "success", time.Second, true)
	snap = stats.Snapshot()
	inst = snap.Instances["test"]
	require.Equal(t, "success", inst.LastRefresh)
	require.True(t, inst.SnapshotReady)
	require.False(t, inst.LastRefreshAt.IsZero())
	require.False(t, inst.LastRefreshOKAt.IsZero())
}

func TestDetectLocale(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?lang=zh", nil)
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	require.Equal(t, "zh", detectLocale(req))

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.8")
	require.Equal(t, "zh", detectLocale(req))

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	require.Equal(t, "en", detectLocale(req))
}

func TestDetectTheme(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?theme=dark", nil)
	req.AddCookie(&http.Cookie{Name: "theme", Value: "light"})
	require.Equal(t, "dark", detectTheme(req))

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
	require.Equal(t, "dark", detectTheme(req))

	req = httptest.NewRequest(http.MethodGet, "/?theme=nope", nil)
	req.AddCookie(&http.Cookie{Name: "theme", Value: "oops"})
	require.Equal(t, "light", detectTheme(req))
}

func TestI18NMapsShareSameKeys(t *testing.T) {
	en := i18nMaps["en"]
	zh := i18nMaps["zh"]
	require.NotEmpty(t, en)
	require.NotEmpty(t, zh)

	for key := range en {
		_, ok := zh[key]
		require.Truef(t, ok, "missing zh key %q", key)
	}
	for key := range zh {
		_, ok := en[key]
		require.Truef(t, ok, "missing en key %q", key)
	}
	for _, key := range []string{
		"result_updated",
		"result_unchanged",
		"reason_published",
		"reason_same_as_current",
	} {
		require.NotEmpty(t, en[key])
		require.NotEmpty(t, zh[key])
	}
}

func TestBindURLUsesForwardedHostAndProto(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "127.0.0.1:8080"
	req.Header.Set("X-Forwarded-Host", "cache.example.test")
	req.Header.Set("X-Forwarded-Proto", "https")
	require.Equal(t, "https://cache.example.test:5000", bindURL(req, "0.0.0.0:5000"))
}

func TestBuildHomeRepositoryUsesFallbackLabels(t *testing.T) {
	now := time.Date(2026, 7, 2, 16, 4, 5, 0, time.Local)
	repository := buildHomeRepository(proxyruntime.RepositoryStatus{
		ID:            "core/os/x86_64",
		Path:          "core/os/x86_64",
		DisplayName:   "core/os/x86_64",
		Layout:        "pacman",
		ArtifactCount: 42,
		Upstream:      "https://mirror.example.test/root",
	}, i18nMaps["en"], now)
	require.Equal(t, "Generation None", repository.Generation)
	require.Equal(t, "Booting", repository.StateLabel)
	require.Equal(t, "mirror.example.test", repository.Upstream)
	require.Equal(t, "Pacman Repository", repository.Layout)

	repository = buildHomeRepository(proxyruntime.RepositoryStatus{
		ID:          "deb_flat:/",
		DisplayName: "/",
		Layout:      "deb_flat",
		HasCurrent:  true,
	}, i18nMaps["en"], now)
	require.Equal(t, "Pending", repository.StateLabel)
	require.Equal(t, "/", repository.Path)
	require.Equal(t, "Debian Flat", repository.Layout)
}
