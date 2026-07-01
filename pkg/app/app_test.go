package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func TestValidateRejectsConflictingPaths(t *testing.T) {
	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "one", "/files", "https://example.com", file.Policy{}),
		fileInstance(t, "two", "/files", "https://example.org", file.Policy{}),
	})
	err := Validate(doc)
	require.ErrorContains(t, err, "listen path /files conflicts")
}

func TestValidateRejectsInvalidName(t *testing.T) {
	_, err := config.Decode(strings.NewReader(`
server:
  bind: 127.0.0.1:8080
  backend: /tmp/cache
instances:
  - name: my proxy
    enabled: true
    file:
      route:
        path: /files
      upstreams:
        - https://example.com
`))
	require.ErrorContains(t, err, "invalid instance name")
}

func TestFileProxyCachesImmutableObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var upstreamRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		_, _ = io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{
			DefaultPolicy: config.PolicyImmutable,
			BusyPolicy:    config.BusyPolicyBypass,
		}),
	})
	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/a.txt"))
	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/a.txt"))
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestMetricsRequireBearerToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), nil)
	doc.Metrics.Token = "secret"

	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHomePageRendersConfiguredInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{
			DefaultPolicy: config.PolicyImmutable,
			BusyPolicy:    config.BusyPolicyStale,
			Rules: []file.Rule{
				{Match: "**/*.tgz", Policy: config.PolicyRevalidate},
			},
		}),
	})
	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "proxy.example.test"
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "Cache Proxy")
	require.Contains(t, body, "files")
	require.Contains(t, body, "http://proxy.example.test/files")
	require.Contains(t, body, `class="badge badge-file"`)
	require.Contains(t, body, "copyToClipboard")
}

func TestHomePageUsesPublicURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})
	doc.Server.PublicURL = "https://cache.home.lan"
	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Contains(t, body, "https://cache.home.lan/files")
	require.NotContains(t, body, "http://127.0.0.1")
}

func TestHomePageShowsBindDisplayURL(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "registry",
		Mode:    "oci",
		Enabled: true,
		Bind:    "127.0.0.1:5000",
		Home: proxyruntime.HomeEntry{
			Name:       "registry",
			Mode:       "oci",
			DisplayURL: "https://cache.home.lan:5000",
		},
	}

	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		entries:      map[string]*proxyruntime.Entry{"registry": entry},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
	}
	app.ready.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Contains(t, body, "https://cache.home.lan:5000")
	require.Contains(t, body, "docker pull")
	require.Contains(t, body, `class="badge badge-oci"`)
}

func TestHomePageReturns503WhenNotReady(t *testing.T) {
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
	}

	req := httptest.NewRequest(http.MethodGet, "/files/test.txt", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
}

func TestSetupCommandGeneration(t *testing.T) {
	tests := []struct {
		mode        string
		url         string
		noteContain string
		cmdContain  string
	}{
		{"npm", "http://cache/npm", "npm registry", "npm config set registry http://cache/npm"},
		{"go", "http://cache/go", "Go module proxy", "go env -w GOPROXY=http://cache/go"},
		{"maven", "http://cache/maven", "{mirror_id}", "http://cache/maven</url>"},
		{"cargo", "http://cache/cargo", "{source_name}", "sparse+http://cache/cargo/"},
		{"pypi", "http://cache/pypi", "{package}", "pip install --index-url http://cache/pypi/simple {package}"},
		{"oci", "http://cache:5000", "{image}", "docker pull http://cache:5000/{image}:{tag}"},
		{"apk", "http://cache/apk", "{alpine_branch}", "http://cache/apk/{alpine_branch}/{repository}"},
		{"deb", "http://cache/deb", "{distribution}", "deb http://cache/deb {distribution} {component}"},
		{"rpm", "http://cache/rpm", "{releasever}", "baseurl=http://cache/rpm/{releasever}/{repository}/{arch}"},
		{"pacman", "http://cache/pacman", "{repo}", "Server = http://cache/pacman/{repo}/os/{arch}"},
		{"file", "http://cache/files", "Base URL", "http://cache/files"},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			note, cmd := setupCommand(tt.mode, tt.url)
			require.Contains(t, note, tt.noteContain)
			require.Contains(t, cmd, tt.cmdContain)
		})
	}
}

func TestValidateRejectsUnknownModeField(t *testing.T) {
	doc, err := config.Decode(strings.NewReader(`
server:
  bind: 127.0.0.1:8080
  backend: /tmp/cache
instances:
  - name: files
    enabled: true
    file:
      expire_after: 720h
      route:
        path: /files
      upstreams:
        - https://example.com
      default_polciy: immutable
`))
	require.NoError(t, err)

	err = Validate(doc)
	require.Error(t, err)
	require.ErrorContains(t, err, "field default_polciy not found")
}

func TestAppCloseRespectsContextWhenHandlerStopBlocks(t *testing.T) {
	app := &App{
		stopRuntime: func() {},
		handlers: []proxyruntime.Instance{
			blockingInstance{},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := app.Close(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPrepareHandlersUsesPerInstanceContext(t *testing.T) {
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var startedCtx context.Context
	entry := &proxyruntime.Entry{
		Name:    "test",
		Mode:    config.ModeFile,
		Enabled: true,
		Path:    "/files",
		Runtime: startContextInstance{
			onStart: func(ctx context.Context) error {
				startedCtx = ctx
				return nil
			},
		},
	}
	app := &App{
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		entries: map[string]*proxyruntime.Entry{
			"test": entry,
		},
	}

	require.NoError(t, app.prepareHandlers(lifecycleCtx))
	require.NotNil(t, startedCtx, "Start should have been called")
	require.NotNil(t, entry.Ctx, "per-instance context should be set")
	require.NotNil(t, entry.Cancel, "per-instance cancel should be set")
	entry.Cancel()
	select {
	case <-startedCtx.Done():
	default:
		t.Fatal("per-instance context should be cancelled by entry.Cancel()")
	}
}

func TestPrepareHandlersCancelsStartedInstancesOnLaterStartFailure(t *testing.T) {
	started := &cleanupContextInstance{}
	failed := startContextInstance{
		onStart: func(context.Context) error {
			return fmt.Errorf("boom")
		},
	}
	app := &App{
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		entries: map[string]*proxyruntime.Entry{
			"a-started": {
				Name:    "a-started",
				Mode:    config.ModeFile,
				Enabled: true,
				Path:    "/started",
				Runtime: started,
			},
			"z-failed": {
				Name:    "z-failed",
				Mode:    config.ModeFile,
				Enabled: true,
				Path:    "/failed",
				Runtime: failed,
			},
		},
	}

	err := app.prepareHandlers(context.Background())
	require.ErrorContains(t, err, "boom")
	require.True(t, started.stopped.Load())
	require.ErrorIs(t, started.ctx.Err(), context.Canceled)
}

func TestPrepareHandlersWrapsBindHomePage(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "registry",
		Mode:    config.ModeOCI,
		Enabled: true,
		Bind:    "127.0.0.1:5000",
		Runtime: proxyruntime.HandlerInstance{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				_, _ = io.WriteString(w, req.URL.Path)
			}),
		},
		Home: proxyruntime.HomeEntry{
			Name: "registry",
			Mode: config.ModeOCI,
		},
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		entries: map[string]*proxyruntime.Entry{
			"registry": entry,
		},
	}
	app.ready.Store(true)

	require.NoError(t, app.prepareHandlers(context.Background()))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "proxy.example.test"
	rec := httptest.NewRecorder()
	app.bindHandlers[entry.Bind].ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "registry")
	require.NotContains(t, rec.Body.String(), "<section class=\"toolbar\">")

	req = httptest.NewRequest(http.MethodGet, "/v2/", nil)
	rec = httptest.NewRecorder()
	app.bindHandlers[entry.Bind].ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "/v2/", rec.Body.String())
}

func TestBindHomePageHeadReturnsOK(t *testing.T) {
	entry := &proxyruntime.Entry{
		Name:    "registry",
		Mode:    config.ModeOCI,
		Enabled: true,
		Bind:    "127.0.0.1:5000",
		Runtime: proxyruntime.HandlerInstance{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				_, _ = io.WriteString(w, "proxy")
			}),
		},
		Home: proxyruntime.HomeEntry{
			Name: "registry",
			Mode: config.ModeOCI,
		},
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Bind: "127.0.0.1:0"},
			Metrics: config.MetricsConfig{Path: "/metrics"},
		},
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		entries: map[string]*proxyruntime.Entry{
			"registry": entry,
		},
	}
	app.ready.Store(true)

	require.NoError(t, app.prepareHandlers(context.Background()))

	req := httptest.NewRequest(http.MethodHead, "/", nil)
	rec := httptest.NewRecorder()
	app.bindHandlers[entry.Bind].ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Body.String())
	require.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
}

func TestOpenStopsSchedulerWhenPrepareHandlersFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var runs atomic.Int32
	prev := driverSet
	driverSet = func() map[string]proxyruntime.ModeDriver {
		drivers := prev()
		drivers[config.ModeFile] = startFailingDriver{runs: &runs}
		return drivers
	}
	defer func() { driverSet = prev }()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})

	_, err := Open(ctx, doc, "")
	require.ErrorContains(t, err, "boom")
	first := runs.Load()
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, first, runs.Load())
}

func TestOpenPassesCleanupConfigIntoPlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})
	doc.Storage.Cleanup = config.CleanupConfig{
		DryRun:    true,
		BatchSize: 17,
	}

	var got config.CleanupConfig
	prev := driverSet
	driverSet = func() map[string]proxyruntime.ModeDriver {
		drivers := prev()
		drivers[config.ModeFile] = cleanupConfigDriver{seen: &got}
		return drivers
	}
	defer func() { driverSet = prev }()

	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	require.Equal(t, doc.Storage.Cleanup, got)
}

func TestStatusEndpointsReturnJSON(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app := openApp(t, ctx, testDocument(t.TempDir(), nil))
	defer closeApp(t, app)

	app.status.observeTaskRun(scheduler.TaskRun{
		Key:        scheduler.NewTaskKey("files", scheduler.TypeExpireCleanup, "/pool"),
		StartedAt:  time.Unix(1710000000, 0).UTC(),
		FinishedAt: time.Unix(1710000005, 0).UTC(),
		Duration:   5 * time.Second,
		Result:     "success",
	})

	for _, path := range []string{"/-/status/summary", "/-/status/disk", "/-/status/events?limit=1"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, path)
		require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
		require.NotEmpty(t, rec.Body.String())
	}
}

func TestStatusEventsEndpointClampsLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), nil)
	doc.Server.Status.EventLimit = 2
	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	app.status.observeTaskRun(scheduler.TaskRun{Key: scheduler.NewTaskKey("one", scheduler.TypeExpireCleanup, "/a"), StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Duration: time.Second, Result: "success"})
	app.status.observeTaskRun(scheduler.TaskRun{Key: scheduler.NewTaskKey("two", scheduler.TypeExpireCleanup, "/b"), StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Duration: time.Second, Result: "success"})
	app.status.observeTaskRun(scheduler.TaskRun{Key: scheduler.NewTaskKey("three", scheduler.TypeExpireCleanup, "/c"), StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Duration: time.Second, Result: "success"})

	req := httptest.NewRequest(http.MethodGet, "/-/status/events?limit=999", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"storage":"two"`)
	require.Contains(t, rec.Body.String(), `"storage":"three"`)
	require.NotContains(t, rec.Body.String(), `"storage":"one"`)
}

func TestStatusEndpointRejectsUnsupportedMethod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app := openApp(t, ctx, testDocument(t.TempDir(), nil))
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodPost, "/-/status/summary", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
}

func TestStatusEventsEndpointFallsBackOnInvalidLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), nil)
	doc.Server.Status.EventLimit = 2
	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	app.status.observeTaskRun(scheduler.TaskRun{Key: scheduler.NewTaskKey("one", scheduler.TypeExpireCleanup, "/a"), StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Duration: time.Second, Result: "success"})
	app.status.observeTaskRun(scheduler.TaskRun{Key: scheduler.NewTaskKey("two", scheduler.TypeExpireCleanup, "/b"), StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Duration: time.Second, Result: "success"})

	req := httptest.NewRequest(http.MethodGet, "/-/status/events?limit=nope", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"storage":"one"`)
	require.Contains(t, rec.Body.String(), `"storage":"two"`)
}

func TestValidateRejectsInvalidServerStatusWindow(t *testing.T) {
	doc := testDocument(t.TempDir(), nil)
	doc.Server.Status.DiskSampleInterval = config.Duration(30 * time.Minute)
	doc.Server.Status.DiskHistoryWindow = config.Duration(15 * time.Minute)

	err := Validate(doc)
	require.ErrorContains(t, err, "disk_history_window must be greater than or equal")
}

func TestStatusPersistsAndRestoresHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app := openApp(t, ctx, testDocument(t.TempDir(), nil))
	defer closeApp(t, app)

	app.status.observeTaskRun(scheduler.TaskRun{
		Key:        scheduler.NewTaskKey("files", scheduler.TypeExpireCleanup, "/pool"),
		StartedAt:  time.Unix(1710000000, 0).UTC(),
		FinishedAt: time.Unix(1710000004, 0).UTC(),
		Duration:   4 * time.Second,
		Result:     "success",
	})
	app.status.recordDiskUsage(ctx, app)
	app.status.persist()

	restored := newAppStatus(app.config.Server.Status, app.store)
	restored.restore()

	require.NotEmpty(t, restored.diskSamples())
	events := restored.taskEvents(app.config.Server.Status.EventLimit)
	require.Len(t, events, 1)
	require.Equal(t, "files", events[0].Storage)
	require.Equal(t, "expire_cleanup", events[0].TaskType)
}

func TestStatusCapturesUpstreamStateEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := bus.New()
	status := newAppStatus(config.ServerStatusConfig{
		DiskSampleInterval: config.Duration(time.Minute),
		DiskHistoryWindow:  config.Duration(time.Hour),
		EventLimit:         8,
	}, nil)
	status.start(ctx, &App{}, b)

	b.Publish(bus.Event{
		Type: bus.EventUpstreamState,
		Payload: bus.UpstreamStatePayload{
			Instance: "debian",
			Mode:     "deb",
			Upstream: "https://deb.example.com",
			From:     "closed",
			To:       "degraded",
			Reason:   "failure",
			Detail:   "HTTP 502",
		},
	})

	require.Eventually(t, func() bool {
		events := status.taskEvents(8)
		return len(events) == 1 && events[0].Message != ""
	}, time.Second, 20*time.Millisecond)

	events := status.taskEvents(8)
	require.Equal(t, "debian", events[0].Storage)
	require.Equal(t, "upstream_state", events[0].TaskType)
	require.Equal(t, "degraded", events[0].Result)
	require.Contains(t, events[0].Message, "failure")
	require.Contains(t, events[0].Message, "HTTP 502")
}

func openApp(t *testing.T, ctx context.Context, doc *config.Document) *App {
	t.Helper()
	app, err := Open(ctx, doc, "")
	require.NoError(t, err)
	app.ready.Store(true)
	return app
}

func testDocument(backend string, instances []config.Instance) *config.Document {
	return &config.Document{
		Server: config.ServerConfig{
			Bind:    "127.0.0.1:0",
			Backend: backend,
		},
		Metrics: config.MetricsConfig{
			Path: "/metrics",
		},
		Storage: config.StorageConfig{
			GC: config.GCConfig{
				Blob: config.Duration(time.Hour),
			},
			Cleanup: config.DefaultCleanupConfig(),
		},
		Instances: instances,
	}
}

func fileInstance(t *testing.T, name, path, upstream string, policy file.Policy) config.Instance {
	t.Helper()
	mode := map[string]any{
		"expire_after": config.Expiration(time.Hour),
		"route":        map[string]any{"path": path},
		"upstreams":    []string{upstream},
	}
	if len(policy.PassHeaders) > 0 {
		mode["pass_headers"] = policy.PassHeaders
	}
	if policy.DefaultPolicy != "" {
		mode["default_policy"] = policy.DefaultPolicy
	}
	if policy.FreshFor != 0 {
		mode["fresh_for"] = policy.FreshFor
	}
	if policy.BusyPolicy != "" {
		mode["busy_policy"] = policy.BusyPolicy
	}
	if policy.Rules != nil {
		mode["rules"] = policy.Rules
	}
	return config.Instance{
		Name:    name,
		Enabled: true,
		File:    &config.ModeBlock{Node: yamlNode(t, mode)},
	}
}

func yamlNode(t *testing.T, value any) *yaml.Node {
	t.Helper()
	data, err := yaml.Marshal(value)
	require.NoError(t, err)
	var doc yaml.Node
	require.NoError(t, yaml.Unmarshal(data, &doc))
	require.NotEmpty(t, doc.Content)
	return doc.Content[0]
}

func requestBody(t *testing.T, handler http.Handler, method, target string) string {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	return rec.Body.String()
}

func closeApp(t *testing.T, app *App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Close(ctx))
}

type blockingInstance struct{}

func (blockingInstance) ServeHTTP(http.ResponseWriter, *http.Request)        {}
func (blockingInstance) Start(context.Context) error                         { return nil }
func (blockingInstance) Cleanup(context.Context, config.CleanupConfig) error { return nil }
func (blockingInstance) Stop(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type startContextInstance struct {
	onStart func(context.Context) error
}

func (s startContextInstance) ServeHTTP(http.ResponseWriter, *http.Request)        {}
func (s startContextInstance) Cleanup(context.Context, config.CleanupConfig) error { return nil }
func (s startContextInstance) Stop(context.Context) error                          { return nil }
func (s startContextInstance) Start(ctx context.Context) error {
	if s.onStart != nil {
		return s.onStart(ctx)
	}
	return nil
}

type cleanupContextInstance struct {
	ctx     context.Context
	stopped atomic.Bool
}

func (s *cleanupContextInstance) ServeHTTP(http.ResponseWriter, *http.Request)        {}
func (s *cleanupContextInstance) Cleanup(context.Context, config.CleanupConfig) error { return nil }
func (s *cleanupContextInstance) Start(ctx context.Context) error {
	s.ctx = ctx
	return nil
}
func (s *cleanupContextInstance) Stop(context.Context) error {
	s.stopped.Store(true)
	return s.ctx.Err()
}

type startFailingDriver struct{ runs *atomic.Int32 }

func (startFailingDriver) Mode() string { return config.ModeFile }

func (d startFailingDriver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: 10 * time.Millisecond,
		Handler: func(context.Context) error {
			if d.runs != nil {
				d.runs.Add(1)
			}
			return nil
		},
	})
	return plan.BindPath("/files", config.Expiration(time.Hour), startContextInstance{
		onStart: func(context.Context) error {
			return fmt.Errorf("boom")
		},
	})
}

type cleanupConfigDriver struct{ seen *config.CleanupConfig }

func (cleanupConfigDriver) Mode() string { return config.ModeFile }

func (d cleanupConfigDriver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if d.seen != nil {
		*d.seen = plan.CleanupConfig()
	}
	return plan.BindPath("/files", config.Expiration(time.Hour), startContextInstance{})
}
