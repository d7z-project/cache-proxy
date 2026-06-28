package app

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
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

func TestAppCleanupRemovesExpiredObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})
	doc.Storage.Cleanup.Enabled = true

	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	now := time.Now()
	_, err := app.store.Put(ctx, "files", "expired.txt", strings.NewReader("expired"), map[string]string{
		"fetched-at": now.Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	_, err = app.store.Put(ctx, "files", "fresh.txt", strings.NewReader("fresh"), map[string]string{
		"fetched-at": now.Add(-10 * time.Minute).UTC().Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	_, err = app.store.Put(ctx, "files", "missing.txt", strings.NewReader("missing"), map[string]string{})
	require.NoError(t, err)

	app.runCleanup(context.Background())

	_, err = app.store.OpenObject(ctx, "files", "expired.txt")
	require.ErrorIs(t, err, fs.ErrNotExist)
	_, err = app.store.OpenObject(ctx, "files", "missing.txt")
	require.ErrorIs(t, err, fs.ErrNotExist)
	_, err = app.store.OpenObject(ctx, "files", "fresh.txt")
	require.NoError(t, err)
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
	require.Contains(t, body, "copyURL")
	require.Contains(t, body, "copyCode")
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

func TestAppCleanupHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})
	doc.Storage.Cleanup.Enabled = true

	app := openApp(t, ctx, doc)
	defer closeApp(t, app)

	now := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	_, err := app.store.Put(ctx, "files", "expired.txt", strings.NewReader("expired"), map[string]string{
		"fetched-at": now,
	})
	require.NoError(t, err)

	cleanupCtx, stopCleanup := context.WithCancel(context.Background())
	stopCleanup()
	app.runCleanup(cleanupCtx)

	_, err = app.store.OpenObject(ctx, "files", "expired.txt")
	require.NoError(t, err)
}

func TestAppCloseRespectsContextWhenHandlerStopBlocks(t *testing.T) {
	app := &App{
		stopRuntime: func() {},
		handlers: []proxyruntime.Instance{
			blockingInstance{},
		},
		gcDone:      makeClosedChan(),
		cleanupDone: makeClosedChan(),
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

func TestDiffInstances(t *testing.T) {
	old := []config.Instance{
		{Name: "keep", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/keep"}, "upstreams": []string{"https://a.com"}})}},
		{Name: "remove", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/remove"}, "upstreams": []string{"https://b.com"}})}},
		{Name: "change", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/old"}, "upstreams": []string{"https://c.com"}})}},
	}
	new_ := []config.Instance{
		{Name: "keep", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/keep"}, "upstreams": []string{"https://a.com"}})}},
		{Name: "change", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/new"}, "upstreams": []string{"https://c.com"}})}},
		{Name: "added", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/added"}, "upstreams": []string{"https://d.com"}})}},
	}
	added, removed, modified := config.DiffInstances(old, new_)
	require.ElementsMatch(t, []string{"remove"}, removed)
	require.ElementsMatch(t, []string{"added"}, added)
	require.ElementsMatch(t, []string{"change"}, modified)
}

func TestDiffInstancesModeSwitch(t *testing.T) {
	old := []config.Instance{
		{Name: "proxy", Enabled: true, File: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/files"}, "upstreams": []string{"https://a.com"}})}},
	}
	new_ := []config.Instance{
		{Name: "proxy", Enabled: true, NPM: &config.ModeBlock{Node: yamlNode(t, map[string]any{"route": map[string]any{"path": "/npm"}, "upstreams": []string{"https://b.com"}})}},
	}
	added, removed, modified := config.DiffInstances(old, new_)
	require.Empty(t, added)
	require.Empty(t, removed)
	require.ElementsMatch(t, []string{"proxy"}, modified)
}

func TestReloadInvalidConfigPreservesRunningInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dir := t.TempDir()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	doc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
	})
	doc.Storage.Cleanup.Enabled = true

	cfgPath := filepath.Join(dir, "config.yaml")
	writeYAML(t, cfgPath, doc)
	app := openAppWithConfig(t, ctx, doc, cfgPath)
	defer closeApp(t, app)

	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/test.txt"))

	badDoc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
		fileInstance(t, "dup", "/files", "https://other.com", file.Policy{}),
	})
	writeYAML(t, cfgPath, badDoc)
	err := app.Reload(ctx)
	require.Error(t, err)

	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/test.txt"))
}

func TestReloadPrepareFailurePreservesRunningInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dir := t.TempDir()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	doc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
	})
	cfgPath := filepath.Join(dir, "config.yaml")
	writeYAML(t, cfgPath, doc)
	app := openAppWithConfig(t, ctx, doc, cfgPath)
	defer closeApp(t, app)

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()

	newDoc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
		ociInstance(t, "registry", occupied.Addr().String(), "https://registry-1.docker.io"),
	})
	writeYAML(t, cfgPath, newDoc)
	err = app.Reload(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "listen "+occupied.Addr().String())

	require.True(t, app.ready.Load())
	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/test.txt"))
}

func TestReloadClosedReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dir := t.TempDir()
	doc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", file.Policy{}),
	})
	cfgPath := filepath.Join(dir, "config.yaml")
	writeYAML(t, cfgPath, doc)
	app := openAppWithConfig(t, ctx, doc, cfgPath)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := app.Close(closeCtx)
	closeCancel()
	require.NoError(t, err)

	err = app.Reload(ctx)
	require.ErrorContains(t, err, "app is closed")
}

func TestReloadConcurrentServeHTTPNoRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	dir := t.TempDir()
	doc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
	})
	cfgPath := filepath.Join(dir, "config.yaml")
	writeYAML(t, cfgPath, doc)
	app := openAppWithConfig(t, ctx, doc, cfgPath)
	defer closeApp(t, app)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			req := httptest.NewRequest(http.MethodGet, "/files/test.txt", nil)
			rec := httptest.NewRecorder()
			app.ServeHTTP(rec, req)
		}
	}()

	newDoc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
		fileInstance(t, "newf", "/newf", upstream.URL, file.Policy{}),
	})
	writeYAML(t, cfgPath, newDoc)
	err := app.Reload(ctx)
	require.NoError(t, err)
	<-done
}

func TestMultipleReloads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	dir := t.TempDir()
	doc := testDocument(dir, []config.Instance{
		fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
	})
	cfgPath := filepath.Join(dir, "config.yaml")
	writeYAML(t, cfgPath, doc)
	app := openAppWithConfig(t, ctx, doc, cfgPath)
	defer closeApp(t, app)

	for i := 0; i < 3; i++ {
		newDoc := testDocument(dir, []config.Instance{
			fileInstance(t, "files", "/files", upstream.URL, file.Policy{}),
		})
		writeYAML(t, cfgPath, newDoc)
		err := app.Reload(ctx)
		require.NoError(t, err)
	}

	require.Equal(t, "ok", requestBody(t, app, http.MethodGet, "/files/test.txt"))
}

func TestReloadAddsAndRemovesInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	dir := t.TempDir()
	doc := testDocument(dir, []config.Instance{
		fileInstance(t, "one", "/one", upstream.URL, file.Policy{}),
	})
	cfgPath := filepath.Join(dir, "config.yaml")
	writeYAML(t, cfgPath, doc)
	app := openAppWithConfig(t, ctx, doc, cfgPath)
	defer closeApp(t, app)

	require.Equal(t, "ok", requestBody(t, app, http.MethodGet, "/one/test.txt"))

	newDoc := testDocument(dir, []config.Instance{
		fileInstance(t, "two", "/two", upstream.URL, file.Policy{}),
	})
	writeYAML(t, cfgPath, newDoc)
	err := app.Reload(ctx)
	require.NoError(t, err)

	require.Equal(t, "ok", requestBody(t, app, http.MethodGet, "/two/test.txt"))

	req := httptest.NewRequest(http.MethodGet, "/one/test.txt", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func openApp(t *testing.T, ctx context.Context, doc *config.Document) *App {
	return openAppWithConfig(t, ctx, doc, "")
}

func openAppWithConfig(t *testing.T, ctx context.Context, doc *config.Document, configPath string) *App {
	t.Helper()
	app, err := Open(ctx, doc, configPath)
	require.NoError(t, err)
	app.ready.Store(true)
	return app
}

func writeYAML(t *testing.T, path string, doc *config.Document) {
	t.Helper()
	var b strings.Builder
	b.WriteString("server:\n")
	b.WriteString("  bind: " + doc.Server.Bind + "\n")
	b.WriteString("  backend: " + doc.Server.Backend + "\n")
	if doc.Server.PublicURL != "" {
		b.WriteString("  public_url: " + doc.Server.PublicURL + "\n")
	}
	b.WriteString("metrics:\n")
	b.WriteString("  path: " + doc.Metrics.Path + "\n")
	b.WriteString("storage:\n")
	b.WriteString("  gc:\n")
	b.WriteString("    blob: 1h\n")
	b.WriteString("  cleanup:\n")
	b.WriteString("    enabled: " + fmt.Sprintf("%v", doc.Storage.Cleanup.Enabled) + "\n")
	b.WriteString("    interval: 6h\n")
	b.WriteString("instances:\n")
	for _, inst := range doc.Instances {
		sel, err := inst.SelectMode()
		require.NoError(t, err)
		b.WriteString("  - name: " + inst.Name + "\n")
		b.WriteString("    enabled: " + fmt.Sprintf("%v", inst.Enabled) + "\n")
		b.WriteString("    " + sel.Mode + ":\n")
		if sel.Block != nil {
			data, err := yaml.Marshal(sel.Block.Node)
			require.NoError(t, err)
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				if line != "" {
					b.WriteString("      " + line + "\n")
				}
			}
		}
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
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

func ociInstance(t *testing.T, name, bind, upstream string) config.Instance {
	t.Helper()
	return config.Instance{
		Name:    name,
		Enabled: true,
		OCI: &config.ModeBlock{Node: yamlNode(t, map[string]any{
			"bind":     bind,
			"upstream": upstream,
		})},
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

func (blockingInstance) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (blockingInstance) Start(context.Context) error                  { return nil }
func (blockingInstance) Cleanup(context.Context) error                { return nil }
func (blockingInstance) Stop(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type startContextInstance struct {
	onStart func(context.Context) error
}

func (s startContextInstance) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (s startContextInstance) Cleanup(context.Context) error                { return nil }
func (s startContextInstance) Stop(context.Context) error                   { return nil }
func (s startContextInstance) Start(ctx context.Context) error {
	if s.onStart != nil {
		return s.onStart(ctx)
	}
	return nil
}

func makeClosedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
