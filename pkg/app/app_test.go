package app

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	fileproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestValidateRejectsConflictingPaths(t *testing.T) {
	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "one", "/files", "https://example.com", fileproxy.Policy{}),
		fileInstance(t, "two", "/files", "https://example.org", fileproxy.Policy{}),
	})
	err := Validate(doc)
	require.ErrorContains(t, err, "listen path /files conflicts")
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
		fileInstance(t, "files", "/files", upstream.URL, fileproxy.Policy{
			DefaultPolicy: config.PolicyImmutable,
			BusyPolicy:    config.BusyPolicyBypass,
		}),
	})
	app, err := Open(ctx, doc)
	require.NoError(t, err)
	defer closeApp(t, app)

	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/a.txt"))
	require.Equal(t, "hello", requestBody(t, app, http.MethodGet, "/files/a.txt"))
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestAppCleanupRemovesExpiredObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{}),
	})
	doc.Storage.Cleanup.Enabled = true

	app, err := Open(ctx, doc)
	require.NoError(t, err)
	defer closeApp(t, app)

	now := time.Now()
	_, err = app.store.Put(ctx, "files", "expired.txt", strings.NewReader("expired"), map[string]string{
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

	app, err := Open(ctx, doc)
	require.NoError(t, err)
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
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{
			DefaultPolicy: config.PolicyImmutable,
			BusyPolicy:    config.BusyPolicyStale,
			Rules: []fileproxy.Rule{
				{Match: "**/*.tgz", Policy: config.PolicyRevalidate},
			},
		}),
	})
	app, err := Open(ctx, doc)
	require.NoError(t, err)
	defer closeApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "proxy.example.test"
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, body, "cache-proxy")
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
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{}),
	})
	doc.Server.PublicURL = "https://cache.home.lan"
	app, err := Open(ctx, doc)
	require.NoError(t, err)
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

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	require.Contains(t, body, "https://cache.home.lan:5000")
	require.Contains(t, body, "docker pull")
	require.Contains(t, body, `class="badge badge-oci"`)
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
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{}),
	})
	doc.Storage.Cleanup.Enabled = true

	app, err := Open(ctx, doc)
	require.NoError(t, err)
	defer closeApp(t, app)

	now := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	_, err = app.store.Put(ctx, "files", "expired.txt", strings.NewReader("expired"), map[string]string{
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
		gcDone:      closedChan(),
		cleanupDone: closedChan(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := app.Close(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPrepareHandlersUsesLifecycleContext(t *testing.T) {
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := atomic.Bool{}
	entry := &proxyruntime.Entry{
		Name:    "test",
		Mode:    config.ModeFile,
		Enabled: true,
		Path:    "/files",
		Runtime: startContextInstance{
			onStart: func(ctx context.Context) error {
				started.Store(ctx == lifecycleCtx)
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
	require.True(t, started.Load())
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

	require.NoError(t, app.prepareHandlers(context.Background()))

	req := httptest.NewRequest(http.MethodHead, "/", nil)
	rec := httptest.NewRecorder()
	app.bindHandlers[entry.Bind].ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Body.String())
	require.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
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

func fileInstance(t *testing.T, name, path, upstream string, policy fileproxy.Policy) config.Instance {
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

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
