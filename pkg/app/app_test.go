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
)

func TestValidateRejectsConflictingPaths(t *testing.T) {
	doc := testDocument(t.TempDir(), []config.Instance{
		fileInstance(t, "one", "/files", "https://example.com", fileproxy.Policy{Rules: []fileproxy.Rule{}}),
		fileInstance(t, "two", "/files", "https://example.org", fileproxy.Policy{Rules: []fileproxy.Rule{}}),
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
			Rules:         []fileproxy.Rule{},
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
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{Rules: []fileproxy.Rule{}}),
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

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "cache-proxy")
	require.Contains(t, rec.Body.String(), "files")
	require.Contains(t, rec.Body.String(), "http://proxy.example.test/files")
	require.Contains(t, rec.Body.String(), "default_policy: immutable")
	require.Contains(t, rec.Body.String(), "busy_policy: stale")
	require.Contains(t, rec.Body.String(), "policy: revalidate")
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
      rules: []
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
		fileInstance(t, "files", "/files", "https://example.com", fileproxy.Policy{Rules: []fileproxy.Rule{}}),
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
