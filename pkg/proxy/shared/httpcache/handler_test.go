package httpcache

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestHeaderName(t *testing.T) {
	tests := map[string]string{
		"content-type":          "Content-Type",
		"content-length":        "Content-Length",
		"last-modified":         "Last-Modified",
		"etag":                  "ETag",
		"docker-content-digest": "Docker-Content-Digest",
		"unknown-key":           "unknown-key",
	}
	for input, expected := range tests {
		require.Equal(t, expected, headerName(input), "headerName(%q)", input)
	}
}

func TestStripInternal(t *testing.T) {
	headers := map[string]string{
		"Content-Type":      "application/json",
		"Content-Length":    "123",
		"ETag":              "\"abc\"",
		"fetched-at":        "2024-01-01T00:00:00Z",
		"mode":              "test",
		"cache":             "HIT",
		"indexed-identity":  "sha256:xyz",
		"X-Cache":           "HIT",
	}
	StripInternal(headers)
	require.Equal(t, "application/json", headers["Content-Type"])
	require.Equal(t, "123", headers["Content-Length"])
	require.Equal(t, "\"abc\"", headers["ETag"])
	require.Equal(t, "HIT", headers["X-Cache"])
	require.NotContains(t, headers, "fetched-at")
	require.NotContains(t, headers, "mode")
	require.NotContains(t, headers, "cache")
	require.NotContains(t, headers, "indexed-identity")
}

func TestCacheDebugHeadersOnCacheHit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "26")
		w.Header().Set("ETag", "\"etag-v1\"")
		w.Header().Set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("01234567890123456789012345"))
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	resolver := &staticResolver{route: Route{
		ObjectPath:   "test/object",
		UpstreamPath: "test/object",
		Policy:       config.PolicyImmutable,
		FreshFor:     config.Freshness(time.Hour),
	}}
	handler := NewHandler("test", RuntimeConfig{
		Mode:            "test",
		ExpireAfter:     config.Expiration(72 * time.Hour),
		Upstreams:       []string{upstream.URL},
		DefaultFreshFor: config.Freshness(time.Minute),
	}, store, resolver, NewStats(prometheus.NewRegistry()))

	// First request: cache miss, fetches from upstream
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/object", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "MISS", rec.Header().Get("X-Cache"))
	require.Equal(t, config.PolicyImmutable, rec.Header().Get("X-Cache-Policy"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Fetched-At"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Expires-At"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Fresh-Until"))
	require.Empty(t, rec.Header().Get("fetched-at"))
	require.Empty(t, rec.Header().Get("mode"))
	require.Empty(t, rec.Header().Get("cache"))

	// Second request: cache hit
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/object", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "HIT", rec.Header().Get("X-Cache"))
	require.Equal(t, config.PolicyImmutable, rec.Header().Get("X-Cache-Policy"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Fetched-At"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Expires-At"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Fresh-Until"))
	require.Empty(t, rec.Header().Get("fetched-at"))
	require.Empty(t, rec.Header().Get("mode"))
}

func TestCacheDebugHeadersOnRevalidateFresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "26")
		w.Header().Set("ETag", "\"etag-v1\"")
		w.Header().Set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("01234567890123456789012345"))
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	resolver := &staticResolver{route: Route{
		ObjectPath:   "test/revalidate",
		UpstreamPath: "test/revalidate",
		Policy:       config.PolicyRevalidate,
		FreshFor:     config.Freshness(time.Hour),
	}}
	handler := NewHandler("test", RuntimeConfig{
		Mode:            "test",
		ExpireAfter:     config.Expiration(72 * time.Hour),
		Upstreams:       []string{upstream.URL},
		DefaultFreshFor: config.Freshness(time.Minute),
	}, store, resolver, NewStats(prometheus.NewRegistry()))

	// First request: cache miss
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/revalidate", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "MISS", rec.Header().Get("X-Cache"))

	// Second request: within FreshFor, returns FRESH
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/revalidate", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "FRESH", rec.Header().Get("X-Cache"))
	require.Equal(t, config.PolicyRevalidate, rec.Header().Get("X-Cache-Policy"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Fetched-At"))
	require.NotEmpty(t, rec.Header().Get("X-Cache-Fresh-Until"))
	require.Empty(t, rec.Header().Get("fetched-at"))
}

func TestCacheDebugHeadersOnBypass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "26")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("01234567890123456789012345"))
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	resolver := &staticResolver{route: Route{
		ObjectPath:   "test/bypass",
		UpstreamPath: "test/bypass",
		Policy:       config.PolicyBypass,
	}}
	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{upstream.URL},
	}, store, resolver, NewStats(prometheus.NewRegistry()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/bypass", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	require.Empty(t, rec.Header().Get("fetched-at"))
	require.Empty(t, rec.Header().Get("X-Cache-Fetched-At"), "bypass should not have cache debug headers")
}

func TestPassthroughStripsInternalHeaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("passthrough"))
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	resolver := &staticResolver{route: Route{
		ObjectPath:   "test/object",
		UpstreamPath: "test/object",
		Policy:       config.PolicyBypass,
	}}
	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{upstream.URL},
	}, store, resolver, NewStats(prometheus.NewRegistry()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/object", nil)
	handler.ProxyPassthrough(rec, req, "other")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	require.Empty(t, rec.Header().Get("fetched-at"))
}

func TestHeadRequestStripsInternalHeaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "26")
		w.Header().Set("ETag", "\"etag-v1\"")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("01234567890123456789012345"))
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	resolver := &staticResolver{route: Route{
		ObjectPath:   "test/head-obj",
		UpstreamPath: "test/head-obj",
		Policy:       config.PolicyImmutable,
		FreshFor:     config.Freshness(time.Hour),
	}}
	handler := NewHandler("test", RuntimeConfig{
		Mode:            "test",
		ExpireAfter:     config.Expiration(72 * time.Hour),
		Upstreams:       []string{upstream.URL},
		DefaultFreshFor: config.Freshness(time.Minute),
	}, store, resolver, NewStats(prometheus.NewRegistry()))

	// First: cache the object
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/head-obj", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Second: HEAD request on cached object
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(ctx, http.MethodHead, "/test/head-obj", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "HIT", rec.Header().Get("X-Cache"))
	require.Empty(t, rec.Header().Get("fetched-at"))
	require.Empty(t, rec.Header().Get("mode"))
}

// --- helpers ---

type staticResolver struct {
	route Route
}

func (r *staticResolver) Resolve(req *http.Request) (Route, error) {
	clean := strings.TrimPrefix(req.URL.Path, "/")
	return Route{
		ObjectPath:   r.route.ObjectPath,
		UpstreamPath: clean,
		TargetURL:    r.route.TargetURL,
		Policy:       r.route.Policy,
		FreshFor:     r.route.FreshFor,
		BusyPolicy:   r.route.BusyPolicy,
		ExpireAfter:  r.route.ExpireAfter,
	}, nil
}

func TestErrorResponseHidesInternalDetails(t *testing.T) {
	resp := ErrorResponse(http.StatusBadGateway, errors.New("sensitive data"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "internal error", string(body))
	require.Equal(t, "ERROR", resp.Headers["X-Cache"])
}

func TestSafePath(t *testing.T) {
	require.True(t, SafePath("foo/bar/baz"))
	require.True(t, SafePath("pypi/files/something.tgz"))
	require.False(t, SafePath("../etc/passwd"))
	require.False(t, SafePath("foo/../../bar"))
	require.False(t, SafePath("/absolute/path"))
	require.False(t, SafePath("."))
	require.False(t, SafePath(".."))
}
