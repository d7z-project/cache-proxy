package httpcache

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
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
		require.Equal(t, expected, HeaderName(input), "HeaderName(%q)", input)
	}
}

func TestStripInternal(t *testing.T) {
	headers := map[string]string{
		"Content-Type":              "application/json",
		"Content-Length":            "123",
		"ETag":                      "\"abc\"",
		"fetched-at":                "2024-01-01T00:00:00Z",
		"mode":                      "test",
		"cache":                     "HIT",
		"indexed":                   "true",
		"indexed-identity":          "sha256:xyz",
		"indexed-root":              "root",
		"indexed-generation":        "7",
		"indexed-upstream":          "https://upstream.example",
		"indexed-digest-algorithm":  "sha256",
		"indexed-digest":            "abc",
		"indexed-digest-verifiable": "true",
		"X-Cache":                   "HIT",
	}
	StripInternal(headers)
	require.Equal(t, "application/json", headers["Content-Type"])
	require.Equal(t, "123", headers["Content-Length"])
	require.Equal(t, "\"abc\"", headers["ETag"])
	require.Equal(t, "HIT", headers["X-Cache"])
	require.NotContains(t, headers, "fetched-at")
	require.NotContains(t, headers, "mode")
	require.NotContains(t, headers, "cache")
	require.NotContains(t, headers, "indexed")
	require.NotContains(t, headers, "indexed-identity")
	require.NotContains(t, headers, "indexed-root")
	require.NotContains(t, headers, "indexed-generation")
	require.NotContains(t, headers, "indexed-upstream")
	require.NotContains(t, headers, "indexed-digest-algorithm")
	require.NotContains(t, headers, "indexed-digest")
	require.NotContains(t, headers, "indexed-digest-verifiable")
}

func TestConfigureClientTransportTimeouts(t *testing.T) {
	client := utils.DefaultHttpClientWrapper()
	ConfigureClientTransport(client, "test", "test", &config.TransportConfig{
		DialTimeout:        config.Duration(2 * time.Second),
		HeaderTimeout:      config.Duration(3 * time.Second),
		IdleBodyTimeout:    config.Duration(4 * time.Second),
		MaxRequestDuration: config.Duration(5 * time.Second),
		MaxIdleConns:       7,
		MaxConnsPerHost:    8,
	})

	require.Equal(t, 5*time.Second, client.Timeout)
	require.Equal(t, 4*time.Second, client.IdleBodyTimeout)
	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 3*time.Second, transport.ResponseHeaderTimeout)
	require.Equal(t, 7, transport.MaxIdleConns)
	require.Equal(t, 8, transport.MaxConnsPerHost)
	require.NotNil(t, transport.DialContext)
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
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

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
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

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
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

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
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/object", nil)
	handler.ProxyPassthrough(rec, req, "other", "")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "PASSTHROUGH", rec.Header().Get("X-Cache"))
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
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

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
		ObjectPath:         r.route.ObjectPath,
		UpstreamPath:       clean,
		TargetURL:          r.route.TargetURL,
		AllowedTargetHosts: append([]string(nil), r.route.AllowedTargetHosts...),
		Policy:             r.route.Policy,
		FreshFor:           r.route.FreshFor,
		BusyPolicy:         r.route.BusyPolicy,
		ExpireAfter:        r.route.ExpireAfter,
	}, nil
}

func TestTargetURLRejectsForeignHostWithoutFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var allowedRequests int
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "foreign")
	}))
	defer foreign.Close()
	allowed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowedRequests++
		_, _ = io.WriteString(w, "allowed")
	}))
	defer allowed.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{allowed.URL},
	}, store, &staticResolver{route: Route{
		ObjectPath: "test/object",
		TargetURL:  foreign.URL + "/object",
		Policy:     config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Zero(t, allowedRequests)
}

func TestTargetURLAllowsRouteHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "target")
	}))
	defer target.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{Mode: "test"}, store, &staticResolver{route: Route{
		ObjectPath:         "test/object",
		TargetURL:          target.URL + "/object",
		AllowedTargetHosts: []string{strings.TrimPrefix(target.URL, "http://")},
		Policy:             config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "target", rec.Body.String())
}

func TestTargetURLReturnsClientErrorWithoutFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var upstreamRequests int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer target.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		_, _ = io.WriteString(w, "upstream")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{Mode: "test", Upstreams: []string{upstream.URL}}, store, &staticResolver{route: Route{
		ObjectPath:         "test/object",
		TargetURL:          target.URL + "/object",
		AllowedTargetHosts: []string{strings.TrimPrefix(target.URL, "http://")},
		Policy:             config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Zero(t, upstreamRequests)
}

func TestTargetURLFallsBackOnRetryableStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "upstream")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{Mode: "test", Upstreams: []string{upstream.URL}}, store, &staticResolver{route: Route{
		ObjectPath:         "test/object",
		TargetURL:          target.URL + "/object",
		AllowedTargetHosts: []string{strings.TrimPrefix(target.URL, "http://")},
		Policy:             config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "upstream", rec.Body.String())
}

func TestFailoverRetriesRetryableStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer second.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{first.URL, second.URL},
	}, store, &staticResolver{route: Route{
		ObjectPath:   "test/object",
		UpstreamPath: "object",
		Policy:       config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok", rec.Body.String())
}

func TestFailoverDoesNotRetryNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first := httptest.NewServer(http.NotFoundHandler())
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer second.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{first.URL, second.URL},
	}, store, &staticResolver{route: Route{
		ObjectPath:   "test/object",
		UpstreamPath: "object",
		Policy:       config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestStaleCacheOnValidationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	resolver := &staticResolver{route: Route{
		ObjectPath:   "test/stale-obj",
		UpstreamPath: "test/stale-obj",
		Policy:       config.PolicyRevalidate,
	}}
	handler := NewHandler("test", RuntimeConfig{
		Mode:        "test",
		ExpireAfter: config.Expiration(72 * time.Hour),
		Upstreams:   []string{upstream.URL},
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/stale-obj", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "MISS", rec.Header().Get("X-Cache"))
	require.Equal(t, "hello", rec.Body.String())

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/stale-obj", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "STALE", rec.Header().Get("X-Cache"))
	require.Equal(t, "hello", rec.Body.String())
}

func TestErrorResponseHidesInternalDetails(t *testing.T) {
	resp := ErrorResponse(http.StatusBadGateway, errors.New("sensitive data"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "internal error", string(body))
	require.Equal(t, "ERROR", resp.Headers["X-Cache"])
}

func TestStreamDownloadClearsInFlightMarkerOnRemoteError(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{"https://upstream.example"},
	}, store, &staticResolver{}, NewStats(prometheus.NewRegistry()), nil)
	handler.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	})
	route := Route{ObjectPath: "test/object", UpstreamPath: "test/object"}
	handler.downloads.Store(route.ObjectPath, struct{}{})

	_, err = handler.streamDownload(context.Background(), httptest.NewRequest(http.MethodGet, "/test/object", nil), route, "MISS")
	require.Error(t, err)
	_, found := handler.downloads.Load(route.ObjectPath)
	require.False(t, found)
}

func TestStreamDownloadClearsInFlightMarkerOnNonOK(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{"https://upstream.example"},
	}, store, &staticResolver{}, NewStats(prometheus.NewRegistry()), nil)
	handler.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     http.Header{"Content-Type": []string{"text/plain"}},
			Body:       io.NopCloser(strings.NewReader("missing")),
			Request:    req,
		}, nil
	})
	route := Route{ObjectPath: "test/object", UpstreamPath: "test/object"}
	handler.downloads.Store(route.ObjectPath, struct{}{})

	resp, err := handler.streamDownload(context.Background(), httptest.NewRequest(http.MethodGet, "/test/object", nil), route, "MISS")
	require.NoError(t, err)
	require.NoError(t, resp.Close())
	_, found := handler.downloads.Load(route.ObjectPath)
	require.False(t, found)
}

func TestStreamToPipeRemovesTempFileOnVerifyFailure(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("TMPDIR", tmpDir)

	var downloads sync.Map
	var wait sync.WaitGroup
	reader, err := StreamToPipe(context.Background(), StreamConfig{
		Body:       io.NopCloser(strings.NewReader("bad-content")),
		ObjectPath: "test/object",
		Downloads:  &downloads,
		Wait:       &wait,
		VerifyFn: func(io.ReadSeeker) error {
			return errors.New("verify failed")
		},
		StoreFn: func(context.Context, io.Reader) error {
			t.Fatal("store must not run after verify failure")
			return nil
		},
	})
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	wait.Wait()

	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestAllUpstreamsUnavailableReturns503(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{"http://127.0.0.1:1/nonexistent"},
	}, store, &staticResolver{route: Route{
		ObjectPath:   "test/downstream",
		UpstreamPath: "test/downstream",
		Policy:       config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/test/downstream", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
