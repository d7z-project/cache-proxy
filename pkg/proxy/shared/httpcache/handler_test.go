package httpcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
		"vary":                  "Vary",
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
		"source-upstream":           "https://upstream.example",
		UserAgentReviewedOption:     "true",
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
	require.NotContains(t, headers, "source-upstream")
	require.NotContains(t, headers, UserAgentReviewedOption)
}

func TestConfigureClientTransportTimeouts(t *testing.T) {
	client := utils.DefaultHttpClientWrapper()
	ConfigureClientTransport(client, "test", &config.TransportConfig{
		DialTimeout:        config.Duration(2 * time.Second),
		HeaderTimeout:      config.Duration(3 * time.Second),
		IdleBodyTimeout:    config.Duration(4 * time.Second),
		MaxRequestDuration: config.Duration(5 * time.Second),
		MaxIdleConns:       7,
		MaxConnsPerHost:    8,
	})

	require.Equal(t, 5*time.Second, client.Timeout)
	require.Equal(t, 4*time.Second, client.IdleBodyTimeout)
	require.Equal(t, DefaultUserAgent, client.UserAgent)
	require.False(t, client.UserAgentConfigured)
	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 3*time.Second, transport.ResponseHeaderTimeout)
	require.Equal(t, 7, transport.MaxIdleConns)
	require.Equal(t, 8, transport.MaxConnsPerHost)
	require.NotNil(t, transport.DialContext)
}

func TestConfigureClientTransportUserAgentOverride(t *testing.T) {
	client := utils.DefaultHttpClientWrapper()
	ConfigureClientTransport(client, "test", &config.TransportConfig{UserAgent: "custom-client/2"})

	require.Equal(t, "custom-client/2", client.UserAgent)
	require.True(t, client.UserAgentConfigured)
}

func TestUserAgentVaryResponsesAreNotStored(t *testing.T) {
	var mu sync.Mutex
	var userAgents []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		userAgents = append(userAgents, req.UserAgent())
		mu.Unlock()
		w.Header().Set("Vary", "Accept-Encoding, User-Agent")
		_, _ = io.WriteString(w, "browser response")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	handler := NewHandler("test", RuntimeConfig{
		Mode:        "test",
		ExpireAfter: config.Expiration(time.Hour),
		Upstreams:   []string{upstream.URL},
	}, store, &staticResolver{route: Route{
		ObjectPath: "test/browser", Policy: config.PolicyImmutable,
	}}, NewStats(prometheus.NewRegistry()), nil)

	const browserUserAgent = "Mozilla/5.0 Firefox/127.0"
	nonBrowser := httptest.NewRequest(http.MethodGet, "/browser", nil)
	nonBrowser.Header.Set("User-Agent", "curl/8.8.0")
	nonBrowserRec := httptest.NewRecorder()
	handler.ServeHTTP(nonBrowserRec, nonBrowser)
	require.Equal(t, "BYPASS", nonBrowserRec.Header().Get("X-Cache"))
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/browser", nil)
		req.Header.Set("User-Agent", browserUserAgent)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
		require.Equal(t, "Accept-Encoding, User-Agent", rec.Header().Get("Vary"))
	}

	_, err = store.StatObject(context.Background(), "test", "test/browser")
	require.Error(t, err)
	mu.Lock()
	require.Equal(t, []string{DefaultUserAgent, browserUserAgent, browserUserAgent}, userAgents)
	mu.Unlock()
}

func TestBrowserRequestRefreshesUnreviewedCacheObject(t *testing.T) {
	var upstreamRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests.Add(1)
		_, _ = io.WriteString(w, "reviewed")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.MkdirAll("test/test", 0o755))
	_, err = store.Put(context.Background(), "test", "test/browser", strings.NewReader("legacy"), map[string]string{
		"fetched-at": time.Now().UTC().Format(time.RFC3339Nano),
	})
	require.NoError(t, err)

	handler := NewHandler("test", RuntimeConfig{
		Mode: "test", ExpireAfter: config.Expiration(time.Hour), Upstreams: []string{upstream.URL},
	}, store, &staticResolver{route: Route{
		ObjectPath: "test/browser", Policy: config.PolicyImmutable,
	}}, NewStats(prometheus.NewRegistry()), nil)
	for _, expectedCache := range []string{"MISS", "HIT"} {
		req := httptest.NewRequest(http.MethodGet, "/browser", nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 Firefox/127.0")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, expectedCache, rec.Header().Get("X-Cache"))
		require.Equal(t, "reviewed", rec.Body.String())
	}
	require.Equal(t, int32(1), upstreamRequests.Load())
}

func TestUserAgentFallbackAndConfiguredOverride(t *testing.T) {
	userAgents := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		userAgents <- req.UserAgent()
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	resolver := &staticResolver{route: Route{Policy: config.PolicyBypass}}
	defaultHandler := NewHandler("default", RuntimeConfig{Mode: "test", Upstreams: []string{upstream.URL}}, store, resolver, NewStats(prometheus.NewRegistry()), nil)
	forcedHandler := NewHandler("forced", RuntimeConfig{
		Mode: "test", Upstreams: []string{upstream.URL}, Transport: &config.TransportConfig{UserAgent: "forced/1"},
	}, store, resolver, NewStats(prometheus.NewRegistry()), nil)

	nonBrowser := httptest.NewRequest(http.MethodGet, "/curl", nil)
	nonBrowser.Header.Set("User-Agent", "curl/8.8.0")
	defaultHandler.ServeHTTP(httptest.NewRecorder(), nonBrowser)
	browser := httptest.NewRequest(http.MethodGet, "/browser", nil)
	browser.Header.Set("User-Agent", "Mozilla/5.0 Chrome/126.0 Safari/537.36")
	forcedHandler.ServeHTTP(httptest.NewRecorder(), browser)

	require.Equal(t, DefaultUserAgent, <-userAgents)
	require.Equal(t, "forced/1", <-userAgents)
}

func TestConfiguredUserAgentCachesSingleVaryRepresentation(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		require.Equal(t, "forced/1", req.UserAgent())
		w.Header().Set("Vary", "User-Agent")
		_, _ = io.WriteString(w, "fixed representation")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	handler := NewHandler("test", RuntimeConfig{
		Mode:        "test",
		ExpireAfter: config.Expiration(time.Hour),
		Upstreams:   []string{upstream.URL},
		Transport:   &config.TransportConfig{UserAgent: "forced/1"},
	}, store, &staticResolver{route: Route{
		ObjectPath: "test/forced", Policy: config.PolicyImmutable,
	}}, NewStats(prometheus.NewRegistry()), nil)

	for _, expectedCache := range []string{"MISS", "HIT"} {
		req := httptest.NewRequest(http.MethodGet, "/forced", nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 Firefox/127.0")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, expectedCache, rec.Header().Get("X-Cache"))
		require.Equal(t, "User-Agent", rec.Header().Get("Vary"))
	}
	require.Equal(t, int32(1), requests.Load())
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

type literalResolver struct {
	route Route
}

func (r literalResolver) Resolve(*http.Request) (Route, error) {
	return r.route, nil
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

	const browserUserAgent = "Mozilla/5.0 Firefox/127.0"
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, browserUserAgent, r.UserAgent())
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
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
	req.Header.Set("User-Agent", browserUserAgent)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "target", rec.Body.String())
}

func TestStatsUpstreamKeyUsesTargetOrigin(t *testing.T) {
	tests := map[string]string{
		"https://User:Pass@Example.COM:8443/a/b?x=1#frag": "https://example.com:8443",
		"http://Example.COM/object":                       "http://example.com",
		"not a url":                                       "not a url",
	}
	for input, expected := range tests {
		require.Equal(t, expected, statsUpstreamKey(input))
	}
}

func TestTargetURLStatsAggregateByOrigin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()

	stats := NewStats(prometheus.NewRegistry())
	handler := NewHandler("test", RuntimeConfig{Mode: "test"}, nil, nil, stats, nil)
	allowedHosts := []string{strings.TrimPrefix(target.URL, "http://")}

	for _, targetPath := range []string{"/files/one.whl?download=1", "/files/two.whl?download=1"} {
		resp, err := handler.openRemote(ctx, http.MethodGet, "", remoteOptions{
			AcceptErrors:       true,
			Record:             true,
			TargetURL:          target.URL + targetPath,
			AllowedTargetHosts: allowedHosts,
		}, nil)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Close())
	}

	snap := stats.Snapshot()
	upstreams := snap.Instances["test"].Upstreams
	require.Len(t, upstreams, 1)
	require.Equal(t, uint64(2), upstreams[target.URL].Requests)
	require.Equal(t, uint64(4), upstreams[target.URL].ResponseBytes)
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

func TestTargetURLFallsBackOnClientError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
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

func TestTargetURLFailsWhenRetryableStatusHasNoFallbackPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer target.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "upstream")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewHandler("test", RuntimeConfig{Mode: "test", Upstreams: []string{upstream.URL}}, store, literalResolver{route: Route{
		ObjectPath:         "test/object",
		TargetURL:          target.URL + "/object",
		AllowedTargetHosts: []string{strings.TrimPrefix(target.URL, "http://")},
		Policy:             config.PolicyBypass,
	}}, NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
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

func TestFailoverRetriesNonNotFoundClientErrors(t *testing.T) {
	tests := map[string]int{
		"bad request": http.StatusBadRequest,
		"forbidden":   http.StatusForbidden,
		"gone":        http.StatusGone,
	}
	for name, status := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, http.StatusText(status), status)
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
		})
	}
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

func TestFailoverReturnsUnavailableWhenAllClientErrorsAreRetryable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const browserUserAgent = "Mozilla/5.0 Chrome/126.0 Safari/537.36"
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, browserUserAgent, r.UserAgent())
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, browserUserAgent, r.UserAgent())
		http.Error(w, "forbidden", http.StatusForbidden)
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
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
	req.Header.Set("User-Agent", browserUserAgent)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestRecordUpstreamCountsNonNotFoundClientErrors(t *testing.T) {
	stats := NewStats(prometheus.NewRegistry())
	stats.RecordUpstream("test", "test", http.MethodGet, http.StatusBadRequest)
	stats.RecordUpstream("test", "test", http.MethodGet, http.StatusForbidden)
	stats.RecordUpstream("test", "test", http.MethodGet, http.StatusNotFound)

	snap := stats.Snapshot()
	instance := snap.Instances["test"]
	require.Equal(t, uint64(3), instance.UpstreamRequests)
	require.Equal(t, uint64(2), instance.UpstreamErrors)
	require.Equal(t, uint64(2), snap.Total.UpstreamErrors)
	require.Equal(t, uint64(1), instance.UpstreamStatus["404"])
}

func TestRecordUpstreamRequestTracksConcreteUpstream(t *testing.T) {
	stats := NewStats(prometheus.NewRegistry())
	release := stats.BeginUpstreamRequest("test", "test", "https://upstream.example")

	snap := stats.Snapshot()
	require.Equal(t, int64(1), snap.Instances["test"].ActiveUpstreams)
	require.Equal(t, int64(1), snap.Instances["test"].Upstreams["https://upstream.example"].ActiveRequests)

	stats.RecordUpstreamRequest(
		"test",
		"test",
		"https://upstream.example",
		http.MethodGet,
		http.StatusBadGateway,
		25*time.Millisecond,
		128,
	)
	release()
	release()

	snap = stats.Snapshot()
	instance := snap.Instances["test"]
	upstream := instance.Upstreams["https://upstream.example"]
	require.Equal(t, uint64(1), instance.UpstreamRequests)
	require.Equal(t, uint64(1), instance.UpstreamErrors)
	require.Equal(t, uint64(128), instance.UpstreamBytes)
	require.Equal(t, int64(0), instance.ActiveUpstreams)
	require.Equal(t, uint64(1), upstream.Requests)
	require.Equal(t, uint64(1), upstream.Errors)
	require.Equal(t, uint64(128), upstream.ResponseBytes)
	require.Equal(t, int64(0), upstream.ActiveRequests)
	require.Equal(t, "502", upstream.LastStatus)
	require.Equal(t, "502", upstream.LastError)
}

func TestOpenRemoteReleasesActiveUpstreamOnBodyClose(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		_, _ = w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	stats := NewStats(prometheus.NewRegistry())
	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{upstream.URL},
	}, nil, nil, stats, nil)

	resp, err := handler.openRemote(context.Background(), http.MethodGet, "object", remoteOptions{
		AcceptErrors: true,
		Record:       true,
	}, nil)
	require.NoError(t, err)

	snap := stats.Snapshot()
	require.Equal(t, int64(1), snap.Instances["test"].Upstreams[upstream.URL].ActiveRequests)
	buffer := make([]byte, 2)
	_, err = io.ReadFull(resp.Body, buffer)
	require.NoError(t, err)
	require.NoError(t, resp.Close())

	snap = stats.Snapshot()
	require.Equal(t, int64(0), snap.Instances["test"].Upstreams[upstream.URL].ActiveRequests)
	require.Equal(t, uint64(2), snap.Instances["test"].Upstreams[upstream.URL].ResponseBytes)
}

func TestOpenRemoteReleasesActiveUpstreamOnFailoverStatus(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer upstream.Close()

	stats := NewStats(prometheus.NewRegistry())
	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{upstream.URL},
	}, nil, nil, stats, nil)

	resp, err := handler.openRemote(context.Background(), http.MethodGet, "object", remoteOptions{
		AcceptErrors: true,
		Record:       true,
	}, nil)
	require.Error(t, err)
	require.Nil(t, resp)

	snap := stats.Snapshot()
	require.Equal(t, int64(0), snap.Instances["test"].ActiveUpstreams)
	require.Equal(t, int64(0), snap.Instances["test"].Upstreams[upstream.URL].ActiveRequests)
	require.Equal(t, uint64(1), snap.Instances["test"].Upstreams[upstream.URL].Errors)
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

func TestStreamDownloadCompletesFlightOnRemoteError(t *testing.T) {
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
	flight, leader := handler.flights.begin(route.ObjectPath)
	require.True(t, leader)

	_, err = handler.streamDownload(httptest.NewRequest(http.MethodGet, "/test/object", nil), route, "MISS", flight)
	require.Error(t, err)
	select {
	case <-flight.done:
	default:
		t.Fatal("flight was not completed")
	}
	_, leader = handler.flights.begin(route.ObjectPath)
	require.True(t, leader)
}

func TestStreamDownloadCompletesFlightOnNonOK(t *testing.T) {
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
	flight, leader := handler.flights.begin(route.ObjectPath)
	require.True(t, leader)

	resp, err := handler.streamDownload(httptest.NewRequest(http.MethodGet, "/test/object", nil), route, "MISS", flight)
	require.NoError(t, err)
	require.NoError(t, resp.Close())
	select {
	case <-flight.done:
	default:
		t.Fatal("flight was not completed")
	}
	_, leader = handler.flights.begin(route.ObjectPath)
	require.True(t, leader)
}

func TestStreamToCacheRemovesTempFileOnVerifyFailure(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("TMPDIR", tmpDir)

	var wait sync.WaitGroup
	reader, err := StreamToCache(context.Background(), StreamConfig{
		Body:       io.NopCloser(strings.NewReader("bad-content")),
		ObjectPath: "test/object",
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

func TestStreamToCacheContinuesAfterClientReaderCloses(t *testing.T) {
	var wait sync.WaitGroup
	done := make(chan error, 1)
	payload := strings.Repeat("x", 1<<20)
	reader, err := StreamToCache(context.Background(), StreamConfig{
		Body:       io.NopCloser(strings.NewReader(payload)),
		ObjectPath: "test/slow-client",
		Wait:       &wait,
		StoreFn: func(_ context.Context, source io.Reader) error {
			stored, readErr := io.ReadAll(source)
			if readErr != nil {
				return readErr
			}
			if string(stored) != payload {
				return errors.New("stored payload mismatch")
			}
			return nil
		},
		Done: func(err error) { done <- err },
	})
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	select {
	case fillErr := <-done:
		require.NoError(t, fillErr)
	case <-time.After(2 * time.Second):
		t.Fatal("cache fill was blocked by an unread client body")
	}
	wait.Wait()
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

func TestConcurrentNPMMetadataMissIsFetchedOnceAndRewritten(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var requests atomic.Int64
	started := make(chan struct{})
	releaseUpstream := make(chan struct{})
	var startOnce sync.Once
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		startOnce.Do(func() { close(started) })
		<-releaseUpstream
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":"pkg","versions":{"1.0.0":{"dist":{"tarball":%q}}}}`, upstream.URL+"/pkg/-/pkg-1.0.0.tgz")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	handler := NewHandler("npm", RuntimeConfig{
		Mode:            config.ModeNPM,
		Upstreams:       []string{upstream.URL},
		DefaultFreshFor: config.Freshness(time.Minute),
	}, store, literalResolver{route: Route{
		ObjectPath:   "npm/metadata/pkg",
		UpstreamPath: "pkg",
		Policy:       config.PolicyRevalidate,
		BusyPolicy:   config.BusyPolicyStale,
		RewriteKind:  "npm-metadata",
	}}, NewStats(prometheus.NewRegistry()), nil)

	const clients = 12
	ready := sync.WaitGroup{}
	ready.Add(clients)
	begin := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, clients)
	for range clients {
		go func() {
			ready.Done()
			<-begin
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "http://cache.example/npm/pkg", nil)
			req.Header.Set("X-Cache-Proxy-Prefix", "/npm")
			handler.ServeHTTP(recorder, req)
			results <- recorder
		}()
	}
	ready.Wait()
	close(begin)
	<-started
	time.Sleep(25 * time.Millisecond)
	close(releaseUpstream)

	for range clients {
		recorder := <-results
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Contains(t, recorder.Body.String(), "http://cache.example/npm/pkg/-/pkg-1.0.0.tgz")
	}
	require.EqualValues(t, 1, requests.Load())
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestHandlerCountsActuallyStreamedResponseBytes(t *testing.T) {
	const body = "chunked response body"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	stats := NewStats(prometheus.NewRegistry())
	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{upstream.URL},
	}, store, literalResolver{route: Route{
		ObjectPath:   "objects/chunked",
		UpstreamPath: "objects/chunked",
		Policy:       config.PolicyImmutable,
	}}, stats, nil)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://cache.example/objects/chunked", nil))
	require.Equal(t, body, recorder.Body.String())
	snapshot := stats.Snapshot()
	require.Equal(t, uint64(len(body)), snapshot.Total.ResponseBytes)
	require.Equal(t, uint64(len(body)), snapshot.Total.UpstreamBytes)
}

func TestUpstreamAdmissionIsAcquiredBeforeRequest(t *testing.T) {
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	limiter := NewDownloadLimiter(1, 1)
	release, err := limiter.Acquire(context.Background(), "test")
	require.NoError(t, err)
	handler := NewHandler("test", RuntimeConfig{
		Mode:            "test",
		Upstreams:       []string{upstream.URL},
		DownloadLimiter: limiter,
	}, nil, literalResolver{route: Route{Policy: config.PolicyBypass}}, NewStats(prometheus.NewRegistry()), nil)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://cache.example/object", nil))
		done <- recorder
	}()
	time.Sleep(25 * time.Millisecond)
	require.Zero(t, requests.Load())
	release()

	recorder := <-done
	require.Equal(t, http.StatusOK, recorder.Code)
	require.EqualValues(t, 1, requests.Load())
}

func TestHandlerCountsInterruptedResponseBytes(t *testing.T) {
	stats := NewStats(prometheus.NewRegistry())
	handler := NewHandler("test", RuntimeConfig{
		Mode:      "test",
		Upstreams: []string{"https://upstream.example"},
	}, nil, literalResolver{route: Route{Policy: config.PolicyBypass}}, stats, nil)
	handler.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: 100,
			Header:        http.Header{"Content-Length": []string{"100"}},
			Body:          &interruptedBody{},
			Request:       req,
		}, nil
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://cache.example/object", nil))
	require.Equal(t, "abc", recorder.Body.String())
	snapshot := stats.Snapshot()
	require.Equal(t, uint64(3), snapshot.Total.ResponseBytes)
	require.Equal(t, uint64(3), snapshot.Total.UpstreamBytes)
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

type interruptedBody struct {
	sent bool
}

func (b *interruptedBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.ErrUnexpectedEOF
	}
	b.sent = true
	return copy(p, "abc"), nil
}

func (*interruptedBody) Close() error { return nil }
