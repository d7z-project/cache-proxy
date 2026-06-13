package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestAdminLoginAndSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "test-secret",
		MetricsToken: "",
	}, nil)
	defer closeRuntime(t, rt)

	// without auth: API returns 401
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/config", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// login with wrong password
	loginBody := strings.NewReader(`{"password":"wrong"}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/-/api/login", loginBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// login with correct password
	loginBody = strings.NewReader(`{"password":"test-secret"}`)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/-/api/login", loginBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	cookies := rec.Result().Cookies()
	require.Len(t, cookies, 1)
	require.Equal(t, "cache-proxy-session", cookies[0].Name)
	require.True(t, cookies[0].HttpOnly)
	require.False(t, cookies[0].Secure)

	// use session cookie to access protected API
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/api/config", nil).WithContext(ctx)
	req.AddCookie(cookies[0])
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// logout
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/-/api/logout", nil).WithContext(ctx)
	req.AddCookie(cookies[0])
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	clearCookie := rec.Result().Cookies()
	require.Len(t, clearCookie, 1)
	require.Equal(t, -1, clearCookie[0].MaxAge)
}

func TestAdminLoginMarksSessionCookieSecureBehindHTTPSProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "test-secret",
		MetricsToken: "",
	}, nil)
	defer closeRuntime(t, rt)

	loginBody := strings.NewReader(`{"password":"test-secret"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/-/api/login", loginBody).WithContext(ctx)
	req.Header.Set("X-Forwarded-Proto", "https")
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	cookies := rec.Result().Cookies()
	require.Len(t, cookies, 1)
	require.True(t, cookies[0].Secure)
}

func TestPublicInstancesAPIVisibleWithoutAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "test-secret",
		MetricsToken: "my-metrics-token",
	}, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/public/instances", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var items []struct {
		Name string `json:"name"`
		Mode string `json:"mode"`
		Path string `json:"path,omitempty"`
		Bind string `json:"bind,omitempty"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&items))
	require.Len(t, items, 1)
	require.Equal(t, "files", items[0].Name)
	require.Equal(t, "/files", items[0].Path)
}

func TestSPAServesWithoutAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "test-secret",
		MetricsToken: "",
	}, nil)
	defer closeRuntime(t, rt)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	// SPA serves index.html even without auth
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestLoginRateLimiterBlocksAfterMaxAttempts(t *testing.T) {
	limiter := newLoginRateLimiter()
	for i := 0; i < 5; i++ {
		require.True(t, limiter.allow("10.0.0.1"), "attempt %d should be allowed", i+1)
	}
	require.False(t, limiter.allow("10.0.0.1"), "6th attempt should be blocked")
}

func TestLoginRateLimiterCleansExpiredIPs(t *testing.T) {
	limiter := &loginRateLimiter{attempts: map[string][]time.Time{
		"10.0.0.1": {time.Now().Add(-2 * time.Minute)},
		"10.0.0.2": {time.Now().Add(-10 * time.Second), time.Now().Add(-5 * time.Second)},
	}}
	require.True(t, limiter.allow("10.0.0.1"))
	require.Len(t, limiter.attempts["10.0.0.1"], 1)
	require.True(t, limiter.allow("10.0.0.2"))
	require.Len(t, limiter.attempts["10.0.0.2"], 3)
}

func TestLoginRateLimiterCloseStopsCleanup(t *testing.T) {
	limiter := newLoginRateLimiter()
	require.True(t, limiter.allow("10.0.0.1"))
	limiter.close()
	// double close should not panic
	require.NotPanics(t, func() { limiter.close() })
}
