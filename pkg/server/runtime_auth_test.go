package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestAdminAPIsRequireSessionWhenPasswordIsConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      t.TempDir(),
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "secret",
		MetricsToken: "",
	}, map[string]config.InstanceSpec{})
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)
	unauthorized := performRequest(t, handler, http.MethodGet, "/-/api/runtime", nil, nil)
	require.Equal(t, http.StatusUnauthorized, unauthorized.Code)

	login := performRequest(t, handler, http.MethodPost, "/-/api/login", strings.NewReader(`{"password":"secret"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, login.Code, login.Body.String())

	authorizedReq := httptest.NewRequest(http.MethodGet, "/-/api/runtime", nil)
	for _, cookie := range login.Result().Cookies() {
		authorizedReq.AddCookie(cookie)
	}
	authorizedRec := httptest.NewRecorder()
	handler.ServeHTTP(authorizedRec, authorizedReq)
	require.Equal(t, http.StatusOK, authorizedRec.Code)
}
