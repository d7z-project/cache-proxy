package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestNPMProxyRewritesTarballURLs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var baseURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/@scope/pkg":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"name":"@scope/pkg","versions":{"1.0.0":{"dist":{"tarball":"`+baseURL+`/@scope/pkg/-/pkg-1.0.0.tgz"}}}}`)
		case "/@scope/pkg/-/pkg-1.0.0.tgz":
			_, _ = io.WriteString(w, "tarball")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	baseURL = upstream.URL

	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"npm": npmSpec(t, "npm", "/npm", upstream.URL)})
	defer closeRuntime(t, rt)

	rec := performRequest(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/npm/@scope/pkg", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "http://example.com/npm/@scope/pkg/-/pkg-1.0.0.tgz")
}
