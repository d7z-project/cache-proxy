package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestCargoProxyRewritesConfigAndServesDownload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/config.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"dl":"`+upstream.URL+`/api/v1/crates/{crate}/{version}/download"}`)
		case "/se/rd/serde":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, `{"name":"serde","vers":"1.0.0"}`+"\n")
		case "/api/v1/crates/serde/1.0.0/download":
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, "crate-bytes")
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()

	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"cargo": cargoSpec(t, "cargo", "/cargo", upstream.URL)})
	defer closeRuntime(t, rt)

	configBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/cargo/config.json")
	require.Contains(t, configBody, `/cargo/api/v1/crates/{crate}/{version}/download`)

	indexBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/cargo/se/rd/serde")
	require.Contains(t, indexBody, `"name":"serde"`)

	crateBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/cargo/api/v1/crates/serde/1.0.0/download")
	require.Equal(t, "crate-bytes", strings.TrimSpace(crateBody))
}

func TestCargoProxyConfigCanRequireAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/config.json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"dl":"`+upstream.URL+`/api/v1/crates/{crate}/{version}/download","auth-required":false}`)
			return
		}
		http.NotFound(w, req)
	}))
	defer upstream.Close()

	spec := cargoSpec(t, "cargo", "/cargo", upstream.URL)
	spec.Policy = mustPolicyJSON(t, map[string]any{
		"indexFreshFor":   "30s",
		"indexBusyPolicy": "stale",
		"cratePolicy":     "immutable",
		"authRequired":    true,
	})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"cargo": spec})
	defer closeRuntime(t, rt)

	configBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/cargo/config.json")
	require.Contains(t, configBody, `"auth-required":true`)
}
