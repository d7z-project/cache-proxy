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
	ociproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/oci"
)

func TestOCIProxyServesBoundRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{"schemaVersion":2}`)
	}))
	defer upstream.Close()

	spec := ociSpec(t, "oci", freeLocalAddr(t), upstream.URL)
	spec.Policy = mustPolicyJSON(t, &ociproxy.Policy{
		DefaultPolicy: config.PolicyRevalidate,
		BusyPolicy:    config.BusyPolicyBypass,
		Auth:          &ociproxy.AuthConfig{Type: "basic", Username: "user", Password: "pass"},
		Rules:         []ociproxy.Rule{},
	})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"oci": spec})
	defer closeRuntime(t, rt)
	require.NoError(t, rt.Start())

	body := httpBody(t, "http://"+spec.Route.Bind+"/v2/library/alpine/manifests/latest")
	require.Contains(t, body, "schemaVersion")
}
