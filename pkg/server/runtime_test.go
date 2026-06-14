package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	cargoproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/cargo"
	fileproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	gomodproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/gomod"
	mavenproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/maven"
	npmproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/npm"
	ociproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/oci"
	pypiproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/pypi"
)

func requestBody(t *testing.T, handler http.Handler, method, target string) string {
	t.Helper()
	rec := performRequest(t, handler, method, target, nil, nil)
	require.True(t, rec.Code < 400, "expected success status, got %d: %s", rec.Code, rec.Body.String())
	return rec.Body.String()
}

func performRequest(t *testing.T, handler http.Handler, method, target string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, method, target, body)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func httpBody(t *testing.T, rawURL string) string {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(rawURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(data)
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().String()
}

func newTestRuntime(t *testing.T, ctx context.Context, instances map[string]config.InstanceSpec) *Runtime {
	t.Helper()
	dir, err := os.MkdirTemp("", "cache-proxy-runtime-*")
	require.NoError(t, err)
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	}, instances)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return rt
}

func newTestRuntimeWithOptions(t *testing.T, ctx context.Context, options Options, instances map[string]config.InstanceSpec) *Runtime {
	t.Helper()
	rt, err := OpenWithOptions(ctx, options)
	require.NoError(t, err)
	if instances != nil {
		global := config.CloneGlobal(rt.global)
		removed := []string{}
		for name := range rt.instances {
			if _, keep := instances[name]; !keep {
				removed = append(removed, name)
			}
		}
		_, err = rt.replaceState(ctx, rt.generation, global, config.CloneInstances(instances), collectChanged(instances), removed)
		require.NoError(t, err)
	}
	return rt
}

func closeRuntime(t *testing.T, rt *Runtime) {
	t.Helper()
	if rt == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, rt.Close(ctx))
}

func mustJSONReader(t *testing.T, value any) *bytes.Reader {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return bytes.NewReader(data)
}

func mustPolicyJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}

func fileSpec(t *testing.T, name, routePath, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Path: routePath},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &fileproxy.Policy{DefaultPolicy: config.PolicyBypass, BusyPolicy: config.BusyPolicyBypass, Rules: []fileproxy.Rule{}}),
	}
}

func npmSpec(t *testing.T, name, routePath, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModeNPM, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Path: routePath},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &npmproxy.Policy{DefaultPolicy: config.PolicyRevalidate, BusyPolicy: config.BusyPolicyBypass, Rules: []npmproxy.Rule{}}),
	}
}

func ociSpec(t *testing.T, name, bind, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModeOCI, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Bind: bind},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &ociproxy.Policy{DefaultPolicy: config.PolicyRevalidate, BusyPolicy: config.BusyPolicyBypass, Rules: []ociproxy.Rule{}}),
	}
}

func goSpec(t *testing.T, name, routePath, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModeGo, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Path: routePath},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &gomodproxy.Policy{SumDB: &gomodproxy.SumDBConfig{Enabled: false}, DisableModuleFetchHeader: true}),
	}
}

func mavenSpec(t *testing.T, name, routePath, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModeMaven, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Path: routePath},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &mavenproxy.Policy{MetadataFreshFor: config.Duration(30 * time.Second), MetadataBusyPolicy: config.BusyPolicyStale, ReleasePolicy: config.PolicyImmutable, SnapshotPolicy: config.PolicyRevalidate, SnapshotFreshFor: config.Duration(15 * time.Second), Rules: []mavenproxy.Rule{}}),
	}
}

func cargoSpec(t *testing.T, name, routePath, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModeCargo, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Path: routePath},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &cargoproxy.Policy{IndexFreshFor: config.Duration(30 * time.Second), IndexBusyPolicy: config.BusyPolicyStale, CratePolicy: config.PolicyImmutable}),
	}
}

func pypiSpec(t *testing.T, name, routePath, upstream string) config.InstanceSpec {
	t.Helper()
	return config.InstanceSpec{
		Name:   name,
		Meta:   config.InstanceMeta{Mode: config.ModePyPI, Enabled: true, ExpireAfter: config.Duration(time.Hour)},
		Route:  config.InstanceRoute{Path: routePath},
		Source: config.InstanceSource{Upstreams: []string{upstream}},
		Policy: mustPolicyJSON(t, &pypiproxy.Policy{SimpleFreshFor: config.Duration(30 * time.Second), SimpleBusyPolicy: config.BusyPolicyStale, FilePolicy: config.PolicyImmutable, ProxyJSON: true}),
	}
}
