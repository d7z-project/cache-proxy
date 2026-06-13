package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestRuntimeStoresConfigInBlobFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	snapshot, err := rt.Snapshot(ctx)
	require.NoError(t, err)
	require.NotZero(t, snapshot.Generation)
	require.Contains(t, snapshot.YAML, "instances:")
}

func TestRuntimeDoesNotOverwriteInvalidStoredConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	}, nil)
	_, err := rt.store.Put(ctx, systemTenant, configPath, strings.NewReader("instances: ["), map[string]string{"type": "config"})
	require.NoError(t, err)
	closeRuntime(t, rt)

	_, err = OpenWithOptions(ctx, Options{Backend: dir, Bind: "127.0.0.1:0", MetricsPath: "/-/metrics", GCInterval: time.Hour})
	require.Error(t, err)

	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	reader, err := store.OpenObject(ctx, systemTenant, configPath)
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "instances: [", string(data))
}

func TestStartRejectsMainBindConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	rt := newTestRuntimeWithOptions(t, ctx, Options{
		Backend:    t.TempDir(),
		Bind:       listener.Addr().String(),
		GCInterval: time.Hour,
	}, nil)
	defer closeRuntime(t, rt)
	require.ErrorContains(t, rt.Start(), "listen")
}

func TestInstanceBindCanChangeDynamically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("dynamic"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{})
	defer closeRuntime(t, rt)
	require.NoError(t, rt.Start())

	firstBind := freeLocalAddr(t)
	cfg := DefaultConfig()
	cfg.Server.Metrics.Path = ""
	cfg.Instances = map[string]config.InstanceConfig{"files": {Mode: config.ModeFile, Listen: config.ListenConfig{Bind: firstBind}, Upstreams: []string{upstream.URL}, Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass}}}
	snapshot, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.NoError(t, err)
	require.Equal(t, "dynamic", httpBody(t, "http://"+firstBind+"/a.txt"))

	secondBind := freeLocalAddr(t)
	next := structuredCloneConfig(snapshot.Config)
	instance := next.Instances["files"]
	instance.Listen = config.ListenConfig{Bind: secondBind}
	next.Instances["files"] = instance
	_, err = rt.UpdateConfig(ctx, snapshot.Generation, next)
	require.NoError(t, err)
	require.Equal(t, "dynamic", httpBody(t, "http://"+secondBind+"/a.txt"))
	resp, err := (&http.Client{Timeout: 200 * time.Millisecond}).Get("http://" + firstBind + "/a.txt")
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
}

func TestUpdateConfigPreservesStartupOnlyFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	snapshot, err := rt.Snapshot(ctx)
	require.NoError(t, err)
	next := structuredCloneConfig(snapshot.Config)
	next.Version = 99
	next.Server.Metrics.Path = "/changed"
	next.Storage.GC.Blob = config.Duration(time.Minute)
	updated, err := rt.UpdateConfig(ctx, snapshot.Generation, next)
	require.NoError(t, err)
	require.Equal(t, snapshot.Config.Version, updated.Config.Version)
	require.Equal(t, snapshot.Config.Server, updated.Config.Server)
	require.Equal(t, snapshot.Config.Storage, updated.Config.Storage)
}

func TestCloneConfigDoesNotShareNestedInstanceState(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"oci": {
		Mode:        config.ModeOCI,
		Listen:      config.ListenConfig{Bind: "127.0.0.1:5000"},
		Upstreams:   []string{"https://registry.example"},
		Transport:   &config.TransportConfig{UserAgent: "original"},
		PassHeaders: []string{"Accept"},
		Cache:       config.CacheConfig{Rules: []config.CacheRule{{Match: "**", Policy: config.PolicyBypass}}},
		OCI: &config.OCIConfig{
			Rules: []config.OCICacheRule{{Match: "library/*", Policy: config.PolicyImmutable}},
			Auth:  &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "secret"},
		},
	}}
	cloned := cloneConfig(cfg)
	item := cloned.Instances["oci"]
	item.Upstreams[0] = "https://changed.example"
	item.PassHeaders[0] = "Accept-Language"
	item.Transport.UserAgent = "changed"
	item.Cache.Rules[0].Match = "changed"
	item.OCI.Rules[0].Match = "changed"
	item.OCI.Auth.Password = "changed"
	cloned.Instances["oci"] = item

	original := cfg.Instances["oci"]
	require.Equal(t, "https://registry.example", original.Upstreams[0])
	require.Equal(t, "Accept", original.PassHeaders[0])
	require.Equal(t, "original", original.Transport.UserAgent)
	require.Equal(t, "**", original.Cache.Rules[0].Match)
	require.Equal(t, "library/*", original.OCI.Rules[0].Match)
	require.Equal(t, "secret", original.OCI.Auth.Password)
}

func TestPreserveMaskedCredentialsClearSentinel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "secret-password"}},
	}})
	defer closeRuntime(t, rt)

	// verify initial password
	rt.mu.RLock()
	require.Equal(t, "secret-password", rt.config.Instances["oci"].OCI.Auth.Password)
	require.Equal(t, "basic", rt.config.Instances["oci"].OCI.Auth.Type)
	rt.mu.RUnlock()

	// update with CLEAR_SENTINEL to clear credentials - auth should be removed
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "-"}},
	}}
	_, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.NoError(t, err)

	// verify credentials are cleared and auth is removed (set to nil)
	rt.mu.RLock()
	require.Nil(t, rt.config.Instances["oci"].OCI.Auth)
	rt.mu.RUnlock()
}

func TestPreserveMaskedCredentialsKeepsMasked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "secret-password"}},
	}})
	defer closeRuntime(t, rt)

	// update with *** to keep credentials
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "***"}},
	}}
	_, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.NoError(t, err)

	// verify credentials are preserved
	rt.mu.RLock()
	require.Equal(t, "secret-password", rt.config.Instances["oci"].OCI.Auth.Password)
	rt.mu.RUnlock()
}

func TestPreserveMaskedCredentialsClearBearerSentinel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "bearer", Token: "secret-token"}},
	}})
	defer closeRuntime(t, rt)

	// verify initial token
	rt.mu.RLock()
	require.Equal(t, "secret-token", rt.config.Instances["oci"].OCI.Auth.Token)
	require.Equal(t, "bearer", rt.config.Instances["oci"].OCI.Auth.Type)
	rt.mu.RUnlock()

	// update with CLEAR_SENTINEL to clear token - auth should be removed
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "bearer", Token: "-"}},
	}}
	_, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.NoError(t, err)

	// verify token is cleared and auth is removed (set to nil)
	rt.mu.RLock()
	require.Nil(t, rt.config.Instances["oci"].OCI.Auth)
	rt.mu.RUnlock()
}

func TestValidateBindAddressRejectsInvalidFormat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Bind: "invalid-address"}, Upstreams: []string{"https://example.com"},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must be host:port format")
}

func TestValidateBindAddressRejectsInvalidPort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Bind: "127.0.0.1:99999"}, Upstreams: []string{"https://example.com"},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "port must be 1-65535")
}

func TestValidateTransportRejectsNegativeTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
		Transport: &config.TransportConfig{Timeout: config.Duration(-5 * time.Second)},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "transport timeout must not be negative")
}

func TestValidateCrossModeConfig(t *testing.T) {
	// File mode with OCI config should fail
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"bad": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
		OCI:   &config.OCIConfig{},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must not have oci config")

	// File mode with NPM config should fail
	cfg.Instances = map[string]config.InstanceConfig{"bad": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
		Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
		NPM:   &config.NPMConfig{},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must not have npm config")

	// OCI mode with NPM config should fail
	cfg.Instances = map[string]config.InstanceConfig{"bad": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5000"}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{},
		NPM: &config.NPMConfig{},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must not have npm config")

	// NPM mode with OCI config should fail
	cfg.Instances = map[string]config.InstanceConfig{"bad": {
		Mode: config.ModeNPM, Listen: config.ListenConfig{Path: "/npm"}, Upstreams: []string{"https://registry.npmjs.org"},
		NPM: &config.NPMConfig{},
		OCI: &config.OCIConfig{},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "must not have oci config")
}

func TestValidateOCIBasicAuthRequiresPassword(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: "127.0.0.1:5000"}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user"}},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "oci basic auth password is empty")
}

func TestValidateCacheRuleRequiresPolicy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
		Cache: config.CacheConfig{
			DefaultPolicy: config.PolicyBypass,
			Rules:         []config.CacheRule{{Match: "**/*.txt", Policy: ""}},
		},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "cache rule policy is empty")
}

func TestValidateListenPathRejectsRoot(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/"},
		Upstreams: []string{"https://example.com"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}
	require.ErrorContains(t, ValidateConfig(cfg, "127.0.0.1:0"), "web UI root")
}

func TestDeleteInstanceCleansCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{
		"files": {
			Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
			Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
		},
		"keep": {
			Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/keep"}, Upstreams: []string{"https://example.com"},
			Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
		},
	})
	defer closeRuntime(t, rt)

	require.NoError(t, rt.store.MkdirAll("files/test", 0o755))
	_, err := rt.store.Put(ctx, "files", "test/obj.txt", strings.NewReader("cached-data"), map[string]string{"type": "cache"})
	require.NoError(t, err)
	require.NoError(t, rt.store.MkdirAll("keep/test", 0o755))
	_, err = rt.store.Put(ctx, "keep", "test/obj.txt", strings.NewReader("cached-data"), map[string]string{"type": "cache"})
	require.NoError(t, err)

	reader, err := rt.store.OpenObject(ctx, "files", "test/obj.txt")
	require.NoError(t, err)
	reader.Close()

	reader, err = rt.store.OpenObject(ctx, "keep", "test/obj.txt")
	require.NoError(t, err)
	reader.Close()

	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{
		"keep": {
			Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/keep"}, Upstreams: []string{"https://example.com"},
			Cache: config.CacheConfig{DefaultPolicy: config.PolicyBypass},
		},
	}
	snapshot, err := rt.UpdateConfig(ctx, rt.generation, cfg)
	require.NoError(t, err)
	require.Len(t, snapshot.Config.Instances, 1)
	require.Contains(t, snapshot.Config.Instances, "keep")

	_, err = rt.store.OpenObject(ctx, "files", "test/obj.txt")
	require.Error(t, err, "deleted instance cache should be gone")

	reader, err = rt.store.OpenObject(ctx, "keep", "test/obj.txt")
	require.NoError(t, err, "kept instance cache should still exist")
	reader.Close()
}
