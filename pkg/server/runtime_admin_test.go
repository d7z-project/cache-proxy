package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestAdminConfigAPIExposesCurrentGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/config", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "generation")
	require.Contains(t, rec.Body.String(), `"blob":"24h0m0s"`)
}

func TestAdminValidateAPIReportsRuntimeConfigErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)
	cfg := DefaultConfig()
	cfg.Instances = map[string]config.InstanceConfig{
		"bad": {Mode: config.ModeOCI, Listen: config.ListenConfig{Path: "/registry"}, Upstreams: []string{"https://registry-1.docker.io"}, OCI: &config.OCIConfig{}},
	}
	body := strings.NewReader(mustJSON(t, cfg))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/-/api/config/validate", body).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "independent listen bind")
}

func TestInstancesImportExportAPIHandlesSuccessAndConflicts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files"},
		Upstreams: []string{"https://example.com"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}})
	defer closeRuntime(t, rt)

	exportRec := httptest.NewRecorder()
	exportReq := httptest.NewRequest(http.MethodGet, "/-/api/instances/export?name=files", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(exportRec, exportReq)
	require.Equal(t, http.StatusOK, exportRec.Code)
	var exported instancesExportResponse
	require.NoError(t, json.NewDecoder(exportRec.Body).Decode(&exported))
	require.Contains(t, exported.Instances, "files")

	importBody := strings.NewReader(mustJSON(t, instancesImportRequest{Generation: exported.Generation, Instances: map[string]config.InstanceConfig{"files2": {
		Mode:      config.ModeFile,
		Listen:    config.ListenConfig{Path: "/files2"},
		Upstreams: []string{"https://example.com"},
		Cache:     config.CacheConfig{DefaultPolicy: config.PolicyBypass},
	}}}))
	importRec := httptest.NewRecorder()
	importReq := httptest.NewRequest(http.MethodPost, "/-/api/instances/import", importBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(importRec, importReq)
	require.Equal(t, http.StatusOK, importRec.Code)

	conflictBody := strings.NewReader(mustJSON(t, instancesImportRequest{Generation: rt.generation, Instances: exported.Instances}))
	conflictRec := httptest.NewRecorder()
	conflictReq := httptest.NewRequest(http.MethodPost, "/-/api/instances/import", conflictBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(conflictRec, conflictReq)
	require.Equal(t, http.StatusConflict, conflictRec.Code)

	emptyBody := strings.NewReader(`{"generation":` + strconv.FormatUint(rt.generation, 10) + `,"instances":{}}`)
	emptyRec := httptest.NewRecorder()
	emptyReq := httptest.NewRequest(http.MethodPost, "/-/api/instances/import", emptyBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(emptyRec, emptyReq)
	require.Equal(t, http.StatusBadRequest, emptyRec.Code)
}

func TestImportRejectsMaskedCredentialsForNewInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)

	importBody := strings.NewReader(mustJSON(t, instancesImportRequest{
		Generation: rt.generation,
		Instances: map[string]config.InstanceConfig{"new-oci": {
			Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL},
			OCI: &config.OCIConfig{Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "***"}},
		}},
	}))
	importRec := httptest.NewRecorder()
	importReq := httptest.NewRequest(http.MethodPost, "/-/api/instances/import", importBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(importRec, importReq)
	require.Equal(t, http.StatusBadRequest, importRec.Code)
	require.Contains(t, importRec.Body.String(), "masked credentials")
}

func TestImportAllowsMaskedCredentialsForExistingInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{"https://registry.example"},
		OCI: &config.OCIConfig{Rules: []config.OCICacheRule{{Match: "library/*", Policy: config.PolicyImmutable}}, Auth: &config.OCIAuthConfig{Type: "basic", Username: "user", Password: "real-password"}},
	}})
	defer closeRuntime(t, rt)

	exportRec := httptest.NewRecorder()
	exportReq := httptest.NewRequest(http.MethodGet, "/-/api/instances/export?name=oci", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(exportRec, exportReq)
	require.Equal(t, http.StatusOK, exportRec.Code)
	var exported instancesExportResponse
	require.NoError(t, json.NewDecoder(exportRec.Body).Decode(&exported))
	require.Equal(t, "***", exported.Instances["oci"].OCI.Auth.Password)

	importBody := strings.NewReader(mustJSON(t, instancesImportRequest{
		Generation: rt.generation,
		Replace:    true,
		Instances:  exported.Instances,
	}))
	importRec := httptest.NewRecorder()
	importReq := httptest.NewRequest(http.MethodPost, "/-/api/instances/import", importBody).WithContext(ctx)
	rt.mainHandler.ServeHTTP(importRec, importReq)
	require.Equal(t, http.StatusOK, importRec.Code)

	rt.mu.RLock()
	require.Equal(t, "real-password", rt.config.Instances["oci"].OCI.Auth.Password)
	rt.mu.RUnlock()
}

func TestCacheLookupAPIReturnsPolicyAndCacheStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var getCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&getCount, 1)
		body := []byte("data")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"files": {
		Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{upstream.URL},
		Cache: config.CacheConfig{
			DefaultPolicy: config.PolicyBypass,
			FreshFor:      config.Duration(5 * time.Minute),
			Rules: []config.CacheRule{
				{Match: "**/*.bin", Policy: config.PolicyImmutable},
				{Match: "**/*.txt", Policy: config.PolicyRevalidate, FreshFor: config.Duration(10 * time.Second)},
			},
		},
		ExpireAfter: config.Duration(time.Hour),
	}})
	defer closeRuntime(t, rt)

	// lookup before caching
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=files&path=a.bin", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var result cacheLookupResult
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	require.Equal(t, "files", result.Instance)
	require.Equal(t, "file", result.Mode)
	require.Equal(t, "immutable", result.Policy)
	require.False(t, result.Cached)

	// fetch to populate cache
	require.Equal(t, "data", requestBody(t, rt.mainHandler, http.MethodGet, "/files/a.bin", ""))

	// lookup after caching
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=files&path=a.bin", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	require.True(t, result.Cached)
	require.True(t, result.Fresh)
	require.NotEmpty(t, result.CachedAt)
	require.NotEmpty(t, result.ExpiresAt)

	// lookup non-existent instance
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=nonexistent&path=a.txt", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// lookup missing params
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=files", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCacheLookupAPIWithOCIGlobRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Docker-Content-Digest", "sha256:abc")
		_, _ = w.Write([]byte("layer"))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"oci": {
		Mode: config.ModeOCI, Listen: config.ListenConfig{Bind: freeLocalAddr(t)}, Upstreams: []string{upstream.URL},
		OCI: &config.OCIConfig{
			DefaultPolicy: config.PolicyRevalidate,
			Rules: []config.OCICacheRule{
				{Match: "library/*", Policy: config.PolicyImmutable, FreshFor: config.Duration(5 * time.Minute), ExpireAfter: config.Duration(24 * time.Hour)},
			},
		},
	}})
	defer closeRuntime(t, rt)

	// lookup OCI manifest using repo:tag format
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=oci&path=library/alpine:latest", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var result cacheLookupResult
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	require.Equal(t, "oci", result.Instance)
	require.Equal(t, "oci", result.Mode)
	require.Equal(t, "immutable", result.Policy)
	require.Equal(t, "5m0s", result.FreshFor)
	require.Equal(t, "24h0m0s", result.ExpireAfter)
	require.False(t, result.Cached)

	// lookup OCI tags using repo-only format
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=oci&path=library/alpine", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	require.Equal(t, "immutable", result.Policy)
}

func TestCacheLookupAPIReturnsCorrectRouteForNPM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceConfig{"npm": {
		Mode: config.ModeNPM, Listen: config.ListenConfig{Path: "/npm"}, Upstreams: []string{upstream.URL},
		NPM: &config.NPMConfig{
			DefaultPolicy: config.PolicyRevalidate,
			Rules: []config.NPMCacheRule{
				{Match: "@angular/*", Policy: config.PolicyImmutable, FreshFor: config.Duration(10 * time.Minute)},
			},
		},
	}})
	defer closeRuntime(t, rt)

	// lookup npm metadata
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/-/api/cache/lookup?instance=npm&path=@angular/core", nil).WithContext(ctx)
	rt.mainHandler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var result cacheLookupResult
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))
	require.Equal(t, "npm", result.Instance)
	require.Equal(t, "npm", result.Mode)
	require.Equal(t, "immutable", result.Policy)
	require.Equal(t, "10m0s", result.FreshFor)
	require.False(t, result.Cached)
}
