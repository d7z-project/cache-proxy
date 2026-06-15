package server

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestRuntimeInitializesShardedConfigState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, nil)
	defer closeRuntime(t, rt)

	for _, objectPath := range []string{
		globalConfigPath,
		instanceIndexPath,
		revisionStatePath,
		instanceShardPath("example-files", "meta.yaml"),
		instanceShardPath("example-files", "route.yaml"),
		instanceShardPath("example-files", "source.yaml"),
		instanceShardPath("example-files", "policy.yaml"),
	} {
		reader, err := rt.store.OpenObject(ctx, systemTenant, objectPath)
		require.NoError(t, err, objectPath)
		_ = reader.Close()
	}
}

func TestInstanceCollectionCRUDAndTenantCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{})
	defer closeRuntime(t, rt)
	handler := http.HandlerFunc(rt.serveMain)

	upstream := "https://example.com"
	spec := fileSpec(t, "files", "/files", upstream)
	create := performRequest(t, handler, http.MethodPost, "/-/api/instances", mustJSONReader(t, saveInstanceRequest{
		Generation: rt.generation,
		Spec:       &spec,
	}), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, create.Code, create.Body.String())

	getResp := performRequest(t, handler, http.MethodGet, "/-/api/instances/files", nil, nil)
	require.Equal(t, http.StatusOK, getResp.Code)
	var doc instanceDocumentResponse
	require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &doc))
	require.Equal(t, "files", doc.Spec.Name)
	require.Equal(t, "/files", doc.Spec.Route.Path)

	require.NoError(t, rt.store.MkdirAll("files/file", 0o755))
	_, err := rt.store.Put(ctx, "files", "file/test.txt", strings.NewReader("cached"), map[string]string{"type": "cache"})
	require.NoError(t, err)

	deleteResp := performRequest(t, handler, http.MethodDelete, "/-/api/instances/files?generation="+strconvFormat(doc.Generation), nil, nil)
	require.Equal(t, http.StatusOK, deleteResp.Code, deleteResp.Body.String())
	_, err = rt.store.OpenObject(ctx, "files", "file/test.txt")
	require.True(t, errors.Is(err, fs.ErrNotExist))
}

func TestInstanceImportExportAPIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{})
	defer closeRuntime(t, rt)
	handler := http.HandlerFunc(rt.serveMain)

	importResp := performRequest(t, handler, http.MethodPost, "/-/api/instances/import", mustJSONReader(t, importInstancesRequest{
		Generation: rt.generation,
		Replace:    false,
		Instances: []config.InstanceSpec{
			fileSpec(t, "files", "/files", "https://example.com"),
			npmSpec(t, "npm", "/npm", "https://registry.npmjs.org"),
		},
	}), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, importResp.Code, importResp.Body.String())

	exportResp := performRequest(t, handler, http.MethodGet, "/-/api/instances/export", nil, nil)
	require.Equal(t, http.StatusOK, exportResp.Code)
	var exported exportResponse
	require.NoError(t, json.Unmarshal(exportResp.Body.Bytes(), &exported))
	require.Equal(t, DefaultMetricsPath, exported.Global.Metrics.Path)
	require.Len(t, exported.Instances, 2)
}

func TestPublicInstancesExposeResolvedEntryURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{
		"oci": {
			Name:   "oci",
			Meta:   config.InstanceMeta{Mode: config.ModeOCI, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Bind: "0.0.0.0:5000", PublicURL: "https://registry.example.com/"},
			Source: config.InstanceSource{Upstreams: []string{"https://registry-1.docker.io"}},
			Policy: mustPolicyJSON(t, map[string]any{"defaultPolicy": "revalidate", "busyPolicy": "bypass", "rules": []any{}}),
		},
		"files": fileSpec(t, "files", "/files", "https://example.com"),
	})
	defer closeRuntime(t, rt)

	rec := performRequest(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/-/api/public/instances", nil, map[string]string{
		"X-Forwarded-Proto": "https",
		"X-Forwarded-Host":  "cache.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var items []config.InstanceSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &items))
	require.Len(t, items, 2)

	require.Equal(t, "https://cache.example.com/files", items[0].EntryURL)
	require.Equal(t, "https://registry.example.com", items[1].EntryURL)
	require.Equal(t, "public_url", items[1].EntryKind)
}

func TestPathRoutesAreNormalizedBeforePersistenceAndDisplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{
		"files": fileSpec(t, "files", "/files/", "https://example.com"),
	})
	defer closeRuntime(t, rt)

	runtimeHandler := http.HandlerFunc(rt.serveMain)

	docResp := performRequest(t, runtimeHandler, http.MethodGet, "/-/api/instances/files", nil, nil)
	require.Equal(t, http.StatusOK, docResp.Code)

	var doc instanceDocumentResponse
	require.NoError(t, json.Unmarshal(docResp.Body.Bytes(), &doc))
	require.Equal(t, "/files", doc.Spec.Route.Path)

	publicResp := performRequest(t, runtimeHandler, http.MethodGet, "/-/api/public/instances", nil, map[string]string{
		"X-Forwarded-Proto": "https",
		"X-Forwarded-Host":  "cache.example.com",
	})
	require.Equal(t, http.StatusOK, publicResp.Code)

	var items []config.InstanceSummary
	require.NoError(t, json.Unmarshal(publicResp.Body.Bytes(), &items))
	require.Len(t, items, 1)
	require.Equal(t, "https://cache.example.com/files", items[0].EntryURL)
}

func TestConflictingPathIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{
		"files": fileSpec(t, "files", "/files", "https://example.com"),
	})
	defer closeRuntime(t, rt)

	_, err := rt.replaceState(ctx, rt.generation, config.CloneGlobal(rt.global), map[string]config.InstanceSpec{
		"files": fileSpec(t, "files", "/files", "https://example.com"),
		"dup":   fileSpec(t, "dup", "/files", "https://mirror.example.com"),
	}, collectChanged(map[string]config.InstanceSpec{
		"files": fileSpec(t, "files", "/files", "https://example.com"),
		"dup":   fileSpec(t, "dup", "/files", "https://mirror.example.com"),
	}), nil)
	require.ErrorContains(t, err, "conflicts")
}

func strconvFormat(value uint64) string {
	return strconv.FormatUint(value, 10)
}
