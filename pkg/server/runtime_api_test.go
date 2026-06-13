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

func TestGlobalConfigAndImportExportAPIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{})
	defer closeRuntime(t, rt)
	handler := http.HandlerFunc(rt.serveMain)

	globalResp := performRequest(t, handler, http.MethodGet, "/-/api/global-config", nil, nil)
	require.Equal(t, http.StatusOK, globalResp.Code)
	var globalDoc globalConfigResponse
	require.NoError(t, json.Unmarshal(globalResp.Body.Bytes(), &globalDoc))
	globalDoc.Config.Metrics.Path = "/metrics-alt"

	saveGlobal := performRequest(t, handler, http.MethodPut, "/-/api/global-config", mustJSONReader(t, saveGlobalRequest{
		Generation: globalDoc.Generation,
		Config:     globalDoc.Config,
	}), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, saveGlobal.Code, saveGlobal.Body.String())
	require.NoError(t, json.Unmarshal(saveGlobal.Body.Bytes(), &globalDoc))

	importResp := performRequest(t, handler, http.MethodPost, "/-/api/instances/import", mustJSONReader(t, importInstancesRequest{
		Generation: globalDoc.Generation,
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
	require.Equal(t, "/metrics-alt", exported.Global.Metrics.Path)
	require.Len(t, exported.Instances, 2)
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
