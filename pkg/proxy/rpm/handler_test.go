package rpm

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

func TestRPMReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	h := newRPMTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, "/repo/pkg.rpm", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestRPMRepomdPublishesCompleteArbitraryDataClosure(t *testing.T) {
	metadata := []byte("future metadata")
	digest := sha256.Sum256(metadata)
	repomd := fmt.Sprintf(`<repomd><data type="future-extension"><checksum type="sha256">%x</checksum><location href="repodata/future.bin"/><size>%d</size></data></repomd>`, digest, len(metadata))
	var metadataRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repo/repodata/repomd.xml":
			_, _ = w.Write([]byte(repomd))
		case "/repo/repodata/future.bin":
			metadataRequests.Add(1)
			_, _ = w.Write(metadata)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)

	anchor := httptest.NewRecorder()
	h.ServeHTTP(anchor, httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	require.Equal(t, http.StatusOK, anchor.Code)
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, int32(1), metadataRequests.Load())

	for range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/repo/repodata/future.bin", nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, metadata, response.Body.Bytes())
	}
	require.Equal(t, int32(1), metadataRequests.Load())
}

func TestRPMRepomdPublishesUpstreamGenerationUpdate(t *testing.T) {
	metadata := [][]byte{[]byte("metadata-v1"), []byte("metadata-v2")}
	repomd := make([]string, len(metadata))
	for i, body := range metadata {
		digest := sha256.Sum256(body)
		repomd[i] = fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%x</checksum><location href="repodata/primary.xml"/><size>%d</size></data></repomd>`, digest, len(body))
	}
	var revision atomic.Int32
	revision.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := int(revision.Load() - 1)
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current+1))
		switch request.URL.Path {
		case "/repo/repodata/repomd.xml":
			_, _ = io.WriteString(w, repomd[current])
		case "/repo/repodata/primary.xml":
			_, _ = w.Write(metadata[current])
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	require.Equal(t, repomd[0], first.Body.String())
	_, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := h.metadata.Current("repo")
	require.NotNil(t, previous)

	revision.Store(2)
	more, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := h.metadata.Current("repo")
	require.NotNil(t, current)
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	for target, expected := range map[string][]byte{
		"/repo/repodata/repomd.xml":  []byte(repomd[1]),
		"/repo/repodata/primary.xml": metadata[1],
	} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, expected, response.Body.Bytes())
	}
}

func TestRPMValidatesOpenChecksumAndSize(t *testing.T) {
	primary := []byte(`<metadata><package><location href="Packages/demo.rpm"/></package></metadata>`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write(primary)
	require.NoError(t, writer.Close())
	wireDigest := sha256.Sum256(compressed.Bytes())
	openDigest := sha256.Sum256(primary)
	repomd := fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%x</checksum><open-checksum type="sha256">%x</open-checksum><location href="repodata/primary.xml.gz"/><size>%d</size><open-size>%d</open-size></data></repomd>`, wireDigest, openDigest, compressed.Len(), len(primary))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/repo/repodata/repomd.xml" {
			_, _ = w.Write([]byte(repomd))
			return
		}
		_, _ = w.Write(compressed.Bytes())
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
}

func TestRPMOpenValidationHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := inspectOpenMetadataReader(ctx, strings.NewReader("metadata"), 8, repomdItem{Location: "repodata/primary.xml", Size: -1, OpenSize: -1})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRPMDoesNotPublishFailedClosure(t *testing.T) {
	metadata := []byte("expected")
	digest := sha256.Sum256(metadata)
	repomd := fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%x</checksum><location href="repodata/primary.xml"/><size>%d</size></data></repomd>`, digest, len(metadata))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/repo/repodata/repomd.xml" {
			_, _ = w.Write([]byte(repomd))
			return
		}
		_, _ = w.Write([]byte("wrong"))
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.Error(t, err)
	require.Nil(t, h.metadata.Current("repo"))
}

func TestRPMArtifactSidecarCachesWithoutMetadataGeneration(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "download=1", request.URL.RawQuery)
		requests.Add(1)
		_, _ = w.Write([]byte("signature"))
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)

	for index := range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/Packages/demo.rpm.asc?download=1", nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "signature", response.Body.String())
		if index == 1 {
			require.Contains(t, []string{"HIT", "COALESCED"}, response.Header().Get("X-Cache"))
		}
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestRPMRootDirectoriesAndAuxiliaryFilesRemainTransparent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, request.URL.RequestURI())
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL+"/repository")

	for range 2 {
		for target, expected := range map[string]string{
			"/":                               "/repository/",
			"/repodata/":                      "/repository/repodata/",
			"/assets/site.css?theme=contrast": "/repository/assets/site.css?theme=contrast",
		} {
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
			require.Equal(t, expected, response.Body.String())
		}
	}
	require.Equal(t, int32(6), requests.Load())
}

func TestRPMAuxiliaryRepodataRemainsTransparentWithCurrentGeneration(t *testing.T) {
	metadata := []byte("primary metadata")
	digest := sha256.Sum256(metadata)
	repomd := fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%x</checksum><location href="repodata/primary.xml"/><size>%d</size></data></repomd>`, digest, len(metadata))
	var auxiliaryRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repo/repodata/repomd.xml":
			_, _ = io.WriteString(w, repomd)
		case "/repo/repodata/primary.xml":
			_, _ = w.Write(metadata)
		case "/repo/repodata/repomd.xml.key":
			auxiliaryRequests.Add(1)
			w.Header().Set("X-Upstream-Resource", "repository-key")
			_, _ = io.WriteString(w, "public key")
		case "/repo/repodata/vendor-resource.json":
			auxiliaryRequests.Add(1)
			w.Header().Set("X-Upstream-Resource", "vendor-extension")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"enabled":true}`)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)

	currentMetadata := httptest.NewRecorder()
	h.ServeHTTP(currentMetadata, httptest.NewRequest(http.MethodGet, "/repo/repodata/primary.xml", nil))
	require.Equal(t, http.StatusOK, currentMetadata.Code)
	require.Equal(t, "HIT", currentMetadata.Header().Get("X-Cache"))
	require.Equal(t, metadata, currentMetadata.Body.Bytes())

	for target, expected := range map[string]struct {
		status   int
		resource string
		body     string
	}{
		"/repo/repodata/repomd.xml.key":       {status: http.StatusOK, resource: "repository-key", body: "public key"},
		"/repo/repodata/vendor-resource.json": {status: http.StatusAccepted, resource: "vendor-extension", body: `{"enabled":true}`},
	} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, expected.status, response.Code)
		require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
		require.Equal(t, expected.resource, response.Header().Get("X-Upstream-Resource"))
		require.Equal(t, expected.body, response.Body.String())
	}
	require.Equal(t, int32(2), auxiliaryRequests.Load())
}

func newRPMTestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("rpm", "rpm", nil, nil, stats)
	require.NoError(t, err)
	h, err := newHandler("rpm", origin, root+"/state", root+"/work", store, client, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
