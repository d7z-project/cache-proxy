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

func TestRPMRepomdPublishesAvailableMetadata(t *testing.T) {
	for _, unavailableStatus := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(unavailableStatus), func(t *testing.T) {
			primary := []byte("primary metadata")
			primaryDigest := sha256.Sum256(primary)
			missing := []byte("future metadata")
			missingDigest := sha256.Sum256(missing)
			repomd := fmt.Sprintf(`<repomd>
<data type="primary"><checksum type="sha256">%x</checksum><location href="repodata/primary.xml"/><size>%d</size></data>
<data type="future-extension"><checksum type="sha256">%x</checksum><location href="repodata/future.bin"/><size>%d</size></data>
</repomd>`, primaryDigest, len(primary), missingDigest, len(missing))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/repo/repodata/repomd.xml":
					_, _ = io.WriteString(w, repomd)
				case "/repo/repodata/primary.xml":
					_, _ = w.Write(primary)
				case "/repo/repodata/future.bin":
					w.WriteHeader(unavailableStatus)
				default:
					http.NotFound(w, request)
				}
			}))
			t.Cleanup(server.Close)
			h := newRPMTestHandler(t, server.URL)

			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
			_, err := h.metadata.Refresh(context.Background(), 10)
			require.NoError(t, err)
			require.NotNil(t, h.metadata.Current("repo"))

			available := httptest.NewRecorder()
			h.ServeHTTP(available, httptest.NewRequest(http.MethodGet, "/repo/repodata/primary.xml", nil))
			require.Equal(t, http.StatusOK, available.Code)
			require.Equal(t, "HIT", available.Header().Get("X-Cache"))
			require.Equal(t, primary, available.Body.Bytes())

			absent := httptest.NewRecorder()
			h.ServeHTTP(absent, httptest.NewRequest(http.MethodGet, "/repo/repodata/future.bin", nil))
			require.Equal(t, unavailableStatus, absent.Code)
			require.Equal(t, "BYPASS", absent.Header().Get("X-Cache"))
			require.Empty(t, absent.Header().Get("Retry-After"))
		})
	}
}

func TestRPMMetadataRecoveryRebuildsUnchangedGeneration(t *testing.T) {
	metadata := []byte("metadata")
	digest := sha256.Sum256(metadata)
	repomd := fmt.Sprintf(`<repomd><data type="future-extension"><checksum type="sha256">%x</checksum><location href="repodata/future.bin"/><size>%d</size></data></repomd>`, digest, len(metadata))
	var available atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repo/repodata/repomd.xml":
			w.Header().Set("ETag", `"stable"`)
			_, _ = io.WriteString(w, repomd)
		case "/repo/repodata/future.bin":
			if !available.Load() {
				http.NotFound(w, request)
				return
			}
			_, _ = w.Write(metadata)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	initial := h.metadata.Current("repo")
	require.NotNil(t, initial)

	missing := httptest.NewRecorder()
	h.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/repo/repodata/future.bin", nil))
	require.Equal(t, http.StatusNotFound, missing.Code)
	require.Equal(t, "BYPASS", missing.Header().Get("X-Cache"))

	available.Store(true)
	recovered := httptest.NewRecorder()
	h.ServeHTTP(recovered, httptest.NewRequest(http.MethodGet, "/repo/repodata/future.bin", nil))
	require.Equal(t, http.StatusOK, recovered.Code)
	require.Equal(t, "BYPASS", recovered.Header().Get("X-Cache"))
	require.Equal(t, metadata, recovered.Body.Bytes())

	more, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := h.metadata.Current("repo")
	require.NotNil(t, current)
	require.Equal(t, initial.Generation, current.Generation)
	require.NotEqual(t, initial.CandidateID, current.CandidateID)

	cached := httptest.NewRecorder()
	h.ServeHTTP(cached, httptest.NewRequest(http.MethodGet, "/repo/repodata/future.bin", nil))
	require.Equal(t, http.StatusOK, cached.Code)
	require.Equal(t, "HIT", cached.Header().Get("X-Cache"))
	require.Equal(t, metadata, cached.Body.Bytes())
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

func TestRPMGenerationDoesNotMixEarlierMetadata(t *testing.T) {
	metadata := [][]byte{[]byte("primary-v1"), []byte("primary-v2")}
	locations := []string{"repodata/primary-a.xml", "repodata/primary-b.xml"}
	repomd := make([]string, len(metadata))
	for index, body := range metadata {
		digest := sha256.Sum256(body)
		repomd[index] = fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%x</checksum><location href="%s"/><size>%d</size></data></repomd>`, digest, locations[index], len(body))
	}
	var revision atomic.Int32
	revision.Store(1)
	var oldObjectRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := int(revision.Load() - 1)
		switch request.URL.Path {
		case "/repo/repodata/repomd.xml":
			w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current+1))
			_, _ = io.WriteString(w, repomd[current])
		case "/repo/" + locations[current]:
			if current == 0 {
				oldObjectRequests.Add(1)
			}
			_, _ = w.Write(metadata[current])
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newRPMTestHandler(t, server.URL)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/repo/repodata/repomd.xml", nil))
	_, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int32(1), oldObjectRequests.Load())
	revision.Store(2)
	more, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)

	old := httptest.NewRecorder()
	h.ServeHTTP(old, httptest.NewRequest(http.MethodGet, "/repo/"+locations[0], nil))
	require.Equal(t, http.StatusNotFound, old.Code)
	require.Equal(t, "BYPASS", old.Header().Get("X-Cache"))
	require.Equal(t, int32(1), oldObjectRequests.Load())
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
