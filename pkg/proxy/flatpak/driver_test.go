package flatpak

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestFlatpakReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/objects/object", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestFlatpakRootDirectoriesAndUnknownResourcesRemainTransparent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, request.URL.RequestURI())
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL+"/repo")

	for range 2 {
		for target, expected := range map[string]string{
			"/":                           "/repo/",
			"/refs/":                      "/repo/refs/",
			"/assets/site.css?theme=dark": "/repo/assets/site.css?theme=dark",
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

func TestDescriptorRewriteUsesTrustedExternalBase(t *testing.T) {
	request := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/repo.flatpakrepo", nil), "https://proxy.example/flatpak")
	rewritten := rewriteDescriptor(request, []byte("[Flatpak Repo]\nUrl=https://upstream.example/repo\n"))
	require.Contains(t, string(rewritten), "Url=https://proxy.example/flatpak")
}

func TestOSTreeDigestObjectIsVerifiedBeforeCacheHit(t *testing.T) {
	body := []byte("ostree object")
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	objectPath := "/objects/" + digest[:2] + "/" + digest[2:] + ".commit"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	for index := range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, objectPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		if index == 0 {
			require.Equal(t, "MISS", response.Header().Get("X-Cache"))
		} else {
			require.Contains(t, []string{"HIT", "COALESCED"}, response.Header().Get("X-Cache"))
		}
		require.Equal(t, body, response.Body.Bytes())
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestOSTreeDigestObjectRangeMissFillsVerifiedObject(t *testing.T) {
	body := []byte("ostree object")
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	objectPath := "/objects/" + digest[:2] + "/" + digest[2:] + ".commit"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		require.Empty(t, request.Header.Get("Range"))
		_, _ = w.Write(body)
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	for byteRange, expected := range map[string]string{"bytes=2-5": "tree", "bytes=7-12": "object"} {
		request := httptest.NewRequest(http.MethodGet, objectPath, nil)
		request.Header.Set("Range", byteRange)
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		require.Equal(t, http.StatusPartialContent, response.Code)
		require.Equal(t, expected, response.Body.String())
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestOSTreeDetachedMetadataCachesNotFound(t *testing.T) {
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte("commit")))
	objectPath := "/objects/" + digest[:2] + "/" + digest[2:] + ".commitmeta"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		http.NotFound(w, request)
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	for _, method := range []string{http.MethodGet, http.MethodGet, http.MethodHead} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(method, objectPath, nil))
		require.Equal(t, http.StatusNotFound, response.Code)
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestOSTreeDeltaIndexUsesFiniteResponseCache(t *testing.T) {
	const deltaIndexPath = "/delta-indexes/_1/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuqg.index"
	for name, status := range map[string]int{"present": http.StatusOK, "absent": http.StatusNotFound} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				require.Equal(t, deltaIndexPath, request.URL.Path)
				w.WriteHeader(status)
				_, _ = w.Write([]byte("delta index"))
			}))
			defer server.Close()
			h := newFlatpakTestHandler(t, server.URL)

			for index := range 2 {
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, deltaIndexPath, nil))
				require.Equal(t, status, response.Code)
				if index == 0 {
					require.Equal(t, "MISS", response.Header().Get("X-Cache"))
				} else {
					require.Contains(t, []string{"HIT", "COALESCED"}, response.Header().Get("X-Cache"))
				}
			}
			require.Equal(t, int32(1), requests.Load())
		})
	}
}

func TestOSTreeDeltaIndexPathValidation(t *testing.T) {
	for _, valid := range []string{
		"delta-indexes/_1/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuqg.index",
		"delta-indexes/bR/OmHnSqM64s6kWW1eNz3P1wOG+zACOInQ2ghDY0BC8.index",
		"delta-indexes/AA/" + strings.Repeat("A", 41) + ".index",
	} {
		require.True(t, isDeltaIndexPath(valid), valid)
	}
	for _, invalid := range []string{
		"delta-indexes/_1/short.index",
		"delta-indexes/_1/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuqg",
		"delta-indexes/_1/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuq!.index",
		"delta-indexes/AA/" + strings.Repeat("A", 40) + "-.index",
		"delta-indexes/_1/nested/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuqg.index",
	} {
		require.False(t, isDeltaIndexPath(invalid), invalid)
	}
}

func TestFlatpakIndexedSummaryDeltaPathValidation(t *testing.T) {
	oldDigest := strings.Repeat("0", 64)
	newDigest := strings.Repeat("f", 64)
	valid := "summaries/" + oldDigest + "-" + newDigest + ".delta"
	require.True(t, isIndexedSummaryDeltaPath(valid))
	require.True(t, isDeltaPath(valid))
	_, metadata := metadataAnchorPath(valid)
	require.False(t, metadata)

	for _, invalid := range []string{
		"summaries/" + strings.Repeat("0", 63) + "-" + newDigest + ".delta",
		"summaries/" + oldDigest + newDigest + ".delta",
		"summaries/" + oldDigest + "-" + strings.Repeat("F", 64) + ".delta",
		"summaries/" + oldDigest + "-" + newDigest + ".delta/extra",
		"summaries/nested/" + oldDigest + "-" + newDigest + ".delta",
	} {
		require.False(t, isIndexedSummaryDeltaPath(invalid), invalid)
	}
}

func TestFlatpakDescriptorUsesDownstreamRepresentationCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"source"`)
		_, _ = w.Write([]byte("[Flatpak Repo]\nUrl=https://upstream.invalid/repo\nFuture=kept\n"))
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	for _, result := range []string{"MISS", "HIT"} {
		request := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/repo.flatpakrepo", nil), "https://proxy.example/flatpak")
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, result, response.Header().Get("X-Cache"))
		require.NotEqual(t, `"source"`, response.Header().Get("ETag"))
		require.Contains(t, response.Body.String(), "Url=https://proxy.example/flatpak")
		require.Contains(t, response.Body.String(), "Future=kept")
	}
}

func TestFlatpakMetadataUsesIndependentGenerations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/summary":
			_, _ = w.Write([]byte("summary"))
		case "/summary.sig":
			_, _ = w.Write([]byte("signature"))
		case "/config":
			_, _ = w.Write([]byte("config"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	for _, anchor := range []string{"/summary", "/config"} {
		first := httptest.NewRecorder()
		h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, anchor, nil))
		require.Equal(t, http.StatusOK, first.Code)
		_, err := h.metadata.Refresh(context.Background(), 10)
		require.NoError(t, err)
	}
	for requestPath, body := range map[string]string{"/summary": "summary", "/summary.sig": "signature", "/config": "config"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, body, response.Body.String())
	}
}

func TestFlatpakIndexedSummaryPublishesVerifiedClosure(t *testing.T) {
	index, compressedSummary := flatpakIndexedSummaryFixture(t)
	const digest = "156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		switch request.URL.Path {
		case "/summary.idx":
			_, _ = w.Write(index)
		case "/summaries/" + digest + ".gz":
			_, _ = w.Write(compressedSummary)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/summary.idx", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, index, response.Body.Bytes())
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)

	for requestPath, expected := range map[string][]byte{
		"/summary.idx":                 index,
		"/summaries/" + digest + ".gz": compressedSummary,
	} {
		response = httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, expected, response.Body.Bytes())
	}
	for _, requestPath := range []string{"/summary.idx.sig", "/summaries/" + fmt.Sprintf("%x", sha256.Sum256(index)) + ".idx.sig"} {
		response = httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusNotFound, response.Code)
	}
	require.Equal(t, int32(4), requests.Load())
}

func TestFlatpakIndexedSummaryDeltaRemainsMutableAfterGenerationPublish(t *testing.T) {
	index, compressedSummary := flatpakIndexedSummaryFixture(t)
	const summaryDigest = "156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e"
	deltaPath := "/summaries/" + strings.Repeat("0", 64) + "-" + summaryDigest + ".delta"
	deltaBody := []byte("opaque indexed summary delta")
	var deltaAvailable atomic.Bool
	var deltaRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/summary.idx":
			_, _ = w.Write(index)
		case "/summaries/" + summaryDigest + ".gz":
			_, _ = w.Write(compressedSummary)
		case deltaPath:
			deltaRequests.Add(1)
			if !deltaAvailable.Load() {
				http.NotFound(w, request)
				return
			}
			w.Header().Set("X-Upstream-Resource", "indexed-summary-delta")
			_, _ = w.Write(deltaBody)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newFlatpakTestHandler(t, server.URL)

	anchor := httptest.NewRecorder()
	h.ServeHTTP(anchor, httptest.NewRequest(http.MethodGet, "/summary.idx", nil))
	require.Equal(t, http.StatusOK, anchor.Code)
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)

	missing := httptest.NewRecorder()
	h.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, deltaPath, nil))
	require.Equal(t, http.StatusNotFound, missing.Code)
	require.Equal(t, "BYPASS", missing.Header().Get("X-Cache"))

	deltaAvailable.Store(true)
	for index, expectedCache := range []string{"MISS", "HIT"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, deltaPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		if index == 0 {
			require.Equal(t, expectedCache, response.Header().Get("X-Cache"))
		} else {
			require.Contains(t, []string{expectedCache, "COALESCED"}, response.Header().Get("X-Cache"))
		}
		require.Equal(t, "indexed-summary-delta", response.Header().Get("X-Upstream-Resource"))
		require.Equal(t, deltaBody, response.Body.Bytes())
	}
	require.Equal(t, int32(2), deltaRequests.Load())
}

func TestFlatpakSummaryPublishesUpstreamGenerationUpdate(t *testing.T) {
	var revision atomic.Int32
	revision.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := revision.Load()
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current))
		switch request.URL.Path {
		case "/summary":
			_, _ = fmt.Fprintf(w, "summary-v%d", current)
		case "/summary.sig":
			_, _ = fmt.Fprintf(w, "signature-v%d", current)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/summary", nil))
	require.Equal(t, "summary-v1", first.Body.String())
	_, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := h.metadata.Current("flatpak:summary")
	require.NotNil(t, previous)

	revision.Store(2)
	more, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := h.metadata.Current("flatpak:summary")
	require.NotNil(t, current)
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	for target, expected := range map[string]string{"/summary": "summary-v2", "/summary.sig": "signature-v2"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, expected, response.Body.String())
	}
}

func TestFlatpakSummaryIndexPublishesUpstreamGenerationUpdate(t *testing.T) {
	initialIndex, initialSummary := flatpakIndexedSummaryFixture(t)
	parsedInitial, err := readSummaryIndex(bytes.NewReader(initialIndex), int64(len(initialIndex)))
	require.NoError(t, err)
	newSummaryBody := []byte("updated indexed summary")
	var newSummary bytes.Buffer
	writer := gzip.NewWriter(&newSummary)
	_, err = writer.Write(newSummaryBody)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	newSummaryDigest := sha256.Sum256(newSummaryBody)
	oldSummaryDigest, err := hex.DecodeString(parsedInitial.subsummaryDigests[0])
	require.NoError(t, err)
	updatedIndex := bytes.Clone(initialIndex)
	digestOffset := bytes.Index(updatedIndex, oldSummaryDigest)
	require.NotEqual(t, -1, digestOffset)
	copy(updatedIndex[digestOffset:digestOffset+sha256.Size], newSummaryDigest[:])
	parsedUpdated, err := readSummaryIndex(bytes.NewReader(updatedIndex), int64(len(updatedIndex)))
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%x", newSummaryDigest), parsedUpdated.subsummaryDigests[0])
	indexes := [][]byte{initialIndex, updatedIndex}
	summaries := [][]byte{initialSummary, newSummary.Bytes()}
	digests := []string{parsedInitial.subsummaryDigests[0], parsedUpdated.subsummaryDigests[0]}
	indexDigests := []string{parsedInitial.digest, parsedUpdated.digest}
	var revision atomic.Int32
	revision.Store(1)
	var oldObjectRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := int(revision.Load() - 1)
		switch request.URL.Path {
		case "/summary.idx":
			w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current+1))
			_, _ = w.Write(indexes[current])
		case "/summaries/" + digests[current] + ".gz":
			if current == 0 {
				oldObjectRequests.Add(1)
			}
			_, _ = w.Write(summaries[current])
		case "/summaries/" + indexDigests[current] + ".idx.sig":
			if current == 0 {
				oldObjectRequests.Add(1)
			}
			_, _ = io.WriteString(w, fmt.Sprintf("signature-%d", current+1))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/summary.idx", nil))
	require.Equal(t, initialIndex, first.Body.Bytes())
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := h.metadata.Current("flatpak:summary.idx")
	require.NotNil(t, previous)

	revision.Store(2)
	more, err := h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	_, err = h.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := h.metadata.Current("flatpak:summary.idx")
	require.NotNil(t, current)
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/summary.idx", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "HIT", response.Header().Get("X-Cache"))
	require.Equal(t, updatedIndex, response.Body.Bytes())

	for requestPath, expected := range map[string][]byte{
		"/summaries/" + digests[0] + ".gz":           initialSummary,
		"/summaries/" + indexDigests[0] + ".idx.sig": []byte("signature-1"),
	} {
		response = httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code, requestPath)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"), requestPath)
		require.Equal(t, expected, response.Body.Bytes(), requestPath)
	}
	require.Equal(t, int32(2), oldObjectRequests.Load())
}

func TestFlatpakIndexedSummaryRejectsInvalidSubsummary(t *testing.T) {
	index, _ := flatpakIndexedSummaryFixture(t)
	const digest = "156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e"
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write([]byte("valid gzip with the wrong digest"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	for name, body := range map[string][]byte{
		"invalid gzip":    []byte("not gzip"),
		"digest mismatch": compressed.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/summary.idx":
					_, _ = w.Write(index)
				case "/summaries/" + digest + ".gz":
					_, _ = w.Write(body)
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()
			h := newFlatpakTestHandler(t, server.URL)

			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/summary.idx", nil))
			require.Equal(t, http.StatusOK, response.Code)
			_, err := h.metadata.Refresh(context.Background(), 10)
			require.Error(t, err)
			require.Nil(t, h.metadata.Current("flatpak:summary.idx"))
		})
	}
}

func TestFlatpakIndexedSummaryCompanionCannotDiscoverRepository(t *testing.T) {
	const digest = "156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/summaries/"+digest+".gz", request.URL.Path)
		_, _ = w.Write([]byte("direct companion"))
	}))
	defer server.Close()
	h := newFlatpakTestHandler(t, server.URL)

	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/summaries/"+digest+".gz", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "direct companion", response.Body.String())
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.Nil(t, h.metadata.Current("flatpak:summary.idx"))
}

func TestFlatpakIndexedSummaryRestoresOffline(t *testing.T) {
	index, compressedSummary := flatpakIndexedSummaryFixture(t)
	const digest = "156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/summary.idx":
			_, _ = w.Write(index)
		case "/summaries/" + digest + ".gz":
			_, _ = w.Write(compressedSummary)
		default:
			http.NotFound(w, request)
		}
	}))

	root := t.TempDir()
	storePath := root + "/store"
	statePath := root + "/state"
	workPath := root + "/work"
	openHandler := func(rawOrigin string) (*handler, *blobfs.Store) {
		store, err := blobfs.Open(storePath, blobfs.DefaultConfig())
		require.NoError(t, err)
		origin, err := url.Parse(rawOrigin)
		require.NoError(t, err)
		stats := metrics.NewStats(prometheus.NewRegistry())
		client, err := transport.NewClient("flatpak", "flatpak", nil, nil, stats)
		require.NoError(t, err)
		h, err := newHandler("flatpak", statePath, origin, workPath, store, client, nil)
		require.NoError(t, err)
		return h, store
	}

	h, store := openHandler(server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/summary.idx", nil))
	require.Equal(t, http.StatusOK, response.Code)
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.NoError(t, h.CloseContext(context.Background()))
	require.NoError(t, store.Close())
	server.Close()

	h, store = openHandler(server.URL)
	defer func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	}()
	for requestPath, expected := range map[string][]byte{
		"/summary.idx":                 index,
		"/summaries/" + digest + ".gz": compressedSummary,
	} {
		response = httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, expected, response.Body.Bytes())
	}
}

func flatpakIndexedSummaryFixture(t *testing.T) ([]byte, []byte) {
	t.Helper()
	index, err := base64.StdEncoding.DecodeString("eDg2XzY0AAAVbP0Wwl8G7AU97WocH1TpOfNjZz2j9N7vypLh13MGXiAgBysAAAAAb3N0cmVlLnN1bW1hcnkubW9kZQAAAAAAYXJjaGl2ZS16MgAAcxQAAG9zdHJlZS5zdW1tYXJ5LnRvbWJzdG9uZS1jb21taXRzAAAAAAAAAAAAAGIhAAAAAG9zdHJlZS5zdW1tYXJ5LmluZGV4ZWQtZGVsdGFzAAAAAQBiHgAAAABvc3RyZWUuc3VtbWFyeS5sYXN0LW1vZGlmaWVkAAAAAAAAAABqmDRAAHQdAAAAAAB4YS5jYWNoZS12ZXJzaW9uAAAAAAAAAAACAAAAAHURJlR8q88sAA==")
	require.NoError(t, err)
	compressed, err := base64.StdEncoding.DecodeString("H4sIAAAAAAAAA0ssKNDPL0rXS61IzC3ISdVzNXLVr7Awizcz0S8uSUzKSWWAABsmCJ0rvEyuZJXxOp1XrtOuPi6+87fAIn8zg3LHXN4FLWYmLPoViXopiSWJUF0sUJphX7RjQUFOZnJiSWZ+XixXXmJuqi2atVxFpXklmWjiATmJJWn5RbmobuIqTslGURacko2mIjk/NzcxL8U21SiVi4FBo6SkWJMD5JD8Er2SYqijUicxNDCUsM3ZrKHEkFhQUFxSlJoIswnm8HVQukxq1ppt585/XPfeJFC73n7Oum8vblvKhVxLMmSvjJLbbI/qbyaY9j6Y3VjsVTDWEETYa4Rq8TYofUJsbs+RF54OJ498KWPb9EptwWFGi1sL3AVXHq45YhOlyo3D3nL89goxQANbn3BgQ/T6QI3+H9ArY7Ln6i11qQpHlYLL33lMb7XyCvx3ufU3LmrNs10rcLgnLToIYiGWuIdZS90EgBzp2ON94moNbQYOxnrG74wMzBWJesWlubmJRZW6ZalFxZn5eRC1jCCiVFiegxkAFKOgxCoDAAA=")
	require.NoError(t, err)
	return index, compressed
}

func newFlatpakTestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("flatpak", "flatpak", nil, nil, stats)
	require.NoError(t, err)
	root := t.TempDir()
	h, err := newHandler("flatpak", root+"/state", origin, root+"/work", store, client, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
