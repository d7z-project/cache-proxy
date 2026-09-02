package cargo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestCratePrefix(t *testing.T) {
	require.Equal(t, "1", cratePrefix("a"))
	require.Equal(t, "2", cratePrefix("ab"))
	require.Equal(t, "3/a", cratePrefix("abc"))
	require.Equal(t, "ab/cd", cratePrefix("abcde"))
}

func TestCargoCacheabilityHonorsNoStore(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/config.json", nil)
	response := &http.Response{Header: http.Header{"Cache-Control": {"no-store"}}}
	require.False(t, cargoCacheable(request, response))
}

func TestCargoReadOnlyProtocolSurface(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "/git-upload-pack", request.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	h := newCargoTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/git-upload-pack", nil))
	require.Equal(t, http.StatusNoContent, response.Code)

	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/v1/crates/new", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/git-receive-pack", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-receive-pack", nil))
	require.Equal(t, http.StatusForbidden, response.Code)
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack&service=git-receive-pack", nil))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Equal(t, int32(1), requests.Load())
}

func TestSparseConfigAndVerifiedCrateDownload(t *testing.T) {
	crateBody := []byte("crate archive")
	digest := sha256.Sum256(crateBody)
	var crateRequests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/config.json":
			_ = json.NewEncoder(w).Encode(map[string]any{"dl": server.URL + "/api/v1/crates", "future": true})
		case "/3/f/foo":
			_, _ = w.Write([]byte(`{"name":"foo","vers":"1.0.0","cksum":"` + hex.EncodeToString(digest[:]) + `","future":true}` + "\n"))
		case "/api/v1/crates/foo/1.0.0/download":
			crateRequests.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(crateBody)))
			if request.Method == http.MethodHead {
				return
			}
			if request.Header.Get("Range") == "bytes=0-3" {
				w.Header().Set("Content-Length", "4")
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-3/%d", len(crateBody)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(crateBody[:4])
				return
			}
			_, _ = w.Write(crateBody)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	h := newCargoTestHandler(t, server.URL)

	configRequest := proxyruntime.WithExternalBaseURL(httptest.NewRequest(http.MethodGet, "/config.json", nil), "https://proxy.example/cargo")
	configResponse := httptest.NewRecorder()
	h.ServeHTTP(configResponse, configRequest)
	require.Equal(t, http.StatusOK, configResponse.Code)
	var configDocument map[string]any
	require.NoError(t, json.Unmarshal(configResponse.Body.Bytes(), &configDocument))
	require.Equal(t, true, configDocument["future"])
	require.Equal(t, "https://proxy.example/cargo/-/crate/{crate}/{version}/download", configDocument["dl"])

	indexResponse := httptest.NewRecorder()
	h.ServeHTTP(indexResponse, httptest.NewRequest(http.MethodGet, "/3/f/foo", nil))
	require.Equal(t, http.StatusOK, indexResponse.Code)
	head := httptest.NewRecorder()
	h.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/-/crate/foo/1.0.0/download", nil))
	require.Equal(t, http.StatusOK, head.Code)
	partialRequest := httptest.NewRequest(http.MethodGet, "/-/crate/foo/1.0.0/download", nil)
	partialRequest.Header.Set("Range", "bytes=0-3")
	partial := httptest.NewRecorder()
	h.ServeHTTP(partial, partialRequest)
	require.Equal(t, http.StatusPartialContent, partial.Code)
	require.Equal(t, crateBody[:4], partial.Body.Bytes())
	for index := range 2 {
		crateResponse := httptest.NewRecorder()
		h.ServeHTTP(crateResponse, httptest.NewRequest(http.MethodGet, "/-/crate/foo/1.0.0/download", nil))
		require.Equal(t, http.StatusOK, crateResponse.Code)
		if index == 0 {
			require.Equal(t, "MISS", crateResponse.Header().Get("X-Cache"))
		} else {
			require.Contains(t, []string{"HIT", "COALESCED"}, crateResponse.Header().Get("X-Cache"))
		}
		require.Equal(t, crateBody, crateResponse.Body.Bytes())
	}
	require.Equal(t, int32(3), crateRequests.Load())
}

func newCargoTestHandler(t *testing.T, rawOrigin string) *handler {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	origin, err := url.Parse(rawOrigin)
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("cargo", "cargo", nil, nil, stats)
	require.NoError(t, err)
	h, err := newHandler(origin, root+"/state", root+"/work", store, client)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return h
}
