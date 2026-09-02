package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func TestResolveRequestClassifiesManifest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	resolved, err := resolveRequest(req, &Options{})
	require.NoError(t, err)
	require.Equal(t, requestManifest, resolved.kind)
	require.Equal(t, "library/alpine", resolved.repo)
	require.Equal(t, "latest", resolved.ref)
}

func TestValidateConfigExpandsAuthEnvironment(t *testing.T) {
	t.Setenv("OCI_TEST_USERNAME", "registry-user")
	t.Setenv("OCI_TEST_PASSWORD", "registry-password")
	options := Options{Auth: &AuthConfig{
		Type:     "basic",
		Username: "${OCI_TEST_USERNAME}",
		Password: "${OCI_TEST_PASSWORD}",
	}}

	require.NoError(t, validateConfig("https://registry.example", &options))
	require.Equal(t, "registry-user", options.Auth.Username)
	require.Equal(t, "registry-password", options.Auth.Password)
}

func TestOCIClassifiesDigestManifest(t *testing.T) {
	digest := sha256Digest("manifest")
	resolved, err := resolveRequest(httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/"+digest, nil), &Options{})
	require.NoError(t, err)
	require.Equal(t, requestManifest, resolved.kind)
	require.Equal(t, digest, resolved.ref)
}

func TestOCIReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var writes atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		writes.Add(1)
	}))
	defer upstream.Close()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: upstream.URL}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	request := httptest.NewRequest(http.MethodPatch, "/v2/library/demo/blobs/uploads/id?_state=x", strings.NewReader("chunk"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, writes.Load())
	_, err = handler.remoteRead(context.Background(), context.Background(), http.MethodPatch, "/v2/library/demo", "", "", nil)
	require.ErrorContains(t, err, "must use GET or HEAD")
	require.Zero(t, writes.Load())
}

func TestOCIPingIsServedLocally(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer upstream.Close()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: upstream.URL}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, "/v2/", nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "registry/2.0", response.Header().Get("Docker-Distribution-API-Version"))
		require.Equal(t, "LOCAL", response.Header().Get("X-Cache"))
	}
	require.Zero(t, requests.Load())
}

func TestOCIRevalidatesTagManifestWithConditionalGet(t *testing.T) {
	manifest := `{"schemaVersion":2}`
	digest := sha256Digest(manifest)
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Docker-Content-Digest", digest)
		_, _ = io.WriteString(w, manifest)
	}))
	defer upstream.Close()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: upstream.URL, MetadataTTL: time.Nanosecond}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, first.Code)
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "REVALIDATED", second.Header().Get("X-Cache"))
	require.EqualValues(t, 2, requests.Load())
}

func TestOCIRevalidationStateFailureServesCommittedManifest(t *testing.T) {
	manifest := `{"schemaVersion":2}`
	digest := sha256Digest(manifest)
	var store *blobfs.Store
	var statePath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("If-None-Match") == `"v1"` {
			if err := store.DeleteObject(context.Background(), "oci", statePath); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := store.MkdirAll("oci/"+statePath, 0o755); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Docker-Content-Digest", digest)
		_, _ = io.WriteString(w, manifest)
	}))
	defer upstream.Close()
	var err error
	store, err = blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: upstream.URL, MetadataTTL: time.Nanosecond}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	statePath = handler.refStatePath("library/alpine", "latest")

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, first.Code)
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "STALE", second.Header().Get("X-Cache"))
	require.Equal(t, manifest, second.Body.String())
}

func TestOCIBearerTokenSingleflight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tokenRequests int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenRequests, 1)
		require.Equal(t, "registry.example", r.URL.Query().Get("service"))
		require.Equal(t, "repository:library/alpine:pull", r.URL.Query().Get("scope"))
		require.Equal(t, "Basic dXNlcjpwYXNz", r.Header.Get("Authorization"))
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, `{"token":"token-`+strconv.Itoa(int(atomic.LoadInt32(&tokenRequests)))+`","expires_in":60}`)
	}))
	defer tokenServer.Close()

	handler := newHandler("oci", Block{
		Upstream: "https://registry.example",
		Options: Options{
			Auth: &AuthConfig{Type: "basic", Username: "user", Password: "pass"},
		},
	}, config.DefaultRetention, nil, metrics.NewStats(prometheus.NewRegistry()), nil)
	challenge := ociChallenge{
		scheme: "Bearer",
		realm:  tokenServer.URL + "/token",
		params: map[string]string{"service": "registry.example", "scope": "repository:library/alpine:pull"},
	}

	const workers = 16
	var wg sync.WaitGroup
	results := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := handler.ociBearerToken(ctx, challenge)
			require.NoError(t, err)
			results <- token
		}()
	}
	wg.Wait()
	close(results)
	for token := range results {
		require.Equal(t, "token-1", token)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&tokenRequests))
}

func TestOCIChallengePreservesBrowserUserAgentButTokenUsesInternalAgent(t *testing.T) {
	const browserUserAgent = "Mozilla/5.0 Firefox/127.0"
	tokenUserAgent := make(chan string, 1)
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		tokenUserAgent <- req.UserAgent()
		_, _ = io.WriteString(w, `{"token":"registry-token","expires_in":60}`)
	}))
	defer tokenServer.Close()

	var registryRequests atomic.Int32
	registryUserAgents := make(chan string, 2)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		registryUserAgents <- req.UserAgent()
		if registryRequests.Add(1) == 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+tokenServer.URL+`/token",service="registry.test",scope="registry:catalog:*"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer registry.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{
		Upstream: registry.URL,
	}, config.DefaultRetention, store, metrics.NewStats(prometheus.NewRegistry()), nil)

	req := httptest.NewRequest(http.MethodGet, "/v2/_catalog", nil)
	req.Header.Set("User-Agent", browserUserAgent)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, browserUserAgent, <-registryUserAgents)
	require.Equal(t, browserUserAgent, <-registryUserAgents)
	require.Equal(t, utils.DefaultUserAgent, <-tokenUserAgent)
}

func TestOCIChallengeReleasesHostAdmissionBeforeTokenRequest(t *testing.T) {
	var upstream *httptest.Server
	var registryRequests atomic.Int32
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/token":
			_, _ = io.WriteString(w, `{"token":"registry-token","expires_in":60}`)
		case "/v2/_catalog":
			if registryRequests.Add(1) == 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="`+upstream.URL+`/token",service="registry.test",scope="registry:catalog:*"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			require.Equal(t, "Bearer registry-token", req.Header.Get("Authorization"))
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	limiter := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{
		MaxActive: 8, MaxActivePerHost: 8,
	})
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.DefaultRetention, store, metrics.NewStats(prometheus.NewRegistry()), limiter)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/_catalog", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.EqualValues(t, 2, registryRequests.Load())
}

func TestOCIBrowserVaryResponsesAreNotPublished(t *testing.T) {
	const browserUserAgent = "Mozilla/5.0 Chrome/126.0 Safari/537.36"
	blobBody := "browser-varying-blob"
	blobDigest := sha256Digest(blobBody)
	manifestBody := `{"schemaVersion":2,"layers":[{"digest":"` + blobDigest + `"}]}`
	manifestDigest := sha256Digest(manifestBody)
	var manifestRequests atomic.Int32
	var blobRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			manifestRequests.Add(1)
			require.Equal(t, browserUserAgent, req.UserAgent())
			w.Header().Set("Vary", "User-Agent")
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = io.WriteString(w, manifestBody)
		case "/v2/library/alpine/manifests/stored":
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = io.WriteString(w, manifestBody)
		case "/v2/library/alpine/blobs/" + blobDigest:
			blobRequests.Add(1)
			require.Equal(t, browserUserAgent, req.UserAgent())
			w.Header().Set("Vary", "*")
			_, _ = io.WriteString(w, blobBody)
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.DefaultRetention, store, metrics.NewStats(prometheus.NewRegistry()), nil)

	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
		req.Header.Set("User-Agent", browserUserAgent)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	}
	_, err = store.StatObject(context.Background(), "oci", handler.refStatePath("library/alpine", "latest"))
	require.Error(t, err)
	require.Equal(t, int32(2), manifestRequests.Load())

	storedManifest := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/stored", nil)
	storedManifest.Header.Set("User-Agent", "container-client/1")
	handler.ServeHTTP(httptest.NewRecorder(), storedManifest)
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
		req.Header.Set("User-Agent", browserUserAgent)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	}
	_, err = store.StatObject(context.Background(), "oci", handler.blobPath(blobDigest))
	require.Error(t, err)
	require.Equal(t, int32(2), blobRequests.Load())
}

func TestOCIBlobCacheIsIndependentFromRefExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	blobBody := "blob-data"
	blobDigest := sha256Digest(blobBody)
	manifestBody := `{"schemaVersion":2,"config":{"digest":"` + sha256Digest("config") + `"},"layers":[{"digest":"` + blobDigest + `"}]}`
	manifestDigest := sha256Digest(manifestBody)
	var manifestRequests atomic.Int64
	var blobRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			manifestRequests.Add(1)
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = io.WriteString(w, manifestBody)
		case "/v2/library/alpine/blobs/" + blobDigest:
			blobRequests.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, blobBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, manifestBody, rec.Body.String())

	req = httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, blobBody, rec.Body.String())

	require.Equal(t, int64(1), manifestRequests.Load())
	require.Equal(t, int64(1), blobRequests.Load())

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, []string{"COALESCED", "HIT"}, rec.Header().Get("X-Cache"))
	require.Equal(t, int64(1), blobRequests.Load())

	statePath := handler.refStatePath("library/alpine", "latest")
	state, err := handler.readState(ctx, statePath)
	require.NoError(t, err)
	state.FetchedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, handler.writeState(ctx, state))

	_, err = handler.Cleanup(ctx, config.CleanupConfig{})
	require.NoError(t, err)

	reader, err := store.OpenObject(ctx, handler.name, handler.blobPath(blobDigest))
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestOCICachesDigestBlobWithoutActiveRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	t.Setenv("TMPDIR", tmpDir)

	var blobRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		blobRequests.Add(1)
		_, _ = io.WriteString(w, "blob-data")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	blobDigest := sha256Digest("blob-data")
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "MISS", rec.Header().Get("X-Cache"))
	require.Equal(t, int64(1), blobRequests.Load())

	require.Eventually(t, func() bool {
		reader, openErr := store.OpenObject(ctx, handler.name, handler.blobPath(blobDigest))
		if openErr != nil {
			return false
		}
		return reader.Close() == nil
	}, time.Second, 10*time.Millisecond)
	require.False(t, strings.Contains(rec.Body.String(), "Bad Gateway"))
	require.NoError(t, handler.Stop(ctx))
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestOCIBlobFetchClearsBusyStateOnUpstreamError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	blobDigest := sha256Digest("blob")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream failed", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))

	objectPath := handler.blobPath(blobDigest)
	_, busy := handler.downloads.Load(objectPath)
	require.False(t, busy)
}

func TestOCIManifestDigestMismatchFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Docker-Content-Digest", sha256Digest("different"))
		_, _ = io.WriteString(w, `{"schemaVersion":2}`)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusBadGateway, rec.Code)
	_, err = store.OpenObject(ctx, handler.name, handler.refStatePath("library/alpine", "latest"))
	require.Error(t, err)
}

func TestOCIManifestPersistenceFailureReturnsVerifiedResponse(t *testing.T) {
	body := `{"schemaVersion":2}`
	digest := sha256Digest(body)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Docker-Content-Digest", digest)
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	require.NoError(t, store.Close())

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	require.Equal(t, body, rec.Body.String())
}

func TestOCICachePreparationFailureBypassesForManifestAndBlob(t *testing.T) {
	manifest := `{"schemaVersion":2}`
	manifestDigest := sha256Digest(manifest)
	blob := "blob-data"
	blobDigest := sha256Digest(blob)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = io.WriteString(w, manifest)
		case "/v2/library/alpine/blobs/" + blobDigest:
			_, _ = io.WriteString(w, blob)
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: upstream.URL}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	t.Cleanup(func() { require.NoError(t, handler.Stop(context.Background())) })
	tempPath := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(tempPath, []byte("x"), 0o600))
	t.Setenv("TMPDIR", tempPath)

	for _, test := range []struct {
		path string
		body string
	}{
		{path: "/v2/library/alpine/manifests/latest", body: manifest},
		{path: "/v2/library/alpine/blobs/" + blobDigest, body: blob},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, "BYPASS", recorder.Header().Get("X-Cache"))
		require.Equal(t, test.body, recorder.Body.String())
	}
}

func TestOCIStateCommitFailureDoesNotFailUpstreamResponse(t *testing.T) {
	body := `{"schemaVersion":2,"annotations":{"version":"new"}}`
	digest := sha256Digest(body)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Docker-Content-Digest", digest)
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	statePath := handler.refStatePath("library/alpine", "latest")
	require.NoError(t, store.MkdirAll(handler.name+"/"+statePath, 0o755))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	require.Equal(t, body, rec.Body.String())
	_, err = handler.readState(context.Background(), statePath)
	require.Error(t, err)
}

func TestOCIStateRejectsUnknownFields(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: "https://registry.example.test"}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	t.Cleanup(func() { require.NoError(t, handler.Stop(context.Background())) })
	statePath := handler.refStatePath("library/alpine", "latest")
	require.NoError(t, handler.storeObject(context.Background(), statePath, strings.NewReader("version: 1\nunknown: true\n"), nil))

	_, err = handler.readState(context.Background(), statePath)
	require.ErrorContains(t, err, "field unknown not found")
}

func TestOCIStateReadIsBounded(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: "https://registry.example"}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	statePath := handler.refStatePath("library/alpine", "latest")
	require.NoError(t, handler.storeObject(ctx, statePath, bytes.NewReader(bytes.Repeat([]byte("x"), maxRefStateSize+1)), nil))
	_, err = handler.readState(ctx, statePath)
	require.ErrorContains(t, err, "exceeds")
}

func TestOCIResolverRejectsTraversalWithoutRewriting(t *testing.T) {
	for _, raw := range []string{"/v2/repo/../other/manifests/latest", "/v2/repo/%2e%2e/other/manifests/latest", "/v2/repo%2fother/manifests/latest"} {
		request := httptest.NewRequest(http.MethodGet, raw, nil)
		_, err := resolveRequest(request, &Options{})
		require.Error(t, err, raw)
	}
}

func TestOCIManifestHitUsesOnlyStateDigest(t *testing.T) {
	oldBody := `{"schemaVersion":2,"annotations":{"version":"old"}}`
	newBody := `{"schemaVersion":2,"annotations":{"version":"new"}}`
	newDigest := sha256Digest(newBody)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Docker-Content-Digest", newDigest)
		_, _ = io.WriteString(w, newBody)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	oldDigest := sha256Digest(oldBody)
	require.NoError(t, handler.storeObject(context.Background(), handler.manifestPath(oldDigest), strings.NewReader(oldBody), map[string]string{
		"docker-content-digest": oldDigest,
		userAgentReviewedOption: "true",
	}))
	require.NoError(t, handler.writeState(context.Background(), refState{
		Repo: "library/alpine", Ref: "latest", FetchedAt: time.Now(),
		ExpireAfter: config.Expiration(time.Hour), ManifestDigest: sha256Digest("missing"),
		ContentLength: int64(len(oldBody)),
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, newBody, rec.Body.String())
	require.NotEqual(t, oldBody, rec.Body.String())
}

func TestOCIBlobDigestMismatchIsNotCached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	blobDigest := sha256Digest("expected")
	manifestBody := `{"schemaVersion":2,"layers":[{"digest":"` + blobDigest + `"}]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			_, _ = io.WriteString(w, manifestBody)
		case "/v2/library/alpine/blobs/" + blobDigest:
			_, _ = io.WriteString(w, "wrong")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "wrong", rec.Body.String())
	require.Eventually(t, func() bool {
		_, err := store.OpenObject(ctx, handler.name, handler.blobPath(blobDigest))
		return err != nil
	}, time.Second, 10*time.Millisecond)
}

func TestOCIRejectsMalformedBlobDigest(t *testing.T) {
	handler := newHandler("oci", Block{
		Upstream: "https://registry.example",
	}, config.Expiration(time.Hour), nil, metrics.NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/sha256:short", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestOCIConcurrentManifestMissPublishesOneConsistentWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bodyA := `{"schemaVersion":2,"annotations":{"version":"a"}}`
	bodyB := `{"schemaVersion":2,"annotations":{"version":"b"}}`
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestNumber := requests.Add(1)
		body := bodyB
		if requestNumber == 1 {
			close(firstStarted)
			<-releaseFirst
			body = bodyA
		}
		w.Header().Set("Docker-Content-Digest", sha256Digest(body))
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
		firstDone <- rec
	}()
	<-firstStarted
	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
		secondDone <- rec
	}()
	close(releaseFirst)
	first := <-firstDone
	second := <-secondDone
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, bodyA, second.Body.String())
	require.EqualValues(t, 1, requests.Load())

	state, err := handler.readState(ctx, handler.refStatePath("library/alpine", "latest"))
	require.NoError(t, err)
	require.Equal(t, sha256Digest(bodyA), state.ManifestDigest)
	cached := httptest.NewRecorder()
	handler.ServeHTTP(cached, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, "HIT", cached.Header().Get("X-Cache"))
	require.Equal(t, bodyA, cached.Body.String())
}

func TestOCICleanupSkipsActiveRef(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: "https://registry.example"}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	state := refState{
		Repo:           "library/alpine",
		Ref:            "latest",
		FetchedAt:      time.Now().Add(-2 * time.Hour),
		ExpireAfter:    config.Expiration(time.Hour),
		ManifestDigest: sha256Digest("manifest"),
	}
	require.NoError(t, handler.writeState(ctx, state))
	statePath := handler.refStatePath(state.Repo, state.Ref)
	lock := handler.refLocks.Get(statePath)
	lock.RLock()
	_, err = handler.Cleanup(ctx, config.DefaultCleanupConfig())
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, statePath)
	require.NoError(t, err)
	lock.RUnlock()

	_, err = handler.Cleanup(ctx, config.DefaultCleanupConfig())
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, statePath)
	require.Error(t, err)
}

func TestOCICleanupBlobsWithoutRefDirectory(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: "https://registry.example"}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	body := "blob"
	objectPath := handler.blobPath(sha256Digest(body))
	require.NoError(t, handler.storeObject(ctx, objectPath, strings.NewReader(body), map[string]string{
		"fetched-at": time.Now().Add(-2 * time.Hour).Format(time.RFC3339Nano),
	}))

	_, err = handler.Cleanup(ctx, config.DefaultCleanupConfig())
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, objectPath)
	require.Error(t, err)
}

func TestOCICleanupRemovesAbandonedStateStagingObject(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: "https://registry.example"}, config.ExpirationNever, store, metrics.NewStats(prometheus.NewRegistry()), nil)
	tempPath := handler.refStatePath("library/alpine", "latest") + ".tmp.abandoned"
	require.NoError(t, handler.storeObject(ctx, tempPath, strings.NewReader("partial"), nil))

	_, err = handler.Cleanup(ctx, config.DefaultCleanupConfig())
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, tempPath)
	require.Error(t, err)
}

func TestOCICleanupCursorProgressesWithoutDeletion(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{Upstream: "https://registry.example"}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)
	for _, body := range []string{"one", "two", "three"} {
		require.NoError(t, handler.storeObject(ctx, handler.blobPath(sha256Digest(body)), strings.NewReader(body), map[string]string{
			"fetched-at": time.Now().UTC().Format(time.RFC3339Nano),
		}))
	}
	cursors := make(map[string]struct{})
	for range 3 {
		more, err := handler.Cleanup(ctx, config.CleanupConfig{BatchSize: 1})
		require.NoError(t, err)
		if handler.cleanupCursor != "" {
			cursors[handler.cleanupCursor] = struct{}{}
		}
		if !more {
			break
		}
	}
	require.Len(t, cursors, 2)
	require.Empty(t, handler.cleanupCursor)
	for _, body := range []string{"one", "two", "three"} {
		_, err := store.StatObject(ctx, handler.name, handler.blobPath(sha256Digest(body)))
		require.NoError(t, err)
	}
}

func TestOCIRejectsOversizedManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chunk := make([]byte, 32<<10)
		remaining := int64(maxManifestSize + 1)
		for remaining > 0 {
			writeSize := min(remaining, int64(len(chunk)))
			_, _ = w.Write(chunk[:writeSize])
			remaining -= writeSize
		}
	}))
	defer upstream.Close()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
	}, config.Expiration(time.Hour), store, metrics.NewStats(prometheus.NewRegistry()), nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/v2/library/alpine/manifests/latest", nil))
	require.Equal(t, http.StatusBadGateway, rec.Code)
	_, err = store.StatObject(ctx, handler.name, handler.refStatePath("library/alpine", "latest"))
	require.Error(t, err)
}

func TestOCITokenPurgeExpired(t *testing.T) {
	handler := &handler{auth: authHandler{tokens: map[string]ociToken{}}}

	now := time.Now()
	handler.auth.tokens["expired"] = ociToken{value: "tok1", expire: now.Add(-time.Hour)}
	handler.auth.tokens["valid"] = ociToken{value: "tok2", expire: now.Add(time.Hour)}
	handler.auth.tokens["just-expired"] = ociToken{value: "tok3", expire: now}

	handler.auth.tokenMu.Lock()
	handler.trimTokenCacheLocked(now, "")
	handler.auth.tokenMu.Unlock()

	require.Empty(t, handler.auth.tokens["expired"].value)
	require.Empty(t, handler.auth.tokens["just-expired"].value)
	require.Equal(t, "tok2", handler.auth.tokens["valid"].value)
	require.Len(t, handler.auth.tokens, 1)
}

func TestOCITokenCacheIsBounded(t *testing.T) {
	handler := &handler{auth: authHandler{tokens: map[string]ociToken{}}}

	now := time.Now()
	for i := 0; i < maxTokenCacheEntries+100; i++ {
		key := "scope-" + strconv.Itoa(i)
		handler.auth.tokens[key] = ociToken{
			value:  "tok-" + strconv.Itoa(i),
			expire: now.Add(time.Duration(i+1) * time.Minute),
		}
	}

	handler.auth.tokenMu.Lock()
	handler.trimTokenCacheLocked(now, "")
	handler.auth.tokenMu.Unlock()

	require.LessOrEqual(t, len(handler.auth.tokens), maxTokenCacheEntries)
	require.Empty(t, handler.auth.tokens["scope-0"].value)
	latestKey := "scope-" + strconv.Itoa(maxTokenCacheEntries+99)
	require.Equal(t, "tok-"+strconv.Itoa(maxTokenCacheEntries+99), handler.auth.tokens[latestKey].value)
}

func sha256Digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
