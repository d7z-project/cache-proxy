package deb

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestDebianByHashOnlyFetchesRequestedIndex(t *testing.T) {
	for _, root := range []string{"dists/stable", "nested/debian/dists/stable", "flat", "."} {
		t.Run(root, func(t *testing.T) {
			body := "requested index"
			digest := sha256.Sum256([]byte(body))
			strongest := sha512.Sum512([]byte(body))
			var release strings.Builder
			release.WriteString("Acquire-By-Hash: yes\nSHA256:\n")
			paths := []string{"main/binary-amd64/Packages.gz", "main/binary-arm64/Packages.gz", "main/Contents-all.gz", "main/i18n/Translation-fr.xz", "main/source/Sources.xz"}
			for _, name := range paths {
				fmt.Fprintf(&release, " %x %d %s\n", digest, len(body), name)
			}
			fmt.Fprintf(&release, "SHA512:\n %x %d %s\n", strongest, len(body), paths[0])
			anchor := "/" + joinRoot(root, "InRelease")
			canonical := joinRoot(root, paths[0])
			hashPath := "/" + releaseByHashPath(canonical, "SHA512", fmt.Sprintf("%x", strongest))
			var fetched atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case anchor:
					_, _ = io.WriteString(w, release.String())
				case "/" + joinRoot(root, "Release"), "/" + joinRoot(root, "Release.gpg"):
					http.NotFound(w, r)
				default:
					fetched.Add(1)
					if r.URL.Path != hashPath {
						t.Errorf("unsolicited metadata fetch: %s", r.URL.Path)
					}
					_, _ = io.WriteString(w, body)
				}
			}))
			defer server.Close()
			h := newDebianTestHandler(t, server.URL)
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, anchor, nil))
			_, err := h.metadata.Refresh(context.Background(), 10)
			require.NoError(t, err)
			require.Len(t, h.metadata.Current(root).Objects, 1)
			require.Zero(t, fetched.Load())
			for _, target := range []string{"/" + canonical, hashPath, "/" + releaseByHashPath(canonical, "SHA256", fmt.Sprintf("%x", digest))} {
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
				require.Equal(t, http.StatusOK, response.Code)
				require.Equal(t, body, response.Body.String())
			}
			require.Equal(t, int32(1), fetched.Load())
		})
	}
}

func TestDebianIndexFailuresDoNotPublishOrFallbackOnTransientErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		code   int
	}{
		{"forbidden fallback", 403, "correct", 200},
		{"missing fallback", 404, "correct", 200},
		{"rate limited", 429, "correct", 429},
		{"unavailable", 503, "correct", 503},
		{"partial upstream body", 206, "correct", 502},
		{"unsolicited not modified", 304, "", 502},
		{"truncated transport", 200, "correct", 502},
		{"encoded representation", 200, "correct", 502},
		{"wrong digest", 200, "invalid", 502},
		{"wrong size", 200, "long invalid body", 502},
	} {
		t.Run(test.name, func(t *testing.T) {
			digest := sha256.Sum256([]byte("correct"))
			release := fmt.Sprintf("Acquire-By-Hash: yes\nSHA256:\n %x 7 Packages.gz\n", digest)
			var canonicalRequests, hashRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/flat/InRelease":
					_, _ = io.WriteString(w, release)
				case strings.Contains(r.URL.Path, "/by-hash/"):
					hashRequests.Add(1)
					if test.name == "truncated transport" {
						w.Header().Set("Content-Length", "100")
					}
					if test.name == "encoded representation" {
						w.Header().Set("Content-Encoding", "gzip")
					}
					w.WriteHeader(test.status)
					_, _ = io.WriteString(w, test.body)
				case r.URL.Path == "/flat/Packages.gz":
					canonicalRequests.Add(1)
					_, _ = io.WriteString(w, "correct")
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			h := newDebianTestHandler(t, server.URL)
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/flat/InRelease", nil))
			_, err := h.metadata.Refresh(context.Background(), 10)
			require.NoError(t, err)
			for range 2 {
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/flat/Packages.gz", nil))
				require.Equal(t, test.code, response.Code)
				if test.code == 200 {
					require.Equal(t, "correct", response.Body.String())
				}
			}
			if test.code == 200 {
				require.Equal(t, int32(1), canonicalRequests.Load())
				require.Equal(t, int32(1), hashRequests.Load())
			} else {
				require.Zero(t, canonicalRequests.Load())
				require.Equal(t, int32(2), hashRequests.Load())
			}
		})
	}
}

func TestDebianIndexHTTPAndStoragePolicy(t *testing.T) {
	body := "verified index"
	digest := sha256.Sum256([]byte(body))
	target := fmt.Sprintf("/flat/by-hash/SHA256/%x", digest)
	var requests atomic.Int32
	var noStore atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", `"index"`)
		if noStore.Load() {
			w.Header().Set("Cache-Control", "no-store")
		}
		if r.Method == http.MethodGet {
			require.Empty(t, r.Header.Get("Range"))
			require.Empty(t, r.Header.Get("If-None-Match"))
		}
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	for _, test := range []struct {
		method, header, value string
		code                  int
		body                  string
	}{
		{http.MethodHead, "", "", 200, ""},
		{http.MethodGet, "Range", "bytes=0-7", 206, "verified"},
		{http.MethodHead, "", "", 200, ""},
		{http.MethodGet, "If-None-Match", `"index"`, 304, ""},
		{http.MethodGet, "Range", "bytes=999-1000", 416, "invalid range: failed to overlap\n"},
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(test.method, target, nil)
		if test.header != "" {
			request.Header.Set(test.header, test.value)
		}
		h.ServeHTTP(response, request)
		require.Equal(t, test.code, response.Code)
		require.Equal(t, test.body, response.Body.String())
	}
	require.Equal(t, int32(2), requests.Load())
	noStore.Store(true)
	for range 2 {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		request.Header.Set("Cache-Control", "no-cache")
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		require.Equal(t, 200, response.Code)
		require.Equal(t, body, response.Body.String())
	}
	require.Equal(t, int32(4), requests.Load())
	_, err := storeio.OpenResponse(context.Background(), h.store, debIndexTenant, h.indexResponseKey(strings.TrimPrefix(target, "/")))
	require.Error(t, err)
}

func TestDebianIndexHeadFallbackKeepsMethodAndLeavesGETUncached(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var canonicalRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodHead, r.Method)
				if r.URL.Path == "/flat/Packages.gz" {
					canonicalRequests.Add(1)
					w.Header().Set("Content-Length", "7")
					return
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			h := newDebianTestHandler(t, server.URL)
			descriptor := indexDescriptor{FetchPath: "flat/by-hash/SHA256/" + strings.Repeat("a", 64), FallbackPath: "flat/Packages.gz", CachePaths: []string{"test-index"}, Size: 7}
			response := httptest.NewRecorder()
			h.serveVerifiedIndex(response, httptest.NewRequest(http.MethodHead, "/flat/Packages.gz", nil), descriptor)
			if status == http.StatusForbidden || status == http.StatusNotFound {
				require.Equal(t, http.StatusOK, response.Code)
				require.Equal(t, "7", response.Header().Get("Content-Length"))
				require.Equal(t, int32(1), canonicalRequests.Load())
			} else {
				require.Equal(t, status, response.Code)
				require.Zero(t, canonicalRequests.Load())
			}
			require.Empty(t, response.Body.String())
			_, err := storeio.OpenResponse(context.Background(), h.store, debIndexTenant, h.indexResponseKey(descriptor.CachePaths[0]))
			require.Error(t, err)
		})
	}
}

func TestDebianIndexConcurrentGenerationUpdate(t *testing.T) {
	bodies := []string{"old index", "new index"}
	var revision atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := int(revision.Load())
		if r.URL.Path == "/flat/InRelease" {
			digest := sha256.Sum256([]byte(bodies[current]))
			_, _ = fmt.Fprintf(w, "Acquire-By-Hash: yes\nSHA256:\n %x %d Packages.gz\n", digest, len(bodies[current]))
			return
		}
		for i, body := range bodies {
			digest := sha256.Sum256([]byte(body))
			if r.URL.Path != fmt.Sprintf("/flat/by-hash/SHA256/%x", digest) {
				continue
			}
			downloads.Add(1)
			if i == 0 {
				once.Do(func() { close(started) })
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			_, _ = io.WriteString(w, body)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/flat/InRelease", nil))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/flat/Packages.gz", nil).WithContext(ctx))
		finished <- response
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	cancel()
	revision.Store(1)
	for range 2 {
		_, err = h.metadata.Refresh(context.Background(), 10)
		require.NoError(t, err)
	}
	newResponse := httptest.NewRecorder()
	h.ServeHTTP(newResponse, httptest.NewRequest(http.MethodGet, "/flat/Packages.gz", nil))
	require.Equal(t, bodies[1], newResponse.Body.String())
	close(release)
	select {
	case old := <-finished:
		require.Equal(t, bodies[0], old.Body.String())
	case <-time.After(5 * time.Second):
		t.Fatal("download did not finish")
	}
	for i, body := range bodies {
		target := "/flat/Packages.gz"
		if i == 0 {
			target = fmt.Sprintf("/flat/by-hash/SHA256/%x", sha256.Sum256([]byte(body)))
		}
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
		require.Equal(t, body, response.Body.String())
	}
	require.Equal(t, int32(2), downloads.Load())
}

func TestDebianIndexDescriptorAmbiguity(t *testing.T) {
	digest := strings.Repeat("a", 64)
	manifest := releaseManifest{AcquireByHash: true, Entries: []releaseEntry{
		{Path: "main/Packages.gz", Size: 7, SHA256: digest},
		{Path: "main/Packages.xz", Size: 7, SHA256: digest},
	}}
	hashPath := "flat/main/by-hash/SHA256/" + digest
	hash, found, err := manifest.resolveIndex("flat", hashPath)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, hash.FallbackPath)
	keys := make(map[string]bool)
	for _, entry := range manifest.Entries {
		descriptor, found, err := manifest.resolveIndex("flat", "flat/"+entry.Path)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "flat/"+entry.Path, descriptor.FallbackPath)
		require.NotEqual(t, hash.CachePaths[0], descriptor.CachePaths[0])
		require.False(t, keys[descriptor.CachePaths[0]])
		keys[descriptor.CachePaths[0]] = true
	}
	manifest.Entries[1].Size++
	_, _, err = manifest.resolveIndex("flat", hashPath)
	require.Error(t, err)
}

func TestDebianIndexVerifiesEveryStrongDigest(t *testing.T) {
	body := "correct"
	sha256Digest := sha256.Sum256([]byte(body))
	sha512Digest := sha512.Sum512([]byte(body))
	for _, badDigest := range []string{"sha256", "sha512"} {
		t.Run(badDigest, func(t *testing.T) {
			entry := releaseEntry{Path: "Packages.gz", Size: int64(len(body)), SHA256: fmt.Sprintf("%x", sha256Digest), SHA512: fmt.Sprintf("%x", sha512Digest)}
			if badDigest == "sha256" {
				entry.SHA256 = strings.Repeat("0", 64)
			} else {
				entry.SHA512 = strings.Repeat("0", 128)
			}
			manifest := releaseManifest{AcquireByHash: true, Entries: []releaseEntry{entry}}
			descriptor, found, err := manifest.resolveIndex("flat", "flat/Packages.gz")
			require.NoError(t, err)
			require.True(t, found)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
			defer server.Close()
			h := newDebianTestHandler(t, server.URL)
			response := httptest.NewRecorder()
			status, _ := h.serveVerifiedIndex(response, httptest.NewRequest(http.MethodGet, "/flat/Packages.gz", nil), descriptor)
			require.Equal(t, http.StatusBadGateway, status)
			_, err = storeio.OpenResponse(context.Background(), h.store, debIndexTenant, h.indexResponseKey(descriptor.CachePaths[0]))
			require.Error(t, err)
		})
	}
}

func TestDebianVerifiedIndexSurvivesPublicationFailure(t *testing.T) {
	body := "verified"
	digest := sha256.Sum256([]byte(body))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	closed, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	h.store = closed
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/flat/by-hash/SHA256/%x", digest), nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, body, response.Body.String())
}

func TestDebianInvalidCachedIndexIsReplaced(t *testing.T) {
	body := "verified"
	digest := sha256.Sum256([]byte(body))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	target := fmt.Sprintf("flat/by-hash/SHA256/%x", digest)
	wrong := sha256.Sum256([]byte("wrong"))
	require.NoError(t, storeio.PutResponse(context.Background(), h.store, debIndexTenant, h.indexResponseKey(target), h.origin.String(), 200, nil, fmt.Sprintf("%x", wrong), strings.NewReader("wrong")))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/"+target, nil).WithContext(ctx))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, body, response.Body.String())
	}
}

func TestDebianIndexSpoolBudgetRecovery(t *testing.T) {
	body := "verified"
	digest := sha256.Sum256([]byte(body))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	budget := proxyruntime.NewSpoolBudget(int64(len(body)))
	h.spooler = storeio.NewSpooler(t.TempDir(), 1024, budget)
	reservation, ok := budget.TryReserve(int64(len(body)))
	require.True(t, ok)
	defer reservation.Release()
	target := fmt.Sprintf("/flat/by-hash/SHA256/%x", digest)
	for range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusBadGateway, response.Code)
		require.NotContains(t, response.Body.String(), body)
	}
	reservation.Release()
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, body, response.Body.String())
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func TestDebianHistoricalSHA512IndexWithoutManifest(t *testing.T) {
	body := "historical"
	target := fmt.Sprintf("/flat/by-hash/SHA512/%x", sha512.Sum512([]byte(body)))
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		require.Equal(t, target, r.URL.Path)
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	for range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, body, response.Body.String())
	}
	require.Equal(t, int32(1), downloads.Load())
}

func TestDebianUncachedPreviousHashFallbackKeepsOriginalDigest(t *testing.T) {
	bodies := []string{"old index", "new index"}
	var revision, canonicalRevision, canonicalRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/flat/InRelease":
			body := bodies[revision.Load()]
			_, _ = fmt.Fprintf(w, "Acquire-By-Hash: yes\nSHA256:\n %x %d Packages.gz\n", sha256.Sum256([]byte(body)), len(body))
		case "/flat/Packages.gz":
			canonicalRequests.Add(1)
			_, _ = io.WriteString(w, bodies[canonicalRevision.Load()])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/flat/InRelease", nil))
	_, err := h.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	revision.Store(1)
	canonicalRevision.Store(1)
	for range 2 {
		_, err = h.metadata.Refresh(context.Background(), 10)
		require.NoError(t, err)
	}
	require.Zero(t, canonicalRequests.Load())
	target := fmt.Sprintf("/flat/by-hash/SHA256/%x", sha256.Sum256([]byte(bodies[0])))
	mismatch := httptest.NewRecorder()
	h.ServeHTTP(mismatch, httptest.NewRequest(http.MethodGet, target, nil))
	require.Equal(t, http.StatusBadGateway, mismatch.Code)
	require.Equal(t, int32(1), canonicalRequests.Load())
	canonicalRevision.Store(0)
	for range 2 {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, bodies[0], response.Body.String())
	}
	require.Equal(t, int32(2), canonicalRequests.Load())
}
