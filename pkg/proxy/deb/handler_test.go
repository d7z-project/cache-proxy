package deb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func newDebianTestHandler(t *testing.T, upstream string) *handler {
	t.Helper()
	root := t.TempDir()
	store, err := blobfs.Open(root+"/blobs", blobfs.DefaultConfig())
	require.NoError(t, err)
	stats := metrics.NewStats(prometheus.NewRegistry())
	client, err := transport.NewClient("deb-test", "deb", nil, nil, stats)
	require.NoError(t, err)
	handler, err := newHandler("deb-test", upstream, root+"/state", root+"/work", store, client, stats, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, handler.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return handler
}

func TestDebianReadOnlyBoundaryDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	h := newDebianTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/pool/main/demo.deb", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, requests.Load())
}

func TestDebianFirstAnchorPassesThroughThenPublishesFullClosure(t *testing.T) {
	packages := []byte("Package: demo\nFilename: pool/main/d/demo_1_amd64.deb\n")
	contents := []byte("usr/bin/demo main/demo\n")
	packagesDigest := sha256.Sum256(packages)
	contentsDigest := sha256.Sum256(contents)
	release := fmt.Sprintf("SHA256:\n %x %d main/binary-amd64/Packages\n %x %d main/Contents-amd64\n", packagesDigest, len(packages), contentsDigest, len(contents))
	var metadataRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/binary-amd64/Packages":
			metadataRequests.Add(1)
			_, _ = w.Write(packages)
		case "/dists/trixie/main/Contents-amd64":
			metadataRequests.Add(1)
			_, _ = w.Write(contents)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, release, first.Body.String())

	more, err := handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, int32(2), metadataRequests.Load())

	for requestPath, expected := range map[string][]byte{
		"/dists/trixie/main/binary-amd64/Packages": packages,
		"/dists/trixie/main/Contents-amd64":        contents,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, expected, response.Body.Bytes())
		require.Equal(t, "HIT", response.Header().Get("X-Cache"))
	}
	require.Equal(t, int32(2), metadataRequests.Load())
}

func TestDebianPublishesUpstreamGenerationUpdate(t *testing.T) {
	for name, paths := range map[string]struct {
		anchor   string
		metadata string
		root     string
	}{
		"standard": {anchor: "/dists/trixie/InRelease", metadata: "/dists/trixie/main/Packages", root: "dists/trixie"},
		"flat":     {anchor: "/flat/Release", metadata: "/flat/Packages", root: "flat"},
	} {
		t.Run(name, func(t *testing.T) {
			packages := [][]byte{[]byte("Package: demo\nVersion: 1\n"), []byte("Package: demo\nVersion: 2\n")}
			releases := make([]string, len(packages))
			for i, body := range packages {
				digest := sha256.Sum256(body)
				relative := strings.TrimPrefix(paths.metadata, "/"+paths.root+"/")
				releases[i] = fmt.Sprintf("SHA256:\n %x %d %s\n", digest, len(body), relative)
			}
			var revision atomic.Int32
			revision.Store(1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				current := int(revision.Load() - 1)
				w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current+1))
				switch request.URL.Path {
				case paths.anchor:
					_, _ = io.WriteString(w, releases[current])
				case paths.metadata:
					_, _ = w.Write(packages[current])
				default:
					http.NotFound(w, request)
				}
			}))
			t.Cleanup(server.Close)
			h := newDebianTestHandler(t, server.URL)

			first := httptest.NewRecorder()
			h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, paths.anchor, nil))
			require.Equal(t, releases[0], first.Body.String())
			_, err := h.metadata.Refresh(context.Background(), 1)
			require.NoError(t, err)
			previous := h.metadata.Current(paths.root)
			require.NotNil(t, previous)

			revision.Store(2)
			more, err := h.metadata.Refresh(context.Background(), 1)
			require.NoError(t, err)
			require.True(t, more)
			_, err = h.metadata.Refresh(context.Background(), 1)
			require.NoError(t, err)
			current := h.metadata.Current(paths.root)
			require.NotNil(t, current)
			require.NotEqual(t, previous.CandidateID, current.CandidateID)
			for target, expected := range map[string][]byte{paths.anchor: []byte(releases[1]), paths.metadata: packages[1]} {
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
				require.Equal(t, http.StatusOK, response.Code)
				require.Equal(t, "HIT", response.Header().Get("X-Cache"))
				require.Equal(t, expected, response.Body.Bytes())
			}
		})
	}
}

func TestDebianRetainsPreviousAcquireByHashObject(t *testing.T) {
	packages := [][]byte{[]byte("Package: demo\nVersion: 1\n"), []byte("Package: demo\nVersion: 2\n")}
	releases := make([]string, len(packages))
	byHashPaths := make([]string, len(packages))
	for index, body := range packages {
		digest := sha256.Sum256(body)
		releases[index] = fmt.Sprintf("Acquire-By-Hash: yes\nSHA256:\n %x %d main/binary-amd64/Packages\n", digest, len(body))
		byHashPaths[index] = fmt.Sprintf("/dists/trixie/main/binary-amd64/by-hash/SHA256/%x", digest)
	}
	var revision atomic.Int32
	revision.Store(1)
	var oldObjectRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := int(revision.Load() - 1)
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, current+1))
			_, _ = io.WriteString(w, releases[current])
		case byHashPaths[current]:
			if current == 0 {
				oldObjectRequests.Add(1)
			}
			_, _ = w.Write(packages[current])
		case "/dists/trixie/main/binary-amd64/Packages":
			_, _ = w.Write(packages[current])
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	h := newDebianTestHandler(t, server.URL)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
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
	h.ServeHTTP(old, httptest.NewRequest(http.MethodGet, byHashPaths[0], nil))
	require.Equal(t, http.StatusOK, old.Code)
	require.Equal(t, "HIT", old.Header().Get("X-Cache"))
	require.Equal(t, packages[0], old.Body.Bytes())
	require.Equal(t, int32(1), oldObjectRequests.Load())

	canonical := httptest.NewRecorder()
	h.ServeHTTP(canonical, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/binary-amd64/Packages", nil))
	require.Equal(t, http.StatusOK, canonical.Code)
	require.Equal(t, packages[1], canonical.Body.Bytes())
}

func TestDebianConcurrentFirstAnchorUsesOneStreamingTransfer(t *testing.T) {
	release := "SHA256:\n " + strings.Repeat("a", 64) + " 0 main/Packages\n"
	started := make(chan struct{})
	continueBody := make(chan struct{})
	var once sync.Once
	var continueOnce sync.Once
	t.Cleanup(func() { continueOnce.Do(func() { close(continueBody) }) })
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/dists/trixie/InRelease" {
			http.NotFound(w, request)
			return
		}
		requests.Add(1)
		midpoint := len(release) / 2
		_, _ = io.WriteString(w, release[:midpoint])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		once.Do(func() { close(started) })
		<-continueBody
		_, _ = io.WriteString(w, release[midpoint:])
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)

	responses := make(chan *httptest.ResponseRecorder, 2)
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
		responses <- response
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first anchor transfer did not start")
	}
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
		responses <- response
	}()
	time.Sleep(20 * time.Millisecond)
	continueOnce.Do(func() { close(continueBody) })
	for range 2 {
		select {
		case response := <-responses:
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, release, response.Body.String())
		case <-time.After(time.Second):
			t.Fatal("anchor request did not finish")
		}
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestDebianRetriesMismatchedCandidateBeforePublishing(t *testing.T) {
	expected := []byte("expected")
	digest := sha256.Sum256(expected)
	release := fmt.Sprintf("SHA256:\n %x %d main/Packages\n", digest, len(expected))
	var corrected atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Packages":
			if corrected.Load() {
				_, _ = w.Write(expected)
				return
			}
			_, _ = io.WriteString(w, "mismatch")
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.Error(t, err)
	require.Nil(t, handler.metadata.Current("dists/trixie"))

	corrected.Store(true)
	require.NoError(t, handler.metadata.StageAnchor(context.Background(), "dists/trixie", "dists/trixie/InRelease", nil, bytes.NewReader([]byte(release))))
	_, err = handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.NotNil(t, handler.metadata.Current("dists/trixie"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Packages", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "HIT", response.Header().Get("X-Cache"))
	require.Equal(t, expected, response.Body.Bytes())
}

func TestDebianUnavailableArchitectureDoesNotBlockGeneration(t *testing.T) {
	amd64Packages := []byte("Package: demo\nArchitecture: all\n")
	arm64Packages := amd64Packages
	amd64Digest := sha256.Sum256(amd64Packages)
	arm64Digest := sha256.Sum256(arm64Packages)
	release := fmt.Sprintf("Architectures: amd64 arm64\nAcquire-By-Hash: yes\nSHA256:\n %x %d main/binary-amd64/Packages.gz\n %x %d main/binary-arm64/Packages.gz\n", amd64Digest, len(amd64Packages), arm64Digest, len(arm64Packages))

	for _, unavailableStatus := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(unavailableStatus), func(t *testing.T) {
			var armStatus atomic.Int32
			armStatus.Store(int32(unavailableStatus))
			var armRequests atomic.Int32
			var armByHashRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/dists/trixie/InRelease":
					_, _ = io.WriteString(w, release)
				case "/dists/trixie/main/binary-amd64/Packages.gz":
					_, _ = w.Write(amd64Packages)
				case "/dists/trixie/main/binary-arm64/Packages.gz":
					armRequests.Add(1)
					if status := int(armStatus.Load()); status != http.StatusOK {
						w.WriteHeader(status)
						return
					}
					_, _ = w.Write(arm64Packages)
				default:
					if strings.Contains(request.URL.Path, "/main/binary-arm64/by-hash/SHA256/") {
						armByHashRequests.Add(1)
					}
					http.NotFound(w, request)
				}
			}))
			t.Cleanup(server.Close)
			handler := newDebianTestHandler(t, server.URL)
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
			_, err := handler.metadata.Refresh(context.Background(), 10)
			require.NoError(t, err)
			current := handler.metadata.Current("dists/trixie")
			require.NotNil(t, current)

			amd64Response := httptest.NewRecorder()
			handler.ServeHTTP(amd64Response, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/binary-amd64/Packages.gz", nil))
			require.Equal(t, http.StatusOK, amd64Response.Code)
			require.Equal(t, "HIT", amd64Response.Header().Get("X-Cache"))
			require.Equal(t, amd64Packages, amd64Response.Body.Bytes())

			arm64Path := "/dists/trixie/main/binary-arm64/Packages.gz"
			unavailable := httptest.NewRecorder()
			handler.ServeHTTP(unavailable, httptest.NewRequest(http.MethodGet, arm64Path, nil))
			require.Equal(t, unavailableStatus, unavailable.Code)
			require.Equal(t, "BYPASS", unavailable.Header().Get("X-Cache"))
			require.Empty(t, unavailable.Header().Get("Retry-After"))

			armStatus.Store(http.StatusOK)
			for range 2 {
				available := httptest.NewRecorder()
				handler.ServeHTTP(available, httptest.NewRequest(http.MethodGet, arm64Path, nil))
				require.Equal(t, http.StatusOK, available.Code)
				require.Equal(t, "BYPASS", available.Header().Get("X-Cache"))
				require.Equal(t, arm64Packages, available.Body.Bytes())
			}
			require.Equal(t, int32(4), armRequests.Load())
			require.Equal(t, int32(1), armByHashRequests.Load())
		})
	}
}

func TestDebianUnavailableCanonicalDoesNotUsePreviousGeneration(t *testing.T) {
	firstPackages := []byte("Package: first\n")
	secondPackages := []byte("Package: second\n")
	firstDigest := sha256.Sum256(firstPackages)
	secondDigest := sha256.Sum256(secondPackages)
	firstRelease := fmt.Sprintf("SHA256:\n %x %d main/Packages\n", firstDigest, len(firstPackages))
	secondRelease := fmt.Sprintf("SHA256:\n %x %d main/Packages\n", secondDigest, len(secondPackages))
	var revision atomic.Int32
	revision.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			if revision.Load() == 1 {
				_, _ = io.WriteString(w, firstRelease)
			} else {
				_, _ = io.WriteString(w, secondRelease)
			}
		case "/dists/trixie/main/Packages":
			if revision.Load() == 1 {
				_, _ = w.Write(firstPackages)
			} else {
				http.NotFound(w, request)
			}
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := handler.metadata.Current("dists/trixie")
	require.NotNil(t, previous)

	revision.Store(2)
	require.NoError(t, handler.metadata.StageAnchor(context.Background(), "dists/trixie", "dists/trixie/InRelease", nil, bytes.NewReader([]byte(secondRelease))))
	_, err = handler.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := handler.metadata.Current("dists/trixie")
	require.NotNil(t, current)
	require.NotEqual(t, previous.CandidateID, current.CandidateID)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Packages", nil))
	require.Equal(t, http.StatusNotFound, response.Code)
	require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
	require.NotContains(t, response.Body.String(), "first")
}

func TestDebianConcurrentUnavailableMetadataPassesThroughDuringPoll(t *testing.T) {
	packages := []byte("Package: arm64\n")
	digest := sha256.Sum256(packages)
	release := fmt.Sprintf("SHA256:\n %x %d main/binary-arm64/Packages.gz\n", digest, len(packages))
	var available atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/binary-arm64/Packages.gz":
			if !available.Load() {
				http.NotFound(w, request)
				return
			}
			_, _ = w.Write(packages)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 1)
	require.NoError(t, err)

	missingPath := "/dists/trixie/main/binary-arm64/Packages.gz"
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, missingPath, nil))
	available.Store(true)
	const workers = 16
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, missingPath, nil))
			responses <- response
		}()
	}
	refreshResult := make(chan error, 1)
	go func() {
		<-start
		_, refreshErr := handler.metadata.Refresh(context.Background(), 1)
		refreshResult <- refreshErr
	}()
	close(start)
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent metadata requests did not finish")
	}
	require.NoError(t, <-refreshResult)
	close(responses)
	for response := range responses {
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
		require.Equal(t, packages, response.Body.Bytes())
	}
}

func TestDebianDualAnchorMismatchPreservesCurrent(t *testing.T) {
	firstPackages := []byte("Package: first\n")
	secondPackages := []byte("Package: second\n")
	firstDigest := sha256.Sum256(firstPackages)
	secondDigest := sha256.Sum256(secondPackages)
	firstRelease := fmt.Sprintf("Suite: stable\nSHA256:\n %x %d main/Packages\n", firstDigest, len(firstPackages))
	secondRelease := fmt.Sprintf("Suite: testing\nSHA256:\n %x %d main/Packages\n", secondDigest, len(secondPackages))
	var changed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			if changed.Load() {
				_, _ = io.WriteString(w, secondRelease)
			} else {
				_, _ = io.WriteString(w, firstRelease)
			}
		case "/dists/trixie/Release":
			if changed.Load() {
				_, _ = io.WriteString(w, firstRelease)
			} else {
				http.NotFound(w, request)
			}
		case "/dists/trixie/main/Packages":
			if changed.Load() {
				_, _ = w.Write(secondPackages)
			} else {
				_, _ = w.Write(firstPackages)
			}
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	current := handler.metadata.Current("dists/trixie")
	require.NotNil(t, current)

	changed.Store(true)
	_, err = handler.metadata.Refresh(context.Background(), 10)
	require.Error(t, err)
	require.Equal(t, current.CandidateID, handler.metadata.Current("dists/trixie").CandidateID)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Packages", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, firstPackages, response.Body.Bytes())
}

func TestDebianPublishesMatchingDualAnchorsAndSignature(t *testing.T) {
	packages := []byte("Package: demo\n")
	digest := sha256.Sum256(packages)
	release := fmt.Sprintf("Origin: Debian\nAcquire-By-Hash: yes\nSHA256:\n %x %d main/Packages\n", digest, len(packages))
	inRelease := "-----BEGIN PGP SIGNED MESSAGE-----\nHash: SHA256\n\n" + release + "-----BEGIN PGP SIGNATURE-----\nsignature\n-----END PGP SIGNATURE-----\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, inRelease)
		case "/dists/trixie/Release":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/Release.gpg":
			_, _ = io.WriteString(w, "detached-signature")
		case "/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", digest):
			_, _ = w.Write(packages)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)

	for requestPath, expected := range map[string]string{
		"/dists/trixie/InRelease":   inRelease,
		"/dists/trixie/Release":     release,
		"/dists/trixie/Release.gpg": "detached-signature",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, expected, response.Body.String())
	}
}

func TestDebianByHashFallsBackToVerifiedCanonicalIndex(t *testing.T) {
	packages := []byte("Package: canonical\n")
	digest := sha256.Sum256(packages)
	byHashPath := "/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", digest)
	release := fmt.Sprintf("Acquire-By-Hash: yes\nSHA256:\n %x %d main/Packages\n", digest, len(packages))
	var byHashRequests, canonicalRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case byHashPath:
			byHashRequests.Add(1)
			http.NotFound(w, request)
		case "/dists/trixie/main/Packages":
			canonicalRequests.Add(1)
			_, _ = w.Write(packages)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, int32(1), byHashRequests.Load())
	require.Equal(t, int32(1), canonicalRequests.Load())

	for _, requestPath := range []string{"/dists/trixie/main/Packages", byHashPath} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, packages, response.Body.Bytes())
	}
}

func TestDebianByHashCanonicalFallbackMustMatchRelease(t *testing.T) {
	expected := []byte("expected")
	digest := sha256.Sum256(expected)
	release := fmt.Sprintf("Acquire-By-Hash: yes\nSHA256:\n %x %d main/Packages\n", digest, len(expected))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Packages":
			_, _ = io.WriteString(w, "incorrect")
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.Error(t, err)
	require.Nil(t, handler.metadata.Current("dists/trixie"))
}

func TestDebianSHA512MismatchDoesNotPublish(t *testing.T) {
	expected := []byte("expected")
	digest := sha512.Sum512(expected)
	release := fmt.Sprintf("SHA512:\n %x %d main/Packages\n", digest, len(expected))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Packages":
			_, _ = io.WriteString(w, "mismatch")
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 1)
	require.ErrorContains(t, err, "sha512 mismatch")
	require.Nil(t, handler.metadata.Current("dists/trixie"))
}

func TestDebianCurrentMissPassesThroughUpstream(t *testing.T) {
	packages := []byte("Package: demo\n")
	digest := sha256.Sum256(packages)
	release := fmt.Sprintf("SHA256:\n %x %d main/Packages\n", digest, len(packages))
	var missingRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/Release":
			http.NotFound(w, request)
		case "/dists/trixie/main/Packages":
			_, _ = w.Write(packages)
		default:
			missingRequests.Add(1)
			_, _ = io.WriteString(w, "transparent metadata")
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)
	missingRequests.Store(0)

	for range 2 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
		require.Empty(t, response.Header().Get("Retry-After"))
		require.Equal(t, "transparent metadata", response.Body.String())
	}
	require.Equal(t, int32(2), missingRequests.Load())
}

func TestDebianAnchorlessFlatRepositoryRemainsTransparent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/flat/Packages", request.URL.Path)
		_, _ = io.WriteString(w, "flat metadata")
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/flat/./Packages", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "flat metadata", response.Body.String())
}

func TestDebianRootDirectoriesAndAuxiliaryFilesRemainTransparent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, request.URL.RequestURI())
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)

	for _, test := range []struct {
		name     string
		target   string
		expected string
		empty    bool
	}{
		{name: "empty mount path", target: "/", expected: "/", empty: true},
		{name: "root", target: "/", expected: "/"},
		{name: "directory", target: "/images/", expected: "/images/"},
		{name: "HTML", target: "/index.html", expected: "/index.html"},
		{name: "CSS with escaped plus", target: "/assets/site%2Btheme.css", expected: "/assets/site+theme.css"},
		{name: "unknown extension", target: "/files/catalog.data", expected: "/files/catalog.data"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.target, nil)
			if test.empty {
				request.URL.Path = ""
				request.URL.RawPath = ""
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, test.expected, response.Body.String())
			require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
		})
	}

	require.False(t, isAnchorPath("dists/trixie/Release/"))
	require.False(t, isMetadataPath("dists/trixie/main/Packages/"))
	require.False(t, isArtifactPath("pool/main/demo.deb/"))
}

func TestDebianFirstAnchorPassesThroughWhenSpoolCannotStart(t *testing.T) {
	release := "SHA256:\n " + fmt.Sprintf("%x", sha256.Sum256(nil)) + " 0 main/Packages\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, release)
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	invalidWorkDir := filepath.Join(t.TempDir(), "work")
	require.NoError(t, os.WriteFile(invalidWorkDir, []byte("not a directory"), 0o600))
	handler.spooler = storeio.NewSpooler(invalidWorkDir, int64(len(release)), nil)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, release, response.Body.String())
	require.Equal(t, "BYPASS", response.Header().Get("X-Cache"))
}

func TestDebianArtifactCacheIsIndependentFromMetadataGeneration(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "variant=one", request.URL.RawQuery)
		requests.Add(1)
		w.Header().Set("ETag", `"package-v1"`)
		_, _ = io.WriteString(w, "package-body")
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/pool/main/d/demo.deb?variant=one", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "package-body", first.Body.String())

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/pool/main/d/demo.deb?variant=one", nil))
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "package-body", second.Body.String())
	require.Contains(t, []string{"HIT", "COALESCED"}, second.Header().Get("X-Cache"))
	require.Equal(t, int32(1), requests.Load())

	ranged := httptest.NewRecorder()
	rangeRequest := httptest.NewRequest(http.MethodGet, "/pool/main/d/demo.deb?variant=one", nil)
	rangeRequest.Header.Set("Range", "bytes=0-6")
	handler.ServeHTTP(ranged, rangeRequest)
	require.Equal(t, http.StatusPartialContent, ranged.Code)
	require.Equal(t, "package", ranged.Body.String())
	require.Equal(t, int32(1), requests.Load())
}

func TestDebianArtifactAcceptsAPTPercentEncodingAndSharesCache(t *testing.T) {
	const upstreamPath = "/pool/main/f/frr/frr_10.6.1-1+pve3_amd64.deb"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, upstreamPath, request.URL.Path)
		requests.Add(1)
		_, _ = io.WriteString(w, "pve-package")
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)

	for index, requestPath := range []string{
		"/pool/main/f/frr/frr_10.6.1-1%2bpve3_amd64.deb",
		upstreamPath,
		"/pool/main/f/frr/frr_10.6.1-1%2Bpve3_amd64.deb",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, "pve-package", response.Body.String())
		if index > 0 {
			require.Contains(t, []string{"HIT", "COALESCED"}, response.Header().Get("X-Cache"))
		}
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestDebianNestedRootKeepsByHashCompressionVariantsDistinct(t *testing.T) {
	gzBody := []byte("gzip-wire-representation")
	xzBody := []byte("xz-wire-representation")
	gzDigest := sha256.Sum256(gzBody)
	xzDigest := sha256.Sum256(xzBody)
	release := fmt.Sprintf("Acquire-By-Hash: yes\nSHA256:\n %x %d main/Contents-amd64.gz\n %x %d main/Contents-amd64.xz\n", gzDigest, len(gzBody), xzDigest, len(xzBody))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/mirror/debian/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/mirror/debian/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", gzDigest):
			_, _ = w.Write(gzBody)
		case "/mirror/debian/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", xzDigest):
			_, _ = w.Write(xzBody)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL+"/mirror")

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/debian/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.NoError(t, err)

	for requestPath, expected := range map[string][]byte{
		"/debian/dists/trixie/main/Contents-amd64.gz":                             gzBody,
		"/debian/dists/trixie/main/Contents-amd64.xz":                             xzBody,
		"/debian/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", gzDigest): gzBody,
		"/debian/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", xzDigest): xzBody,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, expected, response.Body.Bytes())
	}
}

func TestDebianRejectsGenerationWhenOneCompressedRepresentationMismatches(t *testing.T) {
	gzBody := []byte("gzip-wire-representation")
	xzBody := []byte("xz-wire-representation")
	gzDigest := sha256.Sum256(gzBody)
	xzDigest := sha256.Sum256(xzBody)
	release := fmt.Sprintf("SHA256:\n %x %d main/Contents-amd64.gz\n %x %d main/Contents-amd64.xz\n", gzDigest, len(gzBody), xzDigest, len(xzBody))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Contents-amd64.gz":
			_, _ = w.Write(gzBody)
		case "/dists/trixie/main/Contents-amd64.xz":
			_, _ = io.WriteString(w, "wrong representation")
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	handler := newDebianTestHandler(t, server.URL)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dists/trixie/InRelease", nil))
	_, err := handler.metadata.Refresh(context.Background(), 10)
	require.Error(t, err)
	require.Nil(t, handler.metadata.Current("dists/trixie"))
}
