package deb

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func TestParsePackagesBuildsCleanupPaths(t *testing.T) {
	paths := &filerepo.PathIndexBuilder{}
	t.Cleanup(func() { require.NoError(t, paths.Close()) })
	count, err := parsePackages(strings.NewReader("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n"), paths, 0)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "pool/main/h/hello/hello_1.0_amd64.deb", paths.Finalize()[0])
}

func TestParseSourcesBuildsCleanupPaths(t *testing.T) {
	paths := &filerepo.PathIndexBuilder{}
	t.Cleanup(func() { require.NoError(t, paths.Close()) })
	count, err := parseSources(strings.NewReader("Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc111 123 hello_1.0.dsc\n def222 456 hello_1.0.orig.tar.xz\n\n"), paths, 0)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	final := paths.Finalize()
	require.Contains(t, final, "pool/main/h/hello/hello_1.0.dsc")
	require.Contains(t, final, "pool/main/h/hello/hello_1.0.orig.tar.xz")
}

func TestReleaseManifestKeepsCompressionVariantsDistinct(t *testing.T) {
	plain := []byte("plain")
	gz := []byte("gzip")
	xz := []byte("xz")
	manifest, err := parseReleaseManifest(strings.NewReader(releaseSHA256(map[string][]byte{
		"main/binary-amd64/Packages":    plain,
		"main/binary-amd64/Packages.gz": gz,
		"main/binary-amd64/Packages.xz": xz,
	})))
	require.NoError(t, err)
	require.Len(t, manifest.Entries, 3)
	require.Equal(t, "main/binary-amd64/Packages", manifest.Entries[0].Path)
	require.Equal(t, "main/binary-amd64/Packages.gz", manifest.Entries[1].Path)
	require.Equal(t, "main/binary-amd64/Packages.xz", manifest.Entries[2].Path)
	require.NotEqual(t, manifest.Entries[1].SHA256, manifest.Entries[2].SHA256)
}

func TestDistributionRefreshReconstructsSignedUncompressedIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	contents := []byte("usr/bin/example main/example\n")
	compressed := gzipData(t, string(contents))
	unrelated := []byte("unrelated")
	release := "Acquire-By-Hash: yes\n" + releaseSHA256(map[string][]byte{
		"main/Contents-all":                  contents,
		"main/Contents-all.gz":               compressed,
		"main/dep11/Components-amd64.yml.gz": unrelated,
	})
	var unrelatedRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Contents-all":
			http.NotFound(w, r)
		case "/dists/trixie/main/Contents-all.gz":
			_, _ = w.Write(compressed)
		case "/dists/trixie/main/dep11/Components-amd64.yml.gz":
			unrelatedRequests.Add(1)
			_, _ = w.Write(unrelated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/Contents-all", Kind: "metadata"},
	)
	require.NoError(t, handler.RefreshRoot(ctx, "deb_distribution:dists/trixie"))
	require.Zero(t, unrelatedRequests.Load())

	contentsDigest := sha256.Sum256(contents)
	compressedDigest := sha256.Sum256(compressed)
	for requestPath, expected := range map[string][]byte{
		"/dists/trixie/main/Contents-all":                                          contents,
		"/dists/trixie/main/Contents-all.gz":                                       compressed,
		"/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", contentsDigest):   contents,
		"/dists/trixie/main/by-hash/SHA256/" + fmt.Sprintf("%x", compressedDigest): compressed,
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, http.StatusOK, recorder.Code, requestPath)
		require.Equal(t, "GENERATION", recorder.Header().Get("X-Cache"), requestPath)
		require.Equal(t, expected, recorder.Body.Bytes(), requestPath)
	}
}

func TestDistributionReconstructionRejectsChangedReleaseAnchor(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)

	firstContents := []byte("first contents\n")
	secondContents := []byte("second contents\n")
	compressed := gzipData(t, string(firstContents))
	firstRelease := releaseSHA256(map[string][]byte{
		"main/Contents-all": firstContents, "main/Contents-all.gz": compressed,
	})
	secondRelease := releaseSHA256(map[string][]byte{
		"main/Contents-all": secondContents, "main/Contents-all.gz": gzipData(t, string(secondContents)),
	})
	var releaseRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			if releaseRequests.Add(1) == 1 {
				_, _ = io.WriteString(w, firstRelease)
				return
			}
			_, _ = io.WriteString(w, secondRelease)
		case "/dists/trixie/main/Contents-all.gz":
			_, _ = w.Write(compressed)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/Contents-all", Kind: "metadata"},
	)
	outcome, err := handler.RefreshRootTask(context.Background(), "deb_distribution:dists/trixie")
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, "restarted", outcome.Result)
	require.Equal(t, "staging_anchor_changed", outcome.ReasonCode)
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDistributionRefreshRejectsInvalidReconstructedIndex(t *testing.T) {
	for _, test := range []struct {
		name               string
		expectedContents   []byte
		declaredCompressed []byte
		servedCompressed   []byte
		errorText          string
	}{
		{
			name: "compressed checksum mismatch", expectedContents: []byte("contents"),
			declaredCompressed: gzipData(t, "contents"), errorText: "metadata SHA256 mismatch",
		},
		{
			name: "decompression failure", expectedContents: []byte("contents"),
			declaredCompressed: []byte("not-gzip"), servedCompressed: []byte("not-gzip"), errorText: "EOF",
		},
		{
			name: "decompressed size mismatch", expectedContents: []byte("contents-longer"),
			declaredCompressed: gzipData(t, "contents"), errorText: "derived metadata size mismatch",
		},
		{
			name: "decompressed digest mismatch", expectedContents: []byte("expected"),
			declaredCompressed: gzipData(t, "differnt"), errorText: "derived metadata SHA256 mismatch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			servedCompressed := test.servedCompressed
			if servedCompressed == nil {
				servedCompressed = append([]byte(nil), test.declaredCompressed...)
			}
			if test.name == "compressed checksum mismatch" {
				servedCompressed[0] ^= 0xff
			}
			release := releaseSHA256(map[string][]byte{
				"main/Contents-all":    test.expectedContents,
				"main/Contents-all.gz": test.declaredCompressed,
			})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dists/trixie/InRelease":
					_, _ = io.WriteString(w, release)
				case "/dists/trixie/main/Contents-all.gz":
					_, _ = w.Write(servedCompressed)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			handler := newDebDistributionTestHandler(t, server.URL,
				filerepo.MetadataTarget{URL: "dists/trixie/main/Contents-all", Kind: "metadata"},
			)
			err := handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie")
			require.ErrorContains(t, err, test.errorText)
			require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
		})
	}
}

func TestDistributionRefreshCommitsUnlistedTargetAsNotFound(t *testing.T) {
	release := releaseSHA256(map[string][]byte{"main/Contents-amd64.gz": gzipData(t, "contents")})
	var targetRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dists/trixie/InRelease" {
			_, _ = io.WriteString(w, release)
			return
		}
		if r.URL.Path == "/dists/trixie/main/Contents-all" {
			targetRequests.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/Contents-all", Kind: "metadata"},
	)
	require.NoError(t, handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie"))
	require.Zero(t, targetRequests.Load())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Equal(t, "GENERATION", recorder.Header().Get("X-Cache"))
}

func TestDistributionBootstrapPassesThroughBeforePublishingSelectedClosure(t *testing.T) {
	contentsAll := []byte("usr/bin/example main/example\n")
	contentsAMD64 := []byte("usr/bin/amd64 main/example\n")
	release := releaseSHA256(map[string][]byte{
		"main/Contents-all": contentsAll, "main/Contents-amd64": contentsAMD64,
	})
	var upstreamRequests atomic.Int32
	var amd64Requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Contents-all":
			_, _ = w.Write(contentsAll)
		case "/dists/trixie/main/Contents-amd64":
			amd64Requests.Add(1)
			_, _ = w.Write(contentsAMD64)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL)
	outcome, err := handler.RefreshRootTask(context.Background(), "deb_distribution:dists/trixie")
	require.ErrorIs(t, err, scheduler.ErrTaskSkipped)
	require.Equal(t, "metadata_closure_pending", outcome.ReasonCode)
	require.Zero(t, upstreamRequests.Load())
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, contentsAll, recorder.Body.Bytes())
	require.NotEqual(t, "GENERATION", recorder.Header().Get("X-Cache"))
	require.Empty(t, recorder.Header().Get("X-Cache-Generation"))

	require.NoError(t, handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie"))
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "GENERATION", recorder.Header().Get("X-Cache"))
	require.Equal(t, contentsAll, recorder.Body.Bytes())

	beforeExpansion := amd64Requests.Load()
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-amd64", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, "1", recorder.Header().Get("Retry-After"))
	require.Equal(t, beforeExpansion, amd64Requests.Load(), "usable generations must not bypass missing metadata")

	require.NoError(t, handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie"))
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-amd64", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "GENERATION", recorder.Header().Get("X-Cache"))
	require.Equal(t, contentsAMD64, recorder.Body.Bytes())
}

func TestDistributionRootAttributesInferPackageClosure(t *testing.T) {
	packages := []byte("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n")
	compressed := gzipData(t, string(packages))
	unusedXZ := []byte("unused xz representation")
	release := releaseSHA256(map[string][]byte{
		"main/binary-amd64/Packages":    packages,
		"main/binary-amd64/Packages.gz": compressed,
		"main/binary-amd64/Packages.xz": unusedXZ,
	})
	var xzRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/binary-amd64/Packages.gz":
			_, _ = w.Write(compressed)
		case "/dists/trixie/main/binary-amd64/Packages.xz":
			xzRequests.Add(1)
			_, _ = w.Write(unusedXZ)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL)
	root := debDistributionRoot("dists/trixie", "trixie", []string{"main"}, []string{"amd64"}, false)
	root.Targets = append(root.Targets, filerepo.MetadataTarget{
		URL: "dists/trixie/main/binary-amd64/Packages.gz", Kind: "packages",
	})
	handler.AddRepository(root)
	require.NoError(t, handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie"))
	require.Zero(t, xzRequests.Load())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/binary-amd64/Packages", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, packages, recorder.Body.Bytes())
}

func TestDistributionRefreshFailurePreservesCurrentGeneration(t *testing.T) {
	oldContents := []byte("old contents\n")
	newContents := []byte("new contents\n")
	release := releaseSHA256(map[string][]byte{"main/Contents-all": oldContents})
	serveContents := true
	var mu sync.RWMutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/Contents-all":
			if serveContents {
				_, _ = w.Write(oldContents)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/Contents-all", Kind: "metadata"},
	)
	require.NoError(t, handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie"))
	mu.Lock()
	release = releaseSHA256(map[string][]byte{"main/Contents-all": newContents})
	serveContents = false
	mu.Unlock()
	require.ErrorContains(t, handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie"), "Contents-all")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, oldContents, recorder.Body.Bytes())
}

func TestDistributionRefreshRejectsGenerationWhenOneReleaseIndexIsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	amd64Index := gzipData(t, "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n")
	release := releaseSHA256(map[string][]byte{
		"main/binary-amd64/Packages.gz": amd64Index,
		"main/binary-arm64/Packages.gz": nil,
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/binary-amd64/Packages.gz":
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(amd64Index)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/binary-amd64/Packages.gz", Kind: "packages"},
		filerepo.MetadataTarget{URL: "dists/trixie/main/binary-arm64/Packages.gz", Kind: "packages"},
	)
	require.ErrorContains(t, handler.RefreshRoot(ctx, "deb_distribution:dists/trixie"), "binary-arm64/Packages.gz")

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.False(t, statuses[0].HasCurrent)
}

func TestDistributionRefreshFailsWhenAllReleaseIndexesAreMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	release := releaseSHA256(map[string][]byte{
		"main/binary-arm64/Packages.gz": nil,
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dists/trixie/InRelease" {
			_, _ = io.WriteString(w, release)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/binary-arm64/Packages.gz", Kind: "packages"},
	)
	err := handler.RefreshRoot(ctx, "deb_distribution:dists/trixie")
	require.Error(t, err)
	require.Contains(t, err.Error(), "binary-arm64/Packages.gz")
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
}

func TestDistributionRefreshFailsOnReleaseIndexChecksumMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	amd64Index := gzipData(t, "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n")
	release := fmt.Sprintf("SHA256:\n %064x %d main/binary-amd64/Packages.gz\n", 1, len(amd64Index))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/binary-amd64/Packages.gz":
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(amd64Index)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/binary-amd64/Packages.gz", Kind: "packages"},
	)
	err := handler.RefreshRoot(ctx, "deb_distribution:dists/trixie")
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHA256 mismatch")
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
}

func TestDistributionRefreshFailsOnReleaseIndexSizeMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body := []byte("components")
	sum := sha256.Sum256(body)
	release := fmt.Sprintf("SHA256:\n %x %d main/dep11/Components-amd64.yml\n", sum, len(body)+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/dep11/Components-amd64.yml":
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/dep11/Components-amd64.yml", Kind: "metadata"},
	)
	err := handler.RefreshRoot(ctx, "deb_distribution:dists/trixie")
	require.ErrorContains(t, err, "size mismatch")
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
}

func TestDistributionRefreshDoesNotSubstituteCompressionVariants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	xzBody := []byte("expected xz bytes")
	gzBody := []byte("different gzip bytes")
	release := releaseSHA256(map[string][]byte{
		"main/dep11/Components-amd64.yml.xz": xzBody,
	})
	var gzipRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/dep11/Components-amd64.yml.gz":
			gzipRequests++
			_, _ = w.Write(gzBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/dep11/Components-amd64.yml.xz", Kind: "metadata"},
	)
	err := handler.RefreshRoot(ctx, "deb_distribution:dists/trixie")
	require.ErrorContains(t, err, "Components-amd64.yml.xz")
	require.Zero(t, gzipRequests, "a missing xz object must not fall back to gzip bytes")
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
}

func TestDistributionRefreshKeepsReleaseFallbackServingIdentityExact(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	components := []byte("components")
	release := releaseSHA256(map[string][]byte{"main/dep11/Components-amd64.yml": components})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/trixie/Release":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/dep11/Components-amd64.yml":
			_, _ = w.Write(components)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/main/dep11/Components-amd64.yml", Kind: "metadata"},
	)
	require.NoError(t, handler.RefreshRoot(ctx, "deb_distribution:dists/trixie"))
	for requestPath, expected := range map[string]struct {
		status int
		body   []byte
	}{
		"/dists/trixie/InRelease":                       {status: http.StatusNotFound},
		"/dists/trixie/Release":                         {status: http.StatusOK, body: []byte(release)},
		"/dists/trixie/main/dep11/Components-amd64.yml": {status: http.StatusOK, body: components},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Equal(t, expected.status, rec.Code, requestPath)
		require.Equal(t, "GENERATION", rec.Header().Get("X-Cache"), requestPath)
		require.NotEmpty(t, rec.Header().Get("X-Cache-Generation"), requestPath)
		require.Equal(t, expected.body, rec.Body.Bytes(), requestPath)
	}
}

func TestDistributionReleaseCompanionsOnlyAllowForbiddenOrNotFound(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		succeeds bool
	}{
		{name: "forbidden companion is optional", status: http.StatusForbidden, succeeds: true},
		{name: "unauthorized companion is fatal", status: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			components := []byte("components")
			release := releaseSHA256(map[string][]byte{"main/dep11/Components-amd64.yml": components})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dists/trixie/Release":
					_, _ = io.WriteString(w, release)
				case "/dists/trixie/Release.gpg", "/dists/trixie/Release.sig", "/dists/trixie/Release.asc":
					w.WriteHeader(test.status)
				case "/dists/trixie/main/dep11/Components-amd64.yml":
					_, _ = w.Write(components)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			handler := newDebDistributionTestHandler(t, server.URL,
				filerepo.MetadataTarget{URL: "dists/trixie/main/dep11/Components-amd64.yml", Kind: "metadata"},
			)
			err := handler.RefreshRoot(context.Background(), "deb_distribution:dists/trixie")
			if test.succeeds {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDistributionGenerationServesExactCanonicalAndByHashMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	packagesPath := "main/binary-amd64/Packages.gz"
	packages := gzipData(t, "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n")
	componentsPath := "main/dep11/Components-amd64.yml.gz"
	components := gzipData(t, "components")
	release := "Acquire-By-Hash: yes\n" + releaseSHA256(map[string][]byte{
		packagesPath: packages, componentsPath: components,
	})
	bodies := map[string][]byte{packagesPath: packages, componentsPath: components}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dists/trixie/InRelease" {
			_, _ = io.WriteString(w, release)
			return
		}
		for relativePath, body := range bodies {
			sum := sha256.Sum256(body)
			byHash := "/dists/trixie/" + path.Dir(relativePath) + "/by-hash/SHA256/" + fmt.Sprintf("%x", sum)
			if r.URL.Path == byHash {
				_, _ = w.Write(body)
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	handler := newDebDistributionTestHandler(t, server.URL,
		filerepo.MetadataTarget{URL: "dists/trixie/" + packagesPath, Kind: "packages"},
		filerepo.MetadataTarget{URL: "dists/trixie/" + componentsPath, Kind: "metadata"},
	)
	require.NoError(t, handler.RefreshRoot(ctx, "deb_distribution:dists/trixie"))
	for relativePath, body := range bodies {
		canonical := "/dists/trixie/" + relativePath
		sum := sha256.Sum256(body)
		byHash := "/dists/trixie/" + path.Dir(relativePath) + "/by-hash/SHA256/" + fmt.Sprintf("%x", sum)
		for _, requestPath := range []string{canonical, byHash} {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, requestPath, nil))
			require.Equal(t, http.StatusOK, rec.Code, requestPath)
			require.Equal(t, "GENERATION", rec.Header().Get("X-Cache"), requestPath)
			require.NotEmpty(t, rec.Header().Get("X-Cache-Generation"), requestPath)
			require.Equal(t, body, rec.Body.Bytes(), requestPath)
		}
	}
}

func TestDistributionRefreshResumesPersistedStagingInBoundedSlices(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries := make(map[string][]byte, 40)
	for i := range 40 {
		entries[fmt.Sprintf("main/dep11/item-%02d.yml", i)] = []byte(fmt.Sprintf("item-%02d", i))
	}
	release := releaseSHA256(entries)
	requests := map[string]int{}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanPath := strings.TrimPrefix(r.URL.Path, "/dists/trixie/")
		mu.Lock()
		requests[cleanPath]++
		mu.Unlock()
		if cleanPath == "InRelease" {
			_, _ = io.WriteString(w, release)
			return
		}
		if body, ok := entries[cleanPath]; ok {
			_, _ = w.Write(body)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	targets := make([]filerepo.MetadataTarget, 0, len(entries))
	for cleanPath := range entries {
		targets = append(targets, filerepo.MetadataTarget{URL: "dists/trixie/" + cleanPath, Kind: "metadata"})
	}
	handler := newDebDistributionTestHandler(t, server.URL, targets...)
	outcome, err := handler.RefreshRootTask(ctx, "deb_distribution:dists/trixie")
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, "partial", outcome.Result)
	require.Equal(t, "staging_continuation", outcome.ReasonCode)
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)

	outcome, err = handler.RefreshRootTask(ctx, "deb_distribution:dists/trixie")
	require.NoError(t, err)
	require.Equal(t, "updated", outcome.Result)
	require.True(t, handler.RepositoryStatuses()[0].HasCurrent)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 3, requests["InRelease"], "resumed slice fetches and then reconfirms its signed anchor")
	for cleanPath := range entries {
		require.Equal(t, 1, requests[cleanPath], cleanPath)
	}
}

func TestDistributionRefreshDiscardsStagingWhenReleaseAnchorChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries := make(map[string][]byte, 40)
	for i := range 40 {
		entries[fmt.Sprintf("main/dep11/item-%02d.yml", i)] = []byte(fmt.Sprintf("item-%02d", i))
	}
	release := releaseSHA256(entries)
	var mu sync.RWMutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		cleanPath := strings.TrimPrefix(r.URL.Path, "/dists/trixie/")
		if cleanPath == "InRelease" {
			_, _ = io.WriteString(w, release)
			return
		}
		if body, ok := entries[cleanPath]; ok {
			_, _ = w.Write(body)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	targets := make([]filerepo.MetadataTarget, 0, len(entries))
	for cleanPath := range entries {
		targets = append(targets, filerepo.MetadataTarget{URL: "dists/trixie/" + cleanPath, Kind: "metadata"})
	}
	handler := newDebDistributionTestHandler(t, server.URL, targets...)
	_, err := handler.RefreshRootTask(ctx, "deb_distribution:dists/trixie")
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)

	mu.Lock()
	entries["main/dep11/item-40.yml"] = []byte("item-40")
	release = releaseSHA256(entries)
	mu.Unlock()
	outcome, err := handler.RefreshRootTask(ctx, "deb_distribution:dists/trixie")
	require.ErrorAs(t, err, &retry)
	require.Equal(t, "restarted", outcome.Result)
	require.Equal(t, "staging_anchor_changed", outcome.ReasonCode)
	require.False(t, handler.RepositoryStatuses()[0].HasCurrent)
}

func TestDEBSnapshotValidatorRejectsMissingReleaseIndex(t *testing.T) {
	index := gzipData(t, "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n")
	releasePath := "dists/trixie/InRelease"
	indexPath := "dists/trixie/main/binary-amd64/Packages.gz"
	release := releaseSHA256(map[string][]byte{"main/binary-amd64/Packages.gz": index})
	snapshot := &filerepo.LiveSnapshot{
		Targets: []filerepo.MetadataTarget{
			{URL: releasePath, Candidates: []string{"dists/trixie/Release"}, Kind: "release"},
			{URL: indexPath, Kind: "packages"},
		},
		Anchors: []filerepo.MetadataAnchor{{Path: releasePath, State: filerepo.MetadataPresent}},
		Metadata: map[string]filerepo.MetadataObject{
			releasePath: {Path: releasePath, State: filerepo.MetadataPresent, Required: true},
		},
	}
	opener := func(cleanPath string) (io.ReadCloser, error) {
		if cleanPath == releasePath {
			return io.NopCloser(strings.NewReader(release)), nil
		}
		return nil, fmt.Errorf("missing %s", cleanPath)
	}
	require.ErrorContains(t, (inspector{}).ValidateSnapshot(context.Background(), snapshot, opener), indexPath)
}

func TestRestoreRejectsAnchorOnlyDistributionCurrent(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"deb-test", config.ModeDEB, config.ModeDEB, inspector{}, []string{"https://upstream.example"}, nil,
		config.Expiration(time.Hour), &filerepo.Policy{}, buildSnapshot, store, stats,
		health.New("deb-test", config.ModeDEB, health.DefaultConfig(), []string{"https://upstream.example"}, stats), nil,
	)
	root := debDistributionRoot("dists/trixie", "trixie", nil, nil, false)
	handler.AddRepository(root)

	rootID := root.ID
	generation := "anchor-only"
	releasePath := "dists/trixie/InRelease"
	release := []byte(releaseSHA256(map[string][]byte{"main/Contents-all": []byte("contents")}))
	releaseSum := sha256.Sum256(release)
	releaseDigest := fmt.Sprintf("%x", releaseSum)
	anchors := []filerepo.MetadataAnchor{{
		Path: releasePath, State: filerepo.MetadataPresent, Size: int64(len(release)), Digest: releaseDigest,
	}}
	anchorData, err := yaml.Marshal(anchors)
	require.NoError(t, err)
	anchorSetSum := sha256.Sum256(anchorData)
	cleanupSum := sha256.Sum256(nil)
	snapshot := filerepo.LiveSnapshot{
		Version: 3, RootID: rootID, RootPath: root.Path, Generation: generation,
		Upstream: "https://upstream.example", Published: time.Now().UTC(),
		AnchorSetDigest: "sha256:" + fmt.Sprintf("%x", anchorSetSum), Anchors: anchors,
		Metadata: map[string]filerepo.MetadataObject{
			releasePath: {
				Path: releasePath, State: filerepo.MetadataPresent, Required: true,
				Digest: "sha256:" + releaseDigest, Size: int64(len(release)),
			},
		},
		Targets: root.Targets, CleanupIndexDigest: "sha256:" + fmt.Sprintf("%x", cleanupSum),
	}
	snapshotData, err := yaml.Marshal(snapshot)
	require.NoError(t, err)
	snapshotSum := sha256.Sum256(snapshotData)
	rootSum := sha256.Sum256([]byte(rootID))
	rootKey := fmt.Sprintf("%x", rootSum)
	basePath := path.Join(config.ModeDEB, ".roots", rootKey)
	metadataPath := path.Join(basePath, "generations", generation, "metadata", releasePath)
	cleanupPath := path.Join(basePath, "generations", generation, "cleanup", "paths.txt")
	snapshotPath := path.Join(basePath, "snapshots", generation+".yaml")
	currentPath := path.Join(basePath, "current.yaml")
	for objectPath, data := range map[string][]byte{
		metadataPath: release,
		cleanupPath:  {},
		snapshotPath: snapshotData,
	} {
		require.NoError(t, store.MkdirAll(path.Join("deb-test", path.Dir(objectPath)), 0o755))
		_, err = store.Put(ctx, "deb-test", objectPath, bytes.NewReader(data), nil)
		require.NoError(t, err)
	}
	currentData, err := yaml.Marshal(struct {
		Version            int    `yaml:"version"`
		RootID             string `yaml:"root_id"`
		Generation         string `yaml:"generation"`
		SnapshotDigest     string `yaml:"snapshot_sha256"`
		CleanupIndexDigest string `yaml:"cleanup_index_sha256"`
	}{
		Version: 2, RootID: rootID, Generation: generation,
		SnapshotDigest:     "sha256:" + fmt.Sprintf("%x", snapshotSum),
		CleanupIndexDigest: snapshot.CleanupIndexDigest,
	})
	require.NoError(t, err)
	_, err = store.Put(ctx, "deb-test", currentPath, bytes.NewReader(currentData), nil)
	require.NoError(t, err)

	require.NoError(t, handler.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, handler.Stop(stopCtx))
	})
	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.False(t, statuses[0].HasCurrent)
	_, err = store.StatObject(ctx, "deb-test", currentPath)
	require.Error(t, err)
}

func TestDEBSnapshotValidatorRejectsMissingSelectedByHashAlias(t *testing.T) {
	index := gzipData(t, "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n")
	digest := sha256.Sum256(index)
	releasePath := "dists/trixie/InRelease"
	indexPath := "dists/trixie/main/binary-amd64/Packages.gz"
	release := "Acquire-By-Hash: yes\n" + releaseSHA256(map[string][]byte{"main/binary-amd64/Packages.gz": index})
	snapshot := &filerepo.LiveSnapshot{
		Targets: []filerepo.MetadataTarget{
			{URL: releasePath, Candidates: []string{"dists/trixie/Release"}, Kind: "release"},
			{URL: indexPath, Kind: "packages"},
		},
		Anchors: []filerepo.MetadataAnchor{{Path: releasePath, State: filerepo.MetadataPresent}},
		Metadata: map[string]filerepo.MetadataObject{
			releasePath: {Path: releasePath, State: filerepo.MetadataPresent, Required: true},
			indexPath: {
				Path: indexPath, State: filerepo.MetadataPresent, Required: true,
				Size: int64(len(index)), ChecksumType: "sha256", Checksum: fmt.Sprintf("%x", digest),
			},
		},
	}
	opener := func(cleanPath string) (io.ReadCloser, error) {
		switch cleanPath {
		case releasePath:
			return io.NopCloser(strings.NewReader(release)), nil
		case indexPath:
			return io.NopCloser(bytes.NewReader(index)), nil
		default:
			return nil, fmt.Errorf("missing %s", cleanPath)
		}
	}
	require.ErrorContains(t, (inspector{}).ValidateSnapshot(context.Background(), snapshot, opener), "by-hash mapping")
}

func TestAnalyzerCreatesRootFromReleaseUnderPrefix(t *testing.T) {
	result := (inspector{}).InspectPath("proxmox/debian/pve/dists/trixie/InRelease")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "deb_distribution:proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, filerepo.LayoutDebDistribution, result.Root.Layout)
	require.Equal(t, []string{
		"proxmox/debian/pve/dists/trixie/InRelease",
		"proxmox/debian/pve/dists/trixie/Release",
	}, result.Root.PrimaryMetadata)
}

func TestAnalyzerTreatsPackagesAsUpdateOnly(t *testing.T) {
	result := (inspector{}).InspectPath("proxmox/debian/pve/dists/trixie/pve-no-subscription/binary-amd64/Packages.gz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryUpdateRoot, result.Role)
	require.Equal(t, "deb_distribution:proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, []string{"pve-no-subscription"}, result.Root.Components)
	require.Equal(t, []string{"amd64"}, result.Root.Architectures)
	require.Contains(t, result.Root.Targets, filerepo.MetadataTarget{
		URL: "proxmox/debian/pve/dists/trixie/pve-no-subscription/binary-amd64/Packages.gz", Kind: "packages",
	})
}

func TestAnalyzerTreatsSourcesAsUpdateOnly(t *testing.T) {
	result := (inspector{}).InspectPath("proxmox/debian/pve/dists/trixie/pve-no-subscription/source/Sources.xz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryUpdateRoot, result.Role)
	require.Equal(t, "deb_distribution:proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, []string{"pve-no-subscription"}, result.Root.Components)
	require.True(t, result.Root.Source)
	require.Contains(t, result.Root.Targets, filerepo.MetadataTarget{
		URL: "proxmox/debian/pve/dists/trixie/pve-no-subscription/source/Sources.xz", Kind: "sources",
	})
}

func TestAnalyzerTreatsAuxiliaryMetadataAsExactUpdateTarget(t *testing.T) {
	result := (inspector{}).InspectPath("dists/trixie/main/Contents-all")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryUpdateRoot, result.Role)
	require.Contains(t, result.Root.Targets, filerepo.MetadataTarget{
		URL: "dists/trixie/main/Contents-all", Kind: "metadata",
	})

	byHash := (inspector{}).InspectPath("dists/trixie/main/by-hash/SHA256/abcdef")
	require.Equal(t, filerepo.DiscoveryIgnore, byHash.Role)
	require.Empty(t, byHash.Root.ID)
}

func TestAnalyzerCreatesFlatRootFromPackagesAtRepositoryRoot(t *testing.T) {
	result := (inspector{}).InspectPath("Packages.gz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "deb_flat:/", result.Root.ID)
	require.Equal(t, "/", result.Root.DisplayName)
	require.Equal(t, filerepo.LayoutDebFlat, result.Root.Layout)
	require.Equal(t, []string{"Packages.xz"}, result.Root.PrimaryMetadata[:1])
}

func TestAnalyzerCreatesFlatRootFromNestedSources(t *testing.T) {
	result := (inspector{}).InspectPath("local/repo/Sources.xz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "deb_flat:local/repo", result.Root.ID)
	require.Equal(t, "local/repo", result.Root.Path)
	require.True(t, result.Root.Source)
	require.Equal(t, []string{"local/repo/Sources.xz"}, result.Root.PrimaryMetadata[:1])
}

func TestDebianFlatSourcesExpandsCommittedPackagesRoot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/Packages":
			_, _ = io.WriteString(w, "Package: binary\nVersion: 1\nFilename: pool/binary.deb\n\n")
		case "/Sources":
			_, _ = io.WriteString(w, "Package: source\nDirectory: pool/source\nFiles:\n abc 1 source.dsc\n\n")
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"deb-test", config.ModeDEB, config.ModeDEB, inspector{}, []string{server.URL}, nil,
		config.Expiration(time.Hour), &filerepo.Policy{}, buildSnapshot, store, stats,
		health.New("deb-test", config.ModeDEB, health.DefaultConfig(), []string{server.URL}, stats), nil,
	)
	rootID := filerepo.RepositoryID(filerepo.LayoutDebFlat, "")
	handler.AddRepository(filerepo.RepositoryRoot{
		ID: rootID, Layout: filerepo.LayoutDebFlat,
		PrimaryMetadata: []string{"Packages"},
		Targets:         []filerepo.MetadataTarget{{URL: "Packages", Kind: "packages"}},
	})
	require.NoError(t, handler.RefreshRoot(context.Background(), rootID))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/Sources", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.NoError(t, handler.RefreshRoot(context.Background(), rootID))

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/Sources", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "GENERATION", recorder.Header().Get("X-Cache"))
}

func TestAnalyzerClassifiesPrefixedArtifactPath(t *testing.T) {
	result := (inspector{}).InspectPath("proxmox/debian/pve/pool/main/p/pkg/pkg_1.0_amd64.deb")
	require.Equal(t, filerepo.ResourceArtifact, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}

func TestAnalyzerClassifiesMetadataWithoutCreatingRoot(t *testing.T) {
	result := (inspector{}).InspectPath("proxmox/debian/pve/dists/trixie/by-hash/SHA256/abcdef")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}

func newDebDistributionTestHandler(t *testing.T, upstream string, targets ...filerepo.MetadataTarget) *filerepo.IndexedHandler {
	t.Helper()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"deb-test",
		config.ModeDEB,
		config.ModeDEB,
		inspector{},
		[]string{upstream},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		buildSnapshot,
		store,
		stats,
		health.New("deb-test", config.ModeDEB, health.DefaultConfig(), []string{upstream}, stats),
		nil,
	)
	root := debDistributionRoot("dists/trixie", "trixie", nil, nil, false)
	root.Targets = append(root.Targets, targets...)
	handler.AddRepository(root)
	return handler
}

func gzipData(t *testing.T, value string) []byte {
	t.Helper()

	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	_, err := writer.Write([]byte(value))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return output.Bytes()
}

func releaseSHA256(entries map[string][]byte) string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var output strings.Builder
	output.WriteString("SHA256:\n")
	for _, key := range keys {
		sum := sha256.Sum256(entries[key])
		_, _ = fmt.Fprintf(&output, " %x %d %s\n", sum, len(entries[key]), key)
	}
	return output.String()
}
