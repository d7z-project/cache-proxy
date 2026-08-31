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
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

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

func TestReleaseIndexSelectionParsesOnePreferredCompressionVariant(t *testing.T) {
	entries := []releaseEntry{
		{Path: "main/binary-amd64/Packages", Size: 100},
		{Path: "main/binary-amd64/Packages.gz", Size: 50},
		{Path: "main/binary-amd64/Packages.xz", Size: 25},
		{Path: "main/source/Sources.gz", Size: 40},
	}
	targets, err := selectReleaseIndexes(entries)
	require.NoError(t, err)
	require.Len(t, targets, 2)
	require.Equal(t, "main/binary-amd64/Packages.xz", targets[0].entry.Path)
	require.Equal(t, "main/source/Sources.gz", targets[1].entry.Path)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

			handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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

	handler := newDebDistributionTestHandler(t, server.URL)
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
		Targets: []filerepo.MetadataTarget{{URL: releasePath, Candidates: []string{"dists/trixie/Release"}}},
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
}

func TestAnalyzerTreatsSourcesAsUpdateOnly(t *testing.T) {
	result := (inspector{}).InspectPath("proxmox/debian/pve/dists/trixie/pve-no-subscription/source/Sources.xz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryUpdateRoot, result.Role)
	require.Equal(t, "deb_distribution:proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, []string{"pve-no-subscription"}, result.Root.Components)
	require.True(t, result.Root.Source)
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

func newDebDistributionTestHandler(t *testing.T, upstream string) *filerepo.IndexedHandler {
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
	handler.AddRepository(debDistributionRoot("dists/trixie", "trixie", nil, nil, false))
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
