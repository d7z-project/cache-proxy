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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParsePackagesBuildsCleanupPaths(t *testing.T) {
	paths := &filerepo.PathIndexBuilder{}
	count, err := parsePackages(strings.NewReader("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\n\n"), paths, 0)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "pool/main/h/hello/hello_1.0_amd64.deb", paths.Finalize()[0])
}

func TestParseSourcesBuildsCleanupPaths(t *testing.T) {
	paths := &filerepo.PathIndexBuilder{}
	count, err := parseSources(strings.NewReader("Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc111 123 hello_1.0.dsc\n def222 456 hello_1.0.orig.tar.xz\n\n"), paths, 0)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	final := paths.Finalize()
	require.Contains(t, final, "pool/main/h/hello/hello_1.0.dsc")
	require.Contains(t, final, "pool/main/h/hello/hello_1.0.orig.tar.xz")
}

func TestReleaseIndexTargetsPreferXZOverGZAndPlain(t *testing.T) {
	sums := map[string]string{
		"main/binary-amd64/Packages":    "plain",
		"main/binary-amd64/Packages.gz": "gz",
		"main/binary-amd64/Packages.xz": "xz",
	}
	targets := releaseIndexTargets("dists/bookworm/InRelease", sums)
	require.Len(t, targets, 1)
	require.Equal(t, "dists/bookworm/main/binary-amd64/Packages.xz", targets[0].URL)
	require.Equal(t, []string{
		"dists/bookworm/main/binary-amd64/Packages.gz",
		"dists/bookworm/main/binary-amd64/Packages",
	}, targets[0].Candidates)
}

func TestDistributionRefreshPublishesWhenOneReleaseIndexIsMissing(t *testing.T) {
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
	require.NoError(t, handler.RefreshRoot(ctx, "deb_distribution:dists/trixie"))

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.True(t, statuses[0].HasCurrent)
	require.Equal(t, 1, statuses[0].ArtifactCount)
	require.Contains(t, statuses[0].Warning, "partial metadata: skipped 1 missing indexes")

	amd64Req := httptest.NewRequest(http.MethodGet, "/dists/trixie/main/binary-amd64/Packages.gz", nil)
	amd64Rec := httptest.NewRecorder()
	handler.ServeHTTP(amd64Rec, amd64Req)
	require.Equal(t, http.StatusOK, amd64Rec.Code)

	arm64Req := httptest.NewRequest(http.MethodGet, "/dists/trixie/main/binary-arm64/Packages.gz", nil)
	arm64Rec := httptest.NewRecorder()
	handler.ServeHTTP(arm64Rec, arm64Req)
	require.Equal(t, http.StatusNotFound, arm64Rec.Code)
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
	require.Contains(t, err.Error(), "no package indexes available")
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
		health.New("deb-test", config.ModeDEB, health.DefaultConfig(), []string{upstream}, stats, "cache-proxy-test"),
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
