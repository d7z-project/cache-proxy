package deb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
