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

func TestDiscovererCreatesRootFromReleaseUnderPrefix(t *testing.T) {
	result := (discoverer{}).Discover("proxmox/debian/pve/dists/trixie/InRelease")
	require.True(t, result.Matched)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, []string{
		"proxmox/debian/pve/dists/trixie/InRelease",
		"proxmox/debian/pve/dists/trixie/Release",
	}, result.Root.PrimaryMetadata)
}

func TestDiscovererTreatsPackagesAsUpdateOnly(t *testing.T) {
	result := (discoverer{}).Discover("proxmox/debian/pve/dists/trixie/pve-no-subscription/binary-amd64/Packages.gz")
	require.True(t, result.Matched)
	require.Equal(t, filerepo.DiscoveryUpdateRoot, result.Role)
	require.Equal(t, "proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, []string{"pve-no-subscription"}, result.Root.Components)
	require.Equal(t, []string{"amd64"}, result.Root.Architectures)
}

func TestDiscovererTreatsSourcesAsUpdateOnly(t *testing.T) {
	result := (discoverer{}).Discover("proxmox/debian/pve/dists/trixie/pve-no-subscription/source/Sources.xz")
	require.True(t, result.Matched)
	require.Equal(t, filerepo.DiscoveryUpdateRoot, result.Role)
	require.Equal(t, "proxmox/debian/pve/dists/trixie", result.Root.ID)
	require.Equal(t, []string{"pve-no-subscription"}, result.Root.Components)
	require.True(t, result.Root.Source)
}

func TestDiscovererRejectsNonMetadataPath(t *testing.T) {
	result := (discoverer{}).Discover("proxmox/debian/pve/pool/main/p/pkg/pkg_1.0_amd64.deb")
	require.False(t, result.Matched)
}
