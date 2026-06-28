package deb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParsePackagesRecordsArtifactChecksum(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]filerepo.RepoObject{}, Auxiliary: map[string]filerepo.RepoObject{}}
	input := "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: abc123\n\n"
	require.NoError(t, parsePackages(strings.NewReader(input), snapshot))
	require.Equal(t, "abc123", snapshot.Artifacts["pool/main/h/hello/hello_1.0_amd64.deb"].Identity)
	require.Equal(t, "abc123", snapshot.Auxiliary["pool/main/h/hello/hello_1.0_amd64.deb.sha256"].Identity)
	require.Equal(t, "abc123", snapshot.Auxiliary["pool/main/h/hello/hello_1.0_amd64.deb.md5sum"].Identity)
}

func TestParseSourcesRecordsArtifacts(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]filerepo.RepoObject{}, Auxiliary: map[string]filerepo.RepoObject{}}
	input := "Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc111 123 hello_1.0.dsc\n def222 456 hello_1.0.orig.tar.xz\n\n"
	require.NoError(t, parseSources(strings.NewReader(input), snapshot))
	require.Equal(t, "abc111", snapshot.Artifacts["pool/main/h/hello/hello_1.0.dsc"].Identity)
	require.Equal(t, "def222", snapshot.Artifacts["pool/main/h/hello/hello_1.0.orig.tar.xz"].Identity)
	require.Equal(t, "abc111", snapshot.Auxiliary["pool/main/h/hello/hello_1.0.dsc.sha256"].Identity)
	require.Equal(t, "def222", snapshot.Auxiliary["pool/main/h/hello/hello_1.0.orig.tar.xz.md5sum"].Identity)
}

func TestDiscovererDetectsDebianSuiteRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("dists/bookworm/main/binary-amd64/Packages.xz")
	require.True(t, ok)
	require.Equal(t, "bookworm", spec.Key())

	root := spec.(*rootSpec)
	require.Equal(t, []string{"main"}, root.Components)
	require.Equal(t, []string{"amd64"}, root.Architectures)
}

func TestRootSpecMergeAggregatesDebianShards(t *testing.T) {
	root := &rootSpec{
		Suite:         "bookworm",
		Components:    []string{"main"},
		Architectures: []string{"amd64"},
	}
	changed := root.Merge(&rootSpec{
		Suite:         "bookworm",
		Components:    []string{"contrib"},
		Architectures: []string{"arm64"},
		Source:        true,
	})
	require.True(t, changed)
	require.ElementsMatch(t, []string{"main", "contrib"}, root.Components)
	require.ElementsMatch(t, []string{"amd64", "arm64"}, root.Architectures)
	require.True(t, root.Source)
}

func TestDiscovererRejectsDebianPoolArtifact(t *testing.T) {
	_, ok := (discoverer{}).Discover("pool/main/h/hello/hello_1.0_amd64.deb")
	require.False(t, ok)
}
