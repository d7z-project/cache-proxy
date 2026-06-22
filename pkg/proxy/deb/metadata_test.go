package deb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestMetadataTargetsExpandRepositories(t *testing.T) {
	targets, upstreams, err := metadataTargets([]Repository{{
		URL:           "https://deb.debian.org/debian",
		Suites:        []string{"bookworm"},
		Components:    []string{"main"},
		Architectures: []string{"amd64"},
		Source:        true,
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"https://deb.debian.org/debian"}, upstreams)
	require.Len(t, targets, 3)
	require.Equal(t, "dists/bookworm/InRelease", targets[0].URL)
	require.Equal(t, []string{"dists/bookworm/Release"}, targets[0].Candidates)
	require.Equal(t, "dists/bookworm/main/binary-amd64/Packages.xz", targets[1].URL)
	require.Equal(t, []string{"dists/bookworm/main/binary-amd64/Packages.gz", "dists/bookworm/main/binary-amd64/Packages"}, targets[1].Candidates)
	require.Equal(t, "dists/bookworm/main/source/Sources.xz", targets[2].URL)
}

func TestMetadataTargetsRejectMixedSuiteForms(t *testing.T) {
	_, _, err := metadataTargets([]Repository{{
		URL:           "https://deb.debian.org/debian",
		Suite:         "bookworm",
		Suites:        []string{"bookworm-updates"},
		Components:    []string{"main"},
		Architectures: []string{"amd64"},
	}})
	require.ErrorContains(t, err, "must not set both suite and suites")
}

func TestParsePackagesRecordsArtifactChecksum(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]string{}}
	input := "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: abc123\n\n"
	require.NoError(t, parsePackages(strings.NewReader(input), snapshot))
	require.Equal(t, "abc123", snapshot.Artifacts["pool/main/h/hello/hello_1.0_amd64.deb"])
}

func TestParseSourcesRecordsArtifacts(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]string{}}
	input := "Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc111 123 hello_1.0.dsc\n def222 456 hello_1.0.orig.tar.xz\n\n"
	require.NoError(t, parseSources(strings.NewReader(input), snapshot))
	require.Equal(t, "abc111", snapshot.Artifacts["pool/main/h/hello/hello_1.0.dsc"])
	require.Equal(t, "def222", snapshot.Artifacts["pool/main/h/hello/hello_1.0.orig.tar.xz"])
}
