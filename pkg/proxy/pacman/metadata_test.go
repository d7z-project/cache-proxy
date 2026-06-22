package pacman

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestMetadataTargetsExpandRepositories(t *testing.T) {
	targets, upstreams, err := metadataTargets([]Repository{{
		URL:           "https://mirror.rackspace.com/archlinux",
		Repos:         []string{"core"},
		Architectures: []string{"x86_64"},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"https://mirror.rackspace.com/archlinux"}, upstreams)
	require.Equal(t, []filerepo.MetadataTarget{{
		URL:  "core/os/x86_64/core.db",
		Repo: "core",
		Arch: "x86_64",
	}}, targets)
}

func TestParseDescExtractsFilenameAndChecksum(t *testing.T) {
	filename, checksum, err := parseDesc(strings.NewReader("%FILENAME%\nfoo-1.0-1-x86_64.pkg.tar.zst\n%SHA256SUM%\nabc123\n"))
	require.NoError(t, err)
	require.Equal(t, "foo-1.0-1-x86_64.pkg.tar.zst", filename)
	require.Equal(t, "abc123", checksum)
}
