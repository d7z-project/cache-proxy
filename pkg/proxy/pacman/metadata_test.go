package pacman

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDescExtractsFilenameAndChecksum(t *testing.T) {
	filename, checksum, err := parseDesc(strings.NewReader("%FILENAME%\nfoo-1.0-1-x86_64.pkg.tar.zst\n%SHA256SUM%\nabc123\n"))
	require.NoError(t, err)
	require.Equal(t, "foo-1.0-1-x86_64.pkg.tar.zst", filename)
	require.Equal(t, "abc123", checksum)
}

func TestDiscovererDetectsPacmanRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("core/os/x86_64/core.db.sig")
	require.True(t, ok)
	require.Equal(t, "core|x86_64", spec.Key())
}

func TestDiscovererRejectsPacmanArtifactPath(t *testing.T) {
	_, ok := (discoverer{}).Discover("pool/packages/foo-1.0-1-x86_64.pkg.tar.zst")
	require.False(t, ok)
}
