package pacman

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDescExtractsFilename(t *testing.T) {
	filename, err := parseDesc(strings.NewReader("%FILENAME%\nhello-1.0-1-any.pkg.tar.zst\n"))
	require.NoError(t, err)
	require.Equal(t, "hello-1.0-1-any.pkg.tar.zst", filename)
}

func TestDiscovererDetectsPacmanRoot(t *testing.T) {
	result := (discoverer{}).Discover("mirror/core/os/x86_64/core.db")
	require.True(t, result.Matched)
	require.Equal(t, "mirror/core/os/x86_64", result.Root.ID)
}

func TestParseDescWithoutFilenameReturnsEmpty(t *testing.T) {
	filename, err := parseDesc(strings.NewReader("%NAME%\nmultilib-staging\n"))
	require.NoError(t, err)
	require.Empty(t, filename)
}
