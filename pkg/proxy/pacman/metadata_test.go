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
	spec, ok := (discoverer{}).Discover("core/os/x86_64/core.db")
	require.True(t, ok)
	require.Equal(t, "core", spec.Key())
}
