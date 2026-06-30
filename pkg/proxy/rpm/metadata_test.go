package rpm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParsePrimaryBuildsCleanupPaths(t *testing.T) {
	input := `<metadata><package><location href="Packages/h/hello.rpm"/><checksum>abc</checksum></package></metadata>`
	paths := &filerepo.PathIndexBuilder{}
	count, err := parsePrimary(strings.NewReader(input), paths, "repo/os")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Contains(t, paths.Finalize(), "repo/os/Packages/h/hello.rpm")
}

func TestDiscovererDetectsRPMRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("repo/os/repodata/repomd.xml")
	require.True(t, ok)
	require.Equal(t, "repo/os", spec.Key())
}
