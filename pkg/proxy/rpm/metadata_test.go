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

func TestAnalyzerDetectsRPMRoot(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/repo/os/repodata/repomd.xml")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, "rpm:mirror/repo/os", result.Root.ID)
	require.Equal(t, filerepo.LayoutRPM, result.Root.Layout)
}

func TestAnalyzerClassifiesMirrorlistAsMetadataWithoutDiscovery(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/repo/os/mirrorlist")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}

func TestAnalyzerSupportsRootLevelRPMRepository(t *testing.T) {
	result := (inspector{}).InspectPath("repodata/repomd.xml")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "rpm:/", result.Root.ID)
	require.Empty(t, result.Root.Path)
	require.Equal(t, "/", result.Root.DisplayName)
}
