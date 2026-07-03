package pacman

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParseDescExtractsFilename(t *testing.T) {
	filename, err := parseDesc(strings.NewReader("%FILENAME%\nhello-1.0-1-any.pkg.tar.zst\n"))
	require.NoError(t, err)
	require.Equal(t, "hello-1.0-1-any.pkg.tar.zst", filename)
}

func TestAnalyzerDetectsPacmanRoot(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/core/os/x86_64/core.db")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, "pacman:mirror/core/os/x86_64", result.Root.ID)
	require.Equal(t, "core", result.Root.Repo)
	require.Equal(t, filerepo.LayoutPacman, result.Root.Layout)
}

func TestAnalyzerClassifiesPacmanFilesAsMetadataWithoutDiscovery(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/core/os/x86_64/core.files")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}

func TestAnalyzerSupportsCompressedDatabaseAtRoot(t *testing.T) {
	result := (inspector{}).InspectPath("core.db.tar.zst")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "pacman:/", result.Root.ID)
	require.Equal(t, []string{"core.db.tar.zst"}, result.Root.PrimaryMetadata)
}

func TestAnalyzerClassifiesCompressedFilesDatabaseAsMetadata(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/core.db.tar.zst")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)

	files := (inspector{}).InspectPath("mirror/core.files.tar.zst")
	require.Equal(t, filerepo.ResourceMetadata, files.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, files.Role)
}

func TestParseDescWithoutFilenameReturnsEmpty(t *testing.T) {
	filename, err := parseDesc(strings.NewReader("%NAME%\nmultilib-staging\n"))
	require.NoError(t, err)
	require.Empty(t, filename)
}
