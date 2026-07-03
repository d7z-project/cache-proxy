package apk

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParseIndexBuildsCleanupPaths(t *testing.T) {
	paths := &filerepo.PathIndexBuilder{}
	count, err := parseIndex("v3.20/main/x86_64", strings.NewReader("P:busybox\nV:1.36.1-r2\n\n"), paths)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, []string{
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.asc",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.gpg",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.md5",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.md5sum",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.sha256",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.sha512",
		"v3.20/main/x86_64/busybox-1.36.1-r2.apk.sig",
	}, paths.Finalize())
}

func TestAnalyzerDetectsAPKRoot(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/alpine/v3.20/main/x86_64/APKINDEX.tar.gz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "apk:mirror/alpine/v3.20/main/x86_64", result.Root.ID)
	require.Equal(t, "mirror/alpine/v3.20/main/x86_64", result.Root.Path)
	require.Equal(t, filerepo.LayoutAPK, result.Root.Layout)
	require.Empty(t, result.Root.Attributes)
}

func TestAnalyzerDetectsRootLevelAPKRepository(t *testing.T) {
	result := (inspector{}).InspectPath("APKINDEX.tar.gz")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "apk:/", result.Root.ID)
	require.Empty(t, result.Root.Path)
	require.Equal(t, "/", result.Root.DisplayName)
}

func TestAnalyzerClassifiesAPKSignatureAsAuxiliary(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/alpine/v3.20/main/x86_64/APKINDEX.tar.gz.sig")
	require.Equal(t, filerepo.ResourceAuxiliary, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}
