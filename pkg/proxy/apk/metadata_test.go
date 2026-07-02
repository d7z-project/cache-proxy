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

func TestDiscovererDetectsAPKRoot(t *testing.T) {
	result := (discoverer{}).Discover("mirror/alpine/v3.20/main/x86_64/APKINDEX.tar.gz")
	require.True(t, result.Matched)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "mirror/alpine/v3.20/main/x86_64", result.Root.ID)
}
