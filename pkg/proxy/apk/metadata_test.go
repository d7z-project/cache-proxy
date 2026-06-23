package apk

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParseIndexBuildsArtifactsAndAuxiliary(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata:  map[string]struct{}{},
		Artifacts: map[string]string{},
		Auxiliary: map[string]string{},
	}
	input := "P:busybox\nV:1.36.1-r2\nC:sha256:abc\n\n"
	require.NoError(t, parseIndex("v3.20/main/x86_64", strings.NewReader(input), snapshot))
	require.Equal(t, "sha256:abc", snapshot.Artifacts["v3.20/main/x86_64/busybox-1.36.1-r2.apk"])
	require.Equal(t, "sha256:abc", snapshot.Auxiliary["v3.20/main/x86_64/busybox-1.36.1-r2.apk.sig"])
}

func TestDiscovererDetectsAPKRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("v3.20/main/x86_64/APKINDEX.tar.gz")
	require.True(t, ok)
	require.Equal(t, "v3.20|main|x86_64", spec.Key())
}

func TestDiscovererRejectsAPKArtifactPath(t *testing.T) {
	_, ok := (discoverer{}).Discover("v3.20/main/x86_64/busybox-1.0.apk")
	require.False(t, ok)
}
