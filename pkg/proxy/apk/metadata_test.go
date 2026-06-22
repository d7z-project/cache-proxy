package apk

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestMetadataTargetsExpandRepositories(t *testing.T) {
	targets, upstreams, err := metadataTargets([]Repository{{
		URL:           "https://dl-cdn.alpinelinux.org/alpine",
		Branches:      []string{"v3.20"},
		Repos:         []string{"main"},
		Architectures: []string{"x86_64"},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"https://dl-cdn.alpinelinux.org/alpine"}, upstreams)
	require.Equal(t, []filerepo.MetadataTarget{{URL: "v3.20/main/x86_64/APKINDEX.tar.gz"}}, targets)
}

func TestMetadataTargetsRejectMixedBranchForms(t *testing.T) {
	_, _, err := metadataTargets([]Repository{{
		URL:           "https://dl-cdn.alpinelinux.org/alpine",
		Branch:        "v3.20",
		Branches:      []string{"v3.21"},
		Repos:         []string{"main"},
		Architectures: []string{"x86_64"},
	}})
	require.ErrorContains(t, err, "must not set both branch and branches")
}

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
