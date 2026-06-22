package rpm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestMetadataTargetsExpandRepositories(t *testing.T) {
	targets, upstreams, err := metadataTargets([]Repository{{
		URL:   "https://download.rockylinux.org/pub/rocky",
		Paths: []string{"9/BaseOS/x86_64/os"},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"https://download.rockylinux.org/pub/rocky"}, upstreams)
	require.Equal(t, []filerepo.MetadataTarget{{URL: "9/BaseOS/x86_64/os/repodata/repomd.xml"}}, targets)
}

func TestParsePrimaryRecordsArtifactsAndAuxiliary(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{
		Artifacts: map[string]string{},
		Auxiliary: map[string]string{},
	}
	input := `
<metadata>
  <package>
    <checksum>abc123</checksum>
    <location href="Packages/h/hello-1.0-1.x86_64.rpm"/>
  </package>
</metadata>`
	require.NoError(t, parsePrimary(strings.NewReader(input), snapshot))
	require.Equal(t, "abc123", snapshot.Artifacts["Packages/h/hello-1.0-1.x86_64.rpm"])
	require.Equal(t, "abc123", snapshot.Auxiliary["Packages/h/hello-1.0-1.x86_64.rpm.sig"])
	require.Equal(t, "abc123", snapshot.Auxiliary["Packages/h/hello-1.0-1.x86_64.rpm.sha256"])
}
