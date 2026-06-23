package rpm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

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

func TestDiscovererDetectsRPMRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("9/BaseOS/x86_64/os/repodata/repomd.xml")
	require.True(t, ok)
	require.Equal(t, "9/BaseOS/x86_64/os", spec.Key())
}

func TestDiscovererRejectsRPMArtifactPath(t *testing.T) {
	_, ok := (discoverer{}).Discover("Packages/h/hello-1.0-1.x86_64.rpm")
	require.False(t, ok)
}
