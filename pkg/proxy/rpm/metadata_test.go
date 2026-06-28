package rpm

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParsePrimaryRecordsArtifacts(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{
		Artifacts: map[string]filerepo.RepoObject{},
	}
	input := `
<metadata>
  <package>
    <checksum>abc123</checksum>
    <location href="Packages/h/hello-1.0-1.x86_64.rpm"/>
  </package>
</metadata>`
	require.NoError(t, parsePrimary(strings.NewReader(input), snapshot, "repo/os"))
	require.Equal(t, "abc123", snapshot.Artifacts["repo/os/Packages/h/hello-1.0-1.x86_64.rpm"].Identity)
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

func mustGzip(t *testing.T, body string) []byte {
	t.Helper()

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}
