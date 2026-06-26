package rpm

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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

func TestRefreshKeepsRepodataCompanionsDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/9/BaseOS/x86_64/os/repodata/repomd.xml":
			_, _ = w.Write([]byte(`
<repomd>
  <data type="primary">
    <location href="repodata/primary.xml.gz"/>
  </data>
  <data type="filelists">
    <location href="repodata/filelists.xml.gz"/>
  </data>
</repomd>`))
		case "/9/BaseOS/x86_64/os/repodata/primary.xml.gz":
			_, _ = w.Write(mustGzip(t, `
<metadata>
  <package>
    <checksum>abc123</checksum>
    <location href="Packages/h/hello-1.0-1.x86_64.rpm"/>
  </package>
</metadata>`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := filerepo.NewIndexedHandler(
		"repo",
		"rpm",
		"repo",
		config.Freshness(time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{RepoPath: "9/BaseOS/x86_64/os"}},
		buildSnapshot,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
	)

	require.NoError(t, handler.Refresh(ctx))
	require.NoError(t, store.MkdirAll("repo/repo/9/BaseOS/x86_64/os/repodata", 0o755))
	for _, name := range []string{
		"repo/9/BaseOS/x86_64/os/repodata/repomd.xml.asc",
		"repo/9/BaseOS/x86_64/os/repodata/filelists.xml.gz",
	} {
		_, err = store.Put(ctx, "repo", name, strings.NewReader("data"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
		require.NoError(t, err)
	}
	require.NoError(t, handler.Cleanup(ctx))
	for _, name := range []string{
		"repo/9/BaseOS/x86_64/os/repodata/repomd.xml.asc",
		"repo/9/BaseOS/x86_64/os/repodata/filelists.xml.gz",
	} {
		_, err = store.OpenObject(ctx, "repo", name)
		require.NoError(t, err)
	}
}

func TestRefreshInvalidatesCompanionAfterRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/9/BaseOS/x86_64/os/repodata/repomd.xml":
			_, _ = w.Write([]byte(`
<repomd>
  <data type="primary">
    <location href="repodata/primary.xml.gz"/>
  </data>
</repomd>`))
		case "/9/BaseOS/x86_64/os/repodata/primary.xml.gz":
			_, _ = w.Write(mustGzip(t, `
<metadata>
  <package>
    <checksum>abc123</checksum>
    <location href="Packages/h/hello-1.0-1.x86_64.rpm"/>
  </package>
</metadata>`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := filerepo.NewIndexedHandler(
		"repo",
		"rpm",
		"repo",
		config.Freshness(time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{RepoPath: "9/BaseOS/x86_64/os"}},
		buildSnapshot,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
	)

	require.NoError(t, store.MkdirAll("repo/repo/9/BaseOS/x86_64/os/repodata", 0o755))
	_, err = store.Put(ctx, "repo", "repo/9/BaseOS/x86_64/os/repodata/repomd.xml.asc", strings.NewReader("data"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)

	require.NoError(t, handler.Refresh(ctx))

	_, err = store.OpenObject(ctx, "repo", "repo/9/BaseOS/x86_64/os/repodata/repomd.xml.asc")
	require.Error(t, err, "companion should be invalidated after refresh")
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
