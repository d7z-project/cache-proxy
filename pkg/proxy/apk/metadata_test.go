package apk

import (
	"archive/tar"
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

func TestRefreshInvalidatesCompanionAfterRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v3.20/main/x86_64/APKINDEX.tar.gz":
			_, _ = w.Write(mustGzipTar(t, "APKINDEX", "P:busybox\nV:1.36.1-r2\nC:sha256:abc\n\n"))
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
		"apk",
		"repo",
		config.Freshness(time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour, Timeout: time.Second},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{Branch: "v3.20", Repo: "main", Arch: "x86_64"}},
		buildSnapshot,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
	)

	require.NoError(t, store.MkdirAll("repo/repo/v3.20/main/x86_64", 0o755))
	_, err = store.Put(ctx, "repo", "repo/v3.20/main/x86_64/APKINDEX.tar.gz.sig", strings.NewReader("sig"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)

	require.NoError(t, handler.Refresh(ctx))

	_, err = store.OpenObject(ctx, "repo", "repo/v3.20/main/x86_64/APKINDEX.tar.gz.sig")
	require.Error(t, err, "companion should be invalidated after refresh")
}

func mustGzipTar(t *testing.T, name, body string) []byte {
	t.Helper()

	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	tarWriter := tar.NewWriter(gzipWriter)
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(body)),
	}))
	_, err := tarWriter.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	return compressed.Bytes()
}
