package pacman

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

func TestParseDescExtractsFilenameAndChecksum(t *testing.T) {
	filename, checksum, err := parseDesc(strings.NewReader("%FILENAME%\nfoo-1.0-1-x86_64.pkg.tar.zst\n%SHA256SUM%\nabc123\n"))
	require.NoError(t, err)
	require.Equal(t, "foo-1.0-1-x86_64.pkg.tar.zst", filename)
	require.Equal(t, "abc123", checksum)
}

func TestDiscovererDetectsPacmanRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("core/os/x86_64/core.db.sig")
	require.True(t, ok)
	require.Equal(t, "core|x86_64", spec.Key())
}

func TestDiscovererRejectsPacmanArtifactPath(t *testing.T) {
	_, ok := (discoverer{}).Discover("pool/packages/foo-1.0-1-x86_64.pkg.tar.zst")
	require.False(t, ok)
}

func TestRefreshKeepsPacmanMetadataCompanionsDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/core/os/x86_64/core.db":
			_, _ = w.Write(mustGzipTar(t, "hello-1.0-1/desc", "%FILENAME%\nhello-1.0-1-x86_64.pkg.tar.zst\n%SHA256SUM%\nabc123\n"))
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
		"pacman",
		"repo",
		config.Freshness(time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{Repo: "core", Arch: "x86_64"}},
		buildSnapshot,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
	)

	require.NoError(t, handler.Refresh(ctx))
	require.NoError(t, store.MkdirAll("repo/repo/core/os/x86_64", 0o755))
	for _, name := range []string{
		"repo/core/os/x86_64/core.db.sig",
		"repo/core/os/x86_64/core.files",
		"repo/core/os/x86_64/core.files.sig",
	} {
		_, err = store.Put(ctx, "repo", name, strings.NewReader("data"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
		require.NoError(t, err)
	}
	require.NoError(t, handler.Cleanup(ctx))
	for _, name := range []string{
		"repo/core/os/x86_64/core.db.sig",
		"repo/core/os/x86_64/core.files",
		"repo/core/os/x86_64/core.files.sig",
	} {
		_, err = store.OpenObject(ctx, "repo", name)
		require.NoError(t, err)
	}
}

func TestRefreshInvalidatesCompanionsAfterRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/core/os/x86_64/core.db":
			_, _ = w.Write(mustGzipTar(t, "hello-1.0-1/desc", "%FILENAME%\nhello-1.0-1-x86_64.pkg.tar.zst\n%SHA256SUM%\nabc123\n"))
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
		"pacman",
		"repo",
		config.Freshness(time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{Repo: "core", Arch: "x86_64"}},
		buildSnapshot,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
	)

	require.NoError(t, store.MkdirAll("repo/repo/core/os/x86_64", 0o755))
	companions := []string{
		"repo/core/os/x86_64/core.db.sig",
		"repo/core/os/x86_64/core.files",
		"repo/core/os/x86_64/core.files.sig",
	}
	for _, name := range companions {
		_, err = store.Put(ctx, "repo", name, strings.NewReader("data"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
		require.NoError(t, err)
	}

	require.NoError(t, handler.Refresh(ctx))

	for _, name := range companions {
		_, err = store.OpenObject(ctx, "repo", name)
		require.Error(t, err, "companion %q should be invalidated after refresh", name)
	}
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
