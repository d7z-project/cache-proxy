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
	"gopkg.d7z.net/cache-proxy/pkg/health"
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

func TestRefreshSucceedsWithoutCompanions(t *testing.T) {
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

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "pacman", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test")
	handler := filerepo.NewIndexedHandler(
		"repo", "pacman", "repo",
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
		store, stats, svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/core/os/x86_64/core.db", nil))
	require.Equal(t, http.StatusOK, rec.Code, "main database should be served without companions")

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/core/os/x86_64/core.db.sig", nil))
	require.Equal(t, http.StatusNotFound, rec.Code, "missing companion should return 404")

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/core/os/x86_64/core.files", nil))
	require.Equal(t, http.StatusNotFound, rec.Code, "missing companion should return 404")
}

func TestRefreshPrefetchesCompanions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/core/os/x86_64/core.db":
			_, _ = w.Write(mustGzipTar(t, "hello-1.0-1/desc", "%FILENAME%\nhello-1.0-1-x86_64.pkg.tar.zst\n%SHA256SUM%\nabc123\n"))
		case "/core/os/x86_64/core.db.sig":
			_, _ = w.Write([]byte("db-sig"))
		case "/core/os/x86_64/core.files":
			_, _ = w.Write([]byte("files-data"))
		case "/core/os/x86_64/core.files.sig":
			_, _ = w.Write([]byte("files-sig"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "pacman", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test")
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
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/core/os/x86_64/core.db.sig", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "db-sig", rec.Body.String())
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
