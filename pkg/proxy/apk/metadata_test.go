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
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParseIndexBuildsArtifacts(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata:  map[string]filerepo.MetadataObject{},
		Artifacts: map[string]filerepo.RepoObject{},
	}
	input := "P:busybox\nV:1.36.1-r2\nC:sha256:abc\n\n"
	require.NoError(t, parseIndex("v3.20/main/x86_64", strings.NewReader(input), snapshot))
	artifact := snapshot.Artifacts["v3.20/main/x86_64/busybox-1.36.1-r2.apk"]
	require.Equal(t, "sha256:abc", artifact.Identity)
	require.Equal(t, "apk-control", artifact.Digest.Algorithm)
	require.False(t, artifact.Digest.Verifiable)
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

func TestRefreshPrefetchesCompanion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v3.20/main/x86_64/APKINDEX.tar.gz":
			_, _ = w.Write(mustGzipTar(t, "APKINDEX", "P:busybox\nV:1.36.1-r2\nC:sha256:abc\n\n"))
		case "/v3.20/main/x86_64/APKINDEX.tar.gz.sig":
			_, _ = w.Write([]byte("sig-data"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "apk", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test")
	handler := filerepo.NewIndexedHandler(
		"repo",
		"apk",
		"repo",
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		discoverer{},
		buildSnapshot,
		store,
		stats,
		svcHealth,
		nil,
	)
	handler.AddRoot("v3.20/main/x86_64", []filerepo.MetadataTarget{{URL: "v3.20/main/x86_64/APKINDEX.tar.gz"}})

	require.NoError(t, handler.RefreshSubPath(ctx, "v3.20/main/x86_64"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v3.20/main/x86_64/APKINDEX.tar.gz.sig", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "sig-data", rec.Body.String())
}

func TestRefreshSucceedsWithoutSig(t *testing.T) {
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

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "apk", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test")
	handler := filerepo.NewIndexedHandler(
		"repo", "apk", "repo",
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		discoverer{},
		buildSnapshot,
		store, stats, svcHealth, nil,
	)
	handler.AddRoot("v3.20/main/x86_64", []filerepo.MetadataTarget{{URL: "v3.20/main/x86_64/APKINDEX.tar.gz"}})

	require.NoError(t, handler.RefreshSubPath(ctx, "v3.20/main/x86_64"))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v3.20/main/x86_64/APKINDEX.tar.gz", nil))
	require.Equal(t, http.StatusOK, rec.Code, "main index should be served without sig companion")

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v3.20/main/x86_64/APKINDEX.tar.gz.sig", nil))
	require.Equal(t, http.StatusNotFound, rec.Code, "missing sig companion should return 404")
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
