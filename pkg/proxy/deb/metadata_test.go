package deb

import (
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

func TestParsePackagesRecordsArtifactChecksum(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]string{}, Auxiliary: map[string]string{}}
	input := "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: abc123\n\n"
	require.NoError(t, parsePackages(strings.NewReader(input), snapshot))
	require.Equal(t, "abc123", snapshot.Artifacts["pool/main/h/hello/hello_1.0_amd64.deb"])
	require.Equal(t, "abc123", snapshot.Auxiliary["pool/main/h/hello/hello_1.0_amd64.deb.sha256"])
	require.Equal(t, "abc123", snapshot.Auxiliary["pool/main/h/hello/hello_1.0_amd64.deb.md5sum"])
}

func TestParseSourcesRecordsArtifacts(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]string{}, Auxiliary: map[string]string{}}
	input := "Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc111 123 hello_1.0.dsc\n def222 456 hello_1.0.orig.tar.xz\n\n"
	require.NoError(t, parseSources(strings.NewReader(input), snapshot))
	require.Equal(t, "abc111", snapshot.Artifacts["pool/main/h/hello/hello_1.0.dsc"])
	require.Equal(t, "def222", snapshot.Artifacts["pool/main/h/hello/hello_1.0.orig.tar.xz"])
	require.Equal(t, "abc111", snapshot.Auxiliary["pool/main/h/hello/hello_1.0.dsc.sha256"])
	require.Equal(t, "def222", snapshot.Auxiliary["pool/main/h/hello/hello_1.0.orig.tar.xz.md5sum"])
}

func TestDiscovererDetectsDebianSuiteRoot(t *testing.T) {
	spec, ok := (discoverer{}).Discover("dists/bookworm/main/binary-amd64/Packages.xz")
	require.True(t, ok)
	require.Equal(t, "bookworm", spec.Key())

	root := spec.(*rootSpec)
	require.Equal(t, []string{"main"}, root.Components)
	require.Equal(t, []string{"amd64"}, root.Architectures)
}

func TestRootSpecMergeAggregatesDebianShards(t *testing.T) {
	root := &rootSpec{
		Suite:         "bookworm",
		Components:    []string{"main"},
		Architectures: []string{"amd64"},
	}
	changed := root.Merge(&rootSpec{
		Suite:         "bookworm",
		Components:    []string{"contrib"},
		Architectures: []string{"arm64"},
		Source:        true,
	})
	require.True(t, changed)
	require.ElementsMatch(t, []string{"main", "contrib"}, root.Components)
	require.ElementsMatch(t, []string{"amd64", "arm64"}, root.Architectures)
	require.True(t, root.Source)
}

func TestDiscovererRejectsDebianPoolArtifact(t *testing.T) {
	_, ok := (discoverer{}).Discover("pool/main/h/hello/hello_1.0_amd64.deb")
	require.False(t, ok)
}

func TestRefreshKeepsReleaseSignatureDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/bookworm/InRelease":
			http.NotFound(w, r)
		case "/dists/bookworm/Release":
			_, _ = w.Write([]byte("Suite: bookworm\n"))
		case "/dists/bookworm/main/binary-amd64/Packages.xz":
			http.NotFound(w, r)
		case "/dists/bookworm/main/binary-amd64/Packages.gz":
			http.NotFound(w, r)
		case "/dists/bookworm/main/binary-amd64/Packages":
			_, _ = w.Write([]byte("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: abc123\n\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "deb", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test")
	handler := filerepo.NewIndexedHandler(
		"repo",
		"deb",
		"repo",
		config.Freshness(2*time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{Suite: "bookworm", Components: []string{"main"}, Architectures: []string{"amd64"}}},
		buildSnapshot,
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))
	require.NoError(t, store.MkdirAll("repo/repo/dists/bookworm", 0o755))
	_, err = store.Put(ctx, "repo", "repo/dists/bookworm/Release.gpg", strings.NewReader("sig"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	require.NoError(t, handler.Cleanup(ctx))
	_, err = store.OpenObject(ctx, "repo", "repo/dists/bookworm/Release.gpg")
	require.NoError(t, err)
}

func TestRefreshPrefetchesCompanion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/bookworm/InRelease":
			http.NotFound(w, r)
		case "/dists/bookworm/Release":
			_, _ = w.Write([]byte("Suite: bookworm\n"))
		case "/dists/bookworm/Release.gpg":
			_, _ = w.Write([]byte("gpg-sig"))
		case "/dists/bookworm/main/binary-amd64/Packages.xz":
			http.NotFound(w, r)
		case "/dists/bookworm/main/binary-amd64/Packages.gz":
			http.NotFound(w, r)
		case "/dists/bookworm/main/binary-amd64/Packages":
			_, _ = w.Write([]byte("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: abc123\n\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "deb", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test")
	handler := filerepo.NewIndexedHandler(
		"repo",
		"deb",
		"repo",
		config.Freshness(2*time.Minute),
		classify,
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{},
		[]filerepo.RootSpec{&rootSpec{Suite: "bookworm", Components: []string{"main"}, Architectures: []string{"amd64"}}},
		buildSnapshot,
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))
	_, err = store.OpenObject(ctx, "repo", "repo/dists/bookworm/Release.gpg")
	require.NoError(t, err, "companion should be pre-fetched during refresh")
}
