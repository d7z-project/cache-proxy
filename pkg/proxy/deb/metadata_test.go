package deb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParsePackagesRecordsArtifactChecksum(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]filerepo.RepoObject{}}
	input := "Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: abc123\n\n"
	require.NoError(t, parsePackages(strings.NewReader(input), snapshot))
	require.Equal(t, "abc123", snapshot.Artifacts["pool/main/h/hello/hello_1.0_amd64.deb"].Identity)
}

func TestParseSourcesRecordsArtifacts(t *testing.T) {
	snapshot := &filerepo.LiveSnapshot{Artifacts: map[string]filerepo.RepoObject{}}
	input := "Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc111 123 hello_1.0.dsc\n def222 456 hello_1.0.orig.tar.xz\n\n"
	require.NoError(t, parseSources(strings.NewReader(input), snapshot))
	require.Equal(t, "abc111", snapshot.Artifacts["pool/main/h/hello/hello_1.0.dsc"].Identity)
	require.Equal(t, "def222", snapshot.Artifacts["pool/main/h/hello/hello_1.0.orig.tar.xz"].Identity)
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

func TestRootSpecTargetsOnlyRelease(t *testing.T) {
	root := &rootSpec{
		Suite:         "bookworm",
		Components:    []string{"main"},
		Architectures: []string{"amd64"},
		Source:        true,
	}
	targets := root.Targets()
	require.Len(t, targets, 1)
	require.Equal(t, "dists/bookworm/InRelease", targets[0].URL)
	require.Equal(t, "release", targets[0].Kind)
}

func TestDiscovererRejectsDebianPoolArtifact(t *testing.T) {
	_, ok := (discoverer{}).Discover("pool/main/h/hello/hello_1.0_amd64.deb")
	require.False(t, ok)
}

func TestReleaseIndexTargetsPreferXZOverGZAndPlain(t *testing.T) {
	sums := map[string]string{
		"main/binary-amd64/Packages":    "plain",
		"main/binary-amd64/Packages.gz": "gz",
		"main/binary-amd64/Packages.xz": "xz",
		"main/source/Sources.gz":        "src",
	}
	targets := releaseIndexTargets("dists/bookworm/InRelease", sums)
	require.Len(t, targets, 2)
	require.Equal(t, "dists/bookworm/main/binary-amd64/Packages.xz", targets[0].URL)
	require.Equal(t, []string{
		"dists/bookworm/main/binary-amd64/Packages.gz",
		"dists/bookworm/main/binary-amd64/Packages",
	}, targets[0].Candidates)
	require.Equal(t, "packages", targets[0].Kind)
	require.Equal(t, "dists/bookworm/main/source/Sources.gz", targets[1].URL)
	require.Equal(t, "sources", targets[1].Kind)
}

func TestMetadataRequestInReleasePublishesArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	packages := []byte("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: " + sha256Hex([]byte("deb-body")) + "\n\n")
	release := []byte("Suite: bookworm\nComponents: main\nArchitectures: amd64\nSHA256:\n " + sha256Hex(packages) + fmt.Sprintf(" %d main/binary-amd64/Packages\n", len(packages)))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/bookworm/InRelease":
			_, _ = w.Write(release)
		case "/dists/bookworm/main/binary-amd64/Packages":
			_, _ = w.Write(packages)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"repo", "deb", "deb",
		0, classify,
		[]string{server.URL}, nil,
		0, &filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{}, nil, buildSnapshot,
		store, stats,
		health.New("repo", "deb", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
	)
	require.NoError(t, handler.Start(ctx))
	defer func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, handler.Stop(stopCtx))
	}()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dists/bookworm/InRelease", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	require.Eventually(t, func() bool {
		releases := handler.RootReleases()
		return len(releases) == 1 && releases[0].ArtifactCount == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestMetadataRequestInReleaseRedirectsToReleaseFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	packages := []byte("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: " + sha256Hex([]byte("deb-body")) + "\n\n")
	release := []byte("Suite: bookworm\nComponents: main\nArchitectures: amd64\nSHA256:\n " + sha256Hex(packages) + fmt.Sprintf(" %d main/binary-amd64/Packages\n", len(packages)))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/bookworm/InRelease":
			http.NotFound(w, r)
		case "/dists/bookworm/Release":
			_, _ = w.Write(release)
		case "/dists/bookworm/main/binary-amd64/Packages":
			_, _ = w.Write(packages)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"repo", "deb", "deb",
		0, classify,
		[]string{server.URL}, nil,
		0, &filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{}, nil, buildSnapshot,
		store, stats,
		health.New("repo", "deb", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
	)
	require.NoError(t, handler.Start(ctx))
	defer func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, handler.Stop(stopCtx))
	}()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dists/bookworm/InRelease", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)

	require.Eventually(t, func() bool {
		releases := handler.RootReleases()
		return len(releases) == 1 && releases[0].ArtifactCount == 1
	}, 2*time.Second, 10*time.Millisecond)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dists/bookworm/InRelease", nil))
	require.Equal(t, http.StatusFound, rec.Code)
	require.Equal(t, "/dists/bookworm/Release", rec.Header().Get("Location"))
}

func TestMetadataRequestStartsAsyncRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	packages := []byte("Package: hello\nFilename: pool/main/h/hello/hello_1.0_amd64.deb\nSHA256: " + sha256Hex([]byte("deb-body")) + "\n\n")
	release := []byte("Suite: bookworm\nComponents: main\nArchitectures: amd64\nSHA256:\n " + sha256Hex(packages) + fmt.Sprintf(" %d main/binary-amd64/Packages\n", len(packages)))
	packagesRequested := make(chan struct{})
	unblockPackages := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/bookworm/InRelease":
			_, _ = w.Write(release)
		case "/dists/bookworm/main/binary-amd64/Packages":
			closeOnce(packagesRequested)
			<-unblockPackages
			_, _ = w.Write(packages)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"repo", "deb", "deb",
		0, classify,
		[]string{server.URL}, nil,
		0, &filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		discoverer{}, nil, buildSnapshot,
		store, stats,
		health.New("repo", "deb", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
	)
	require.NoError(t, handler.Start(ctx))
	defer func() {
		cancel()
		closeOnce(unblockPackages)
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, handler.Stop(stopCtx))
	}()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dists/bookworm/InRelease", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(release), rec.Body.String())

	require.Eventually(t, func() bool {
		select {
		case <-packagesRequested:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	closeOnce(unblockPackages)
	require.Eventually(t, func() bool {
		releases := handler.RootReleases()
		return len(releases) == 1 && releases[0].ArtifactCount == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestBuildSnapshotRejectsReleaseWithoutPackageIndexes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	release := []byte("Suite: bookworm\nSHA256:\n " + sha256Hex([]byte("ignored")) + " 7 main/installer-amd64/current/images/SHA256SUMS\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dists/bookworm/InRelease" {
			_, _ = w.Write(release)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"repo", "deb", "deb",
		0, classify,
		[]string{server.URL}, nil,
		0, &filerepo.Policy{},
		filerepo.RefreshPolicy{Interval: time.Hour},
		nil, []filerepo.RootSpec{&rootSpec{Suite: "bookworm"}}, buildSnapshot,
		store, stats,
		health.New("repo", "deb", health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
	)
	err = handler.Refresh(ctx)
	require.ErrorContains(t, err, "Release contains no package indexes")
	require.Empty(t, handler.RootReleases())
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func closeOnce(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}
