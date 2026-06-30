package filerepo

import (
	"context"
	"io"
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
)

func newTestStore(t *testing.T) *blobfs.Store {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string, builder SnapshotBuilder, rebuild CleanupIndexBuilder) *IndexedHandler {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		func(cleanPath string) ResourceClass {
			switch {
			case strings.HasPrefix(cleanPath, "meta/"):
				return ResourceMetadata
			case strings.HasSuffix(cleanPath, ".sig"):
				return ResourceAuxiliary
			default:
				return ResourceArtifact
			}
		},
		upstreams,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		nil,
		builder,
		rebuild,
		store,
		stats,
		health.New("repo", "test", health.DefaultConfig(), upstreams, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRoot("root", []MetadataTarget{{URL: "meta/index.txt"}})
	return handler
}

func TestRefreshPublishesSnapshotAndCleanupUsesRebuiltPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = io.WriteString(w, "index")
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			paths.AddAuxiliary("pool/pkg.deb")
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: 1,
			}, nil
		},
		func(ctx context.Context, session *LocalSession, paths *PathIndexBuilder) error {
			_, err := session.Fetch(MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			paths.AddAuxiliary("pool/pkg.deb")
			return nil
		},
	)

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	releases := handler.RootReleases()
	require.Len(t, releases, 1)
	require.Equal(t, 1, releases[0].ArtifactCount)

	require.NoError(t, store.MkdirAll("repo/repo/pool", 0o755))
	_, err := store.Put(ctx, "repo", "repo/pool/pkg.deb", strings.NewReader("keep"), map[string]string{"indexed": "true"})
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", "repo/pool/old.deb", strings.NewReader("drop"), map[string]string{"indexed": "true"})
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, config.DefaultCleanupConfig()))

	_, err = store.OpenObject(ctx, "repo", "repo/pool/pkg.deb")
	require.NoError(t, err)
	_, err = store.OpenObject(ctx, "repo", "repo/pool/old.deb")
	require.Error(t, err)
}

func TestRootReleasesIncludesPendingAndRefreshingRoots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = io.WriteString(w, "index")
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store := newTestStore(t)
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-unblock
			blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: 1,
			}, nil
		},
		nil,
	)

	refreshErr := make(chan error, 1)
	go func() {
		refreshErr <- handler.RefreshSubPath(ctx, "root")
	}()
	<-started

	releases := handler.RootReleases()
	require.Len(t, releases, 1)
	require.Equal(t, "root", releases[0].Key)
	require.False(t, releases[0].HasCurrent)
	require.True(t, releases[0].Refreshing)

	stats := handler.stats.Snapshot()
	require.Equal(t, "bootstrapping", stats.Instances["repo"].MetadataState)

	close(unblock)
	require.NoError(t, <-refreshErr)

	releases = handler.RootReleases()
	require.Len(t, releases, 1)
	require.True(t, releases[0].HasCurrent)
	require.False(t, releases[0].Refreshing)
	require.Equal(t, "active", releases[0].State)
}

func TestRefreshFailureKeepsBootingStateWithoutCurrentGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"},
		func(context.Context, *RefreshSession, *PathIndexBuilder) (*LiveSnapshot, error) {
			return nil, errMetadataTransient
		},
		nil,
	)

	require.Error(t, handler.RefreshSubPath(ctx, "root"))
	require.Nil(t, handler.currentSnapshot())

	stats := handler.stats.Snapshot()
	require.Equal(t, "booting", stats.Instances["repo"].MetadataState)
}

func TestPathIndexBuilderFinalizesSortedUniquePaths(t *testing.T) {
	builder := &PathIndexBuilder{}
	builder.Add("pool/b.deb")
	builder.Add("pool/a.deb")
	builder.Add("pool/b.deb")
	builder.AddAuxiliary("pool/a.deb")

	require.Equal(t, []string{
		"pool/a.deb",
		"pool/a.deb.asc",
		"pool/a.deb.gpg",
		"pool/a.deb.md5",
		"pool/a.deb.md5sum",
		"pool/a.deb.sha256",
		"pool/a.deb.sha512",
		"pool/a.deb.sig",
		"pool/b.deb",
	}, builder.Finalize())
}
