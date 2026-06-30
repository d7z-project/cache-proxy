package filerepo

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

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

func TestRestoreGenerationsMarksRecoveredRootActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)

	snapshot := &LiveSnapshot{
		RootKey:    "root",
		Generation: "gen1",
		Upstream:   "https://upstream.example",
		Published:  time.Now().UTC(),
		Metadata: map[string]MetadataObject{
			"meta/index.txt": {
				Path:      "meta/index.txt",
				Required:  true,
				StorePath: handler.generationMetadataPath("root", "gen1", "meta/index.txt"),
			},
		},
		Targets: []MetadataTarget{{URL: "meta/index.txt"}},
	}

	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.snapshotPath("root", "gen1"))), 0o755))
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.currentPath("root"))), 0o755))

	data, err := yaml.Marshal(snapshot)
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.snapshotPath("root", "gen1"), bytes.NewReader(data), nil)
	require.NoError(t, err)

	refData, err := yaml.Marshal(struct {
		RootKey    string `yaml:"root_key"`
		Generation string `yaml:"generation"`
	}{RootKey: "root", Generation: "gen1"})
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.currentPath("root"), bytes.NewReader(refData), nil)
	require.NoError(t, err)

	handler.restoreGenerations(ctx)

	releases := handler.RootReleases()
	require.Len(t, releases, 1)
	require.True(t, releases[0].HasCurrent)
	require.Equal(t, "active", releases[0].State)

	stats := handler.stats.Snapshot()
	require.Equal(t, "ready", stats.Instances["repo"].MetadataState)

	rh, ok := handler.sh.ResourceHealth("root")
	require.True(t, ok)
	require.Equal(t, health.RActive, rh.State)
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

func TestRefreshSkipsRebuildWhenMetadataUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var headRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", `"meta-v1"`)
			_, _ = io.WriteString(w, "index")
		case http.MethodHead:
			headRequests++
			if r.Header.Get("If-None-Match") == `"meta-v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := newTestStore(t)
	builderCalls := 0
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			builderCalls++
			blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
		nil,
	)

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	require.Equal(t, 1, builderCalls)
	first := handler.rootSnapshot("root")
	require.NotNil(t, first)
	require.Equal(t, "meta/index.txt", first.Targets[0].URL)
	obj, ok := first.Metadata["meta/index.txt"]
	require.True(t, ok)
	require.NotEmpty(t, obj.StorePath)
	info, err := store.StatObject(ctx, "repo", obj.StorePath)
	require.NoError(t, err)
	require.Equal(t, `"meta-v1"`, info.Options["etag"])

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	require.Equal(t, 1, builderCalls)
	require.Equal(t, 1, headRequests)
	stats := handler.stats.Snapshot()
	require.Equal(t, "ready", stats.Instances["repo"].MetadataState)
}

func TestCanSkipRefreshReturnsFalseWhenSnapshotNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)

	skip, err := handler.canSkipRefresh(ctx, nil, "https://upstream.example", []MetadataTarget{{URL: "test.txt"}})
	require.NoError(t, err)
	require.False(t, skip)
}

func TestCanSkipRefreshReturnsFalseWhenEmptyTargets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)

	snapshot := &LiveSnapshot{
		Metadata: map[string]MetadataObject{},
		Targets:  []MetadataTarget{},
	}
	skip, err := handler.canSkipRefresh(ctx, snapshot, "https://upstream.example", nil)
	require.NoError(t, err)
	require.False(t, skip)
}

func TestResolveSnapshotMetadataNilSnapshot(t *testing.T) {
	_, ok := resolveSnapshotMetadata(nil, MetadataTarget{URL: "test.txt"})
	require.False(t, ok)
}

func TestResolveSnapshotMetadataDirectMatch(t *testing.T) {
	snapshot := &LiveSnapshot{
		Metadata: map[string]MetadataObject{
			"meta/index.txt": {Path: "meta/index.txt", StorePath: "/store/path", Required: true},
		},
	}
	obj, ok := resolveSnapshotMetadata(snapshot, MetadataTarget{URL: "meta/index.txt"})
	require.True(t, ok)
	require.Equal(t, "/store/path", obj.StorePath)
}

func TestResolveSnapshotMetadataResolvedPath(t *testing.T) {
	snapshot := &LiveSnapshot{
		Metadata: map[string]MetadataObject{
			"meta/index.txt":    {Path: "meta/real.txt", Required: true},
			"meta/real.txt":     {Path: "meta/real.txt", StorePath: "/store/path", Required: true},
		},
	}
	obj, ok := resolveSnapshotMetadata(snapshot, MetadataTarget{URL: "meta/index.txt"})
	require.True(t, ok)
	require.Equal(t, "/store/path", obj.StorePath)
}

func TestMarkResourceActiveOnRemovedNoop(t *testing.T) {
	healthCfg := health.DefaultConfig()
	sh := health.New("test", "test", healthCfg, []string{"https://upstream.example"}, nil, "test")
	rh := sh.AddResource("root", nil, []string{"https://upstream.example"})
	require.Equal(t, health.RPending, rh.State)

	rh.State = health.RRemoved
	sh.MarkResourceActive("root", nil)
	_, ok := sh.ResourceHealth("root")
	require.True(t, ok)
	rh2, _ := sh.ResourceHealth("root")
	require.Equal(t, health.RRemoved, rh2.State)
}

func TestRefreshSkipsRebuildWhenMetadataHeadReturns200(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var headRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", `"meta-v2"`)
			_, _ = io.WriteString(w, "index")
		case http.MethodHead:
			headRequests++
			w.Header().Set("ETag", `"meta-v2"`)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := newTestStore(t)
	builderCalls := 0
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			builderCalls++
			blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
		nil,
	)

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	require.Equal(t, 1, builderCalls)
	first := handler.rootSnapshot("root")
	require.NotNil(t, first)
	obj, ok := first.Metadata["meta/index.txt"]
	require.True(t, ok)
	info, err := store.StatObject(ctx, "repo", obj.StorePath)
	require.NoError(t, err)
	require.Equal(t, `"meta-v2"`, info.Options["etag"])

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	require.Equal(t, 2, builderCalls, "should rebuild since ETag changed")
}
