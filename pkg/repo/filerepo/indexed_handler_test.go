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

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
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
		staticInspector(func(cleanPath string) DiscoveryResult {
			switch {
			case strings.HasPrefix(cleanPath, "meta/"):
				return DiscoveryResult{Class: ResourceMetadata}
			case strings.HasSuffix(cleanPath, ".sig"):
				return DiscoveryResult{Class: ResourceAuxiliary}
			default:
				return DiscoveryResult{Class: ResourceArtifact}
			}
		}),
		upstreams,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		builder,
		rebuild,
		store,
		stats,
		health.New("repo", "test", health.DefaultConfig(), upstreams, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRepository(testRepositoryRoot("root", "meta/index.txt"))
	return handler
}

type staticInspector func(string) DiscoveryResult

func (d staticInspector) InspectPath(cleanPath string) DiscoveryResult {
	return d(cleanPath)
}

type finalizingInspector struct {
	inspect func(string) DiscoveryResult
}

func (i finalizingInspector) InspectPath(cleanPath string) DiscoveryResult {
	return i.inspect(cleanPath)
}

func (finalizingInspector) FinalizeRoot(root RepositoryRoot) RepositoryRoot {
	root.Attributes = []RepositoryAttribute{{LabelKey: "repo_path", Value: root.Path}, {LabelKey: "layout", Value: root.Layout}}
	return root
}

func testRepositoryRoot(id, metadataPath string) RepositoryRoot {
	return RepositoryRoot{
		ID:              id,
		Path:            id,
		DisplayName:     id,
		PrimaryMetadata: []string{metadataPath},
		Targets:         []MetadataTarget{{URL: metadataPath}},
		Attributes:      []RepositoryAttribute{{LabelKey: "repo_path", Value: id}},
	}
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

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, 1, statuses[0].ArtifactCount)

	require.NoError(t, store.MkdirAll("repo/repo/pool", 0o755))
	_, err := store.Put(ctx, "repo", "repo/pool/pkg.deb", strings.NewReader("drop"), nil)
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", "repo/pool/old.deb", strings.NewReader("drop"), nil)
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, config.DefaultCleanupConfig()))

	_, err = store.OpenObject(ctx, "repo", "repo/pool/pkg.deb")
	require.Error(t, err)
	_, err = store.OpenObject(ctx, "repo", "repo/pool/old.deb")
	require.Error(t, err)
}

func TestServeHTTPPrefersCurrentGenerationMetadataCompanion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "index")
		case "/meta/index.txt.sig":
			_, _ = io.WriteString(w, "fresh-signature")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			companion, err := session.FetchDerived(ctx, "meta/index.txt.sig")
			require.NoError(t, err)
			return &LiveSnapshot{
				Metadata: map[string]MetadataObject{
					blob.Path:      {Path: blob.Path, Required: true},
					companion.Path: companion,
				},
			}, nil
		},
		nil,
	)

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	require.NoError(t, store.MkdirAll("repo/repo/meta", 0o755))
	_, err := store.Put(ctx, "repo", "repo/meta/index.txt.sig", strings.NewReader("stale-signature"), nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/meta/index.txt.sig", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "GENERATION", rec.Header().Get("X-Cache"))
	require.Equal(t, "fresh-signature", rec.Body.String())
}

func TestGenerationResolverUsesGenerationScopedObjectPath(t *testing.T) {
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
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: 1,
			}, nil
		},
		nil,
	)

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	firstRoute, err := (&generationResolver{handler: handler, policy: &Policy{}}).Resolve(
		httptest.NewRequest(http.MethodGet, "/pool/pkg.deb", nil),
	)
	require.NoError(t, err)
	require.Contains(t, firstRoute.ObjectPath, "/generations/")
	require.Contains(t, firstRoute.ObjectPath, "/artifacts/pool/pkg.deb")

	time.Sleep(time.Nanosecond)
	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	secondRoute, err := (&generationResolver{handler: handler, policy: &Policy{}}).Resolve(
		httptest.NewRequest(http.MethodGet, "/pool/pkg.deb", nil),
	)
	require.NoError(t, err)
	require.NotEqual(t, firstRoute.ObjectPath, secondRoute.ObjectPath)
}

func TestStartReconcilesMetadataTasksWithoutSchedulerState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	initial := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)
	initial.AddRepository(testRepositoryRoot("root", "meta/index.txt"))
	initial.saveState(ctx)

	b := bus.New()
	sched := scheduler.New(b, store, prometheus.NewRegistry())
	sched.RegisterFactory(scheduler.TaskFactory{
		Instance:        "repo",
		RefreshInterval: time.Hour,
		GCInterval:      6 * time.Hour,
		NewRefresh:      func(string) scheduler.TaskHandler { return func(context.Context) error { return nil } },
		NewGC:           func(string) scheduler.TaskHandler { return func(context.Context) error { return nil } },
	})

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)
	restored.SetBus(b)
	require.NoError(t, restored.Start(ctx))
	defer func() { require.NoError(t, restored.Stop(ctx)) }()

	sched.Start(ctx)
	defer func() { require.NoError(t, sched.Stop(ctx)) }()

	require.Eventually(t, func() bool {
		_, ok := sched.Info(scheduler.NewTaskKey("repo", scheduler.TypeMetadataRefresh, "root"))
		return ok
	}, 5*time.Second, 50*time.Millisecond)
}

func TestDiscoverRootIgnoresUpdateOnlyRootCreation(t *testing.T) {
	store := newTestStore(t)
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		staticInspector(func(string) DiscoveryResult {
			return DiscoveryResult{
				Class: ResourceMetadata,
				Role:  DiscoveryUpdateRoot,
				Root:  testRepositoryRoot("root", "meta/index.txt"),
			}
		}),
		[]string{"https://upstream.example"},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		nil,
		nil,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
		health.New("repo", "test", health.DefaultConfig(), []string{"https://upstream.example"}, nil, "cache-proxy-test"),
		nil,
	)

	rootID, created, changed := handler.registerRoot(handler.inspect("meta/index.txt"))
	require.Equal(t, "root", rootID)
	require.False(t, created)
	require.False(t, changed)
	require.Empty(t, handler.RepositoryStatuses())
}

func TestDiscoverRootMergesExistingRepositoryDetails(t *testing.T) {
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)
	handler.inspector = staticInspector(func(string) DiscoveryResult {
		return DiscoveryResult{
			Class: ResourceMetadata,
			Role:  DiscoveryUpdateRoot,
			Root: RepositoryRoot{
				ID:            "root",
				Path:          "root",
				DisplayName:   "root",
				Targets:       []MetadataTarget{{URL: "meta/index.txt"}},
				Architectures: []string{"amd64"},
				Attributes: []RepositoryAttribute{
					{LabelKey: "repo_path", Value: "root"},
					{LabelKey: "architecture", Value: "amd64"},
				},
			},
		}
	})

	rootID, created, changed := handler.registerRoot(handler.inspect("meta/index.txt"))
	require.Equal(t, "root", rootID)
	require.False(t, created)
	require.True(t, changed)
	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, "amd64", statuses[0].Attributes[1].Value)
}

func TestExistingRootMetadataUpdatePublishesDiscoveryEvent(t *testing.T) {
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)
	b := bus.New()
	sub := b.Subscribe(bus.EventMetadataDiscovered)
	handler.SetBus(b)
	handler.inspector = staticInspector(func(string) DiscoveryResult {
		return DiscoveryResult{
			Class: ResourceMetadata,
			Role:  DiscoveryUpdateRoot,
			Root: RepositoryRoot{
				ID:      "root",
				Path:    "root",
				Targets: []MetadataTarget{{URL: "meta/index.txt"}, {URL: "meta/extra.txt"}},
			},
		}
	})
	handler.setRootSnapshot("root", &LiveSnapshot{RootID: "root", Generation: "gen1"}, nil)

	req := httptest.NewRequest(http.MethodGet, "/meta/extra.txt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	select {
	case evt := <-sub:
		payload := evt.Payload.(bus.MetadataDiscoveredPayload)
		require.Equal(t, "repo", payload.Instance)
		require.Equal(t, "root", payload.RootID)
	case <-time.After(time.Second):
		require.FailNow(t, "expected discovery event for updated root")
	}
}

func TestRegisterRootFinalizesMergedRoot(t *testing.T) {
	store := newTestStore(t)
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		finalizingInspector{inspect: func(string) DiscoveryResult {
			return DiscoveryResult{
				Class: ResourceMetadata,
				Role:  DiscoveryCreateRoot,
				Root: RepositoryRoot{
					ID:     "flat:/",
					Path:   "",
					Layout: "flat",
				},
			}
		}},
		[]string{"https://upstream.example"},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		nil,
		nil,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
		health.New("repo", "test", health.DefaultConfig(), []string{"https://upstream.example"}, nil, "cache-proxy-test"),
		nil,
	)

	rootID, created, changed := handler.registerRoot(handler.inspect("meta/index.txt"))
	require.Equal(t, "flat:/", rootID)
	require.True(t, created)
	require.True(t, changed)
	handler.mu.RLock()
	entry := handler.roots[rootID]
	handler.mu.RUnlock()
	require.NotNil(t, entry)
	require.Equal(t, "", entry.root.Path)
	require.Equal(t, []RepositoryAttribute{
		{LabelKey: "repo_path", Value: ""},
		{LabelKey: "layout", Value: "flat"},
	}, entry.root.Attributes)
	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, []runtime.RepositoryAttribute{
		{LabelKey: "repo_path", Value: ""},
		{LabelKey: "layout", Value: "flat"},
	}, statuses[0].Attributes)
}

func TestSaveAndRestoreRootsWithoutCurrentGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)
	handler.AddRepository(testRepositoryRoot("root", "meta/index.txt"))
	handler.sh.AddResource("root", targetsToProbe([]MetadataTarget{{URL: "meta/index.txt"}}), []string{"https://upstream.example"})
	handler.saveState(ctx)

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)
	restored.restoreRoots(ctx)

	statuses := restored.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, "root", statuses[0].ID)
	require.False(t, statuses[0].HasCurrent)

	info, ok := restored.sh.ResourceHealth("root")
	require.True(t, ok)
	require.Equal(t, "root", info.Path)
	require.Len(t, info.LastTargets, 0)
	require.Equal(t, health.RPending, info.State)

	restored.mu.RLock()
	entry := restored.roots["root"]
	restored.mu.RUnlock()
	require.NotNil(t, entry)
	require.Len(t, entry.root.Targets, 1)
	require.Equal(t, "meta/index.txt", entry.root.Targets[0].URL)
}

func TestRepositoryStatusesIncludePendingAndRefreshingRoots(t *testing.T) {
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
		refreshErr <- handler.RefreshRoot(ctx, "root")
	}()
	<-started

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, "root", statuses[0].ID)
	require.False(t, statuses[0].HasCurrent)
	require.True(t, statuses[0].Refreshing)

	stats := handler.stats.Snapshot()
	require.Equal(t, "bootstrapping", stats.Instances["repo"].MetadataState)

	close(unblock)
	require.NoError(t, <-refreshErr)

	statuses = handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.True(t, statuses[0].HasCurrent)
	require.False(t, statuses[0].Refreshing)
	require.Equal(t, "active", statuses[0].State)
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

	require.Error(t, handler.RefreshRoot(ctx, "root"))
	require.False(t, handler.hasAnyRootSnapshot())

	stats := handler.stats.Snapshot()
	require.Equal(t, "booting", stats.Instances["repo"].MetadataState)
}

func TestRestoreGenerationsMarksRecoveredRootActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil, nil)

	snapshot := &LiveSnapshot{
		RootID:     "root",
		RootPath:   "root",
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
		RootID     string `yaml:"root_id"`
		Generation string `yaml:"generation"`
	}{RootID: "root", Generation: "gen1"})
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.currentPath("root"), bytes.NewReader(refData), nil)
	require.NoError(t, err)

	handler.restoreGenerations(ctx)

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.True(t, statuses[0].HasCurrent)
	require.Equal(t, "active", statuses[0].State)

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

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
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

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
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
			"meta/index.txt": {Path: "meta/real.txt", Required: true},
			"meta/real.txt":  {Path: "meta/real.txt", StorePath: "/store/path", Required: true},
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

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	require.Equal(t, 1, builderCalls)
	first := handler.rootSnapshot("root")
	require.NotNil(t, first)
	obj, ok := first.Metadata["meta/index.txt"]
	require.True(t, ok)
	info, err := store.StatObject(ctx, "repo", obj.StorePath)
	require.NoError(t, err)
	require.Equal(t, `"meta-v2"`, info.Options["etag"])

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	require.Equal(t, 2, builderCalls, "should rebuild since ETag changed")
}
