package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	goruntime "runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

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

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string, builder SnapshotBuilder) *IndexedHandler {
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
				return DiscoveryResult{Class: ResourceSidecar}
			default:
				return DiscoveryResult{Class: ResourceArtifact}
			}
		}),
		upstreams,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		builder,
		store,
		stats,
		health.New("repo", "test", health.DefaultConfig(), upstreams, stats),
		nil,
	)
	handler.AddRepository(testRepositoryRoot("root", "meta/index.txt"))
	return handler
}

func noopSchedulerTask(context.Context) (*scheduler.TaskOutcome, error) {
	return nil, nil
}

func TestMaterializeVerifiedOwnsTemporaryFilesAndPersistsValidatedBytes(t *testing.T) {
	store := newTestStore(t)
	tmpDir := t.TempDir()
	t.Setenv("TMPDIR", tmpDir)
	handler := newTestHandler(t, store, nil, nil)
	payload := []byte("derived metadata")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	session := &RefreshSession{
		handler: handler, rootID: "root", generation: "generation", blobs: map[string]*MetadataBlob{},
	}

	blob, err := session.MaterializeVerified(
		context.Background(), "metadata/index", bytes.NewReader(payload), int64(len(payload)), digest,
	)
	require.NoError(t, err)
	require.Equal(t, digest, blob.Digest)
	stored, err := store.OpenObject(context.Background(), handler.name, handler.generationMetadataPath("root", "generation", "metadata/index"))
	require.NoError(t, err)
	data, err := io.ReadAll(stored)
	require.NoError(t, err)
	require.NoError(t, stored.Close())
	require.Equal(t, payload, data)
	session.Close()
	resumed := &RefreshSession{
		handler: handler, rootID: "root", generation: "generation", blobs: map[string]*MetadataBlob{},
	}
	resumedBlob, err := resumed.MaterializeVerified(
		context.Background(), "metadata/index", strings.NewReader("must not be read"), int64(len(payload)), digest,
	)
	require.NoError(t, err)
	require.Equal(t, digest, resumedBlob.Digest)
	resumed.Close()

	failing := &RefreshSession{
		handler: handler, rootID: "root", generation: "invalid", blobs: map[string]*MetadataBlob{},
	}
	_, err = failing.MaterializeVerified(
		context.Background(), "metadata/index", strings.NewReader("wrong"), int64(len(payload)), digest,
	)
	require.ErrorContains(t, err, "size mismatch")
	failing.Close()
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestConfirmAnchorsCleansResponseWhenAbsentAnchorAppears(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, "new anchor")
	}))
	defer server.Close()

	handler := newTestHandler(t, newTestStore(t), []string{server.URL}, nil)
	session := &RefreshSession{
		handler: handler, rootID: "root", upstream: server.URL, generation: "generation",
		blobs: map[string]*MetadataBlob{}, anchors: map[string]MetadataAnchor{}, expectedAnchors: map[string]MetadataAnchor{},
	}
	defer session.Close()
	object, err := session.FetchOptionalAnchor(context.Background(), "meta/index.sig")
	require.NoError(t, err)
	require.Equal(t, MetadataNotFound, object.State)

	_, _, err = session.ConfirmAnchors(context.Background())
	require.ErrorIs(t, err, errMetadataAnchorChanged)
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestPendingMetadataClosureLeavesNoCurrentOrStagingState(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"},
		func(context.Context, *RefreshSession, *PathIndexBuilder) (*LiveSnapshot, error) {
			return nil, ErrMetadataClosurePending
		},
	)
	handler.mode = config.ModeDEB

	outcome, err := handler.RefreshRootTask(context.Background(), "root")
	require.ErrorIs(t, err, scheduler.ErrTaskSkipped)
	require.Equal(t, "metadata_closure_pending", outcome.ReasonCode)
	require.Nil(t, handler.rootSnapshot("root"))
	_, current := handler.durableCurrentGeneration(context.Background(), "root")
	require.False(t, current)
	_, err = store.StatObject(context.Background(), handler.name, handler.stagingStatePath("root"))
	require.Error(t, err)
	requireGenerationDirCount(t, store, handler.name, handler.objectRoot, "root", 0)
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestRefreshDoesNotPublishSnapshotFromChangedRootClosure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, path.Base(r.URL.Path))
	}))
	defer server.Close()

	started := make(chan struct{})
	unblock := make(chan struct{})
	var builds atomic.Int32
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, _ *PathIndexBuilder) (*LiveSnapshot, error) {
			metadata := make(map[string]MetadataObject, len(session.Targets()))
			for _, target := range session.Targets() {
				blob, err := session.FetchAnchor(ctx, target)
				if err != nil {
					return nil, err
				}
				metadata[blob.Path] = MetadataObject{Path: blob.Path, Required: true}
			}
			if builds.Add(1) == 1 {
				close(started)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-unblock:
				}
			}
			return &LiveSnapshot{Metadata: metadata}, nil
		},
	)

	type refreshResult struct {
		outcome *scheduler.TaskOutcome
		err     error
	}
	result := make(chan refreshResult, 1)
	go func() {
		outcome, err := handler.RefreshRootTask(context.Background(), "root")
		result <- refreshResult{outcome: outcome, err: err}
	}()
	<-started
	expanded := testRepositoryRoot("root", "meta/index.txt")
	expanded.Targets = append(expanded.Targets, MetadataTarget{URL: "meta/extra.txt"})
	handler.AddRepository(expanded)
	close(unblock)

	first := <-result
	require.ErrorIs(t, first.err, scheduler.ErrTaskSkipped)
	require.Equal(t, "root_closure_changed", first.outcome.ReasonCode)
	require.Nil(t, handler.rootSnapshot("root"))
	_, current := handler.durableCurrentGeneration(context.Background(), "root")
	require.False(t, current)
	var currentTemps []string
	err := fs.WalkDir(store.TenantFS(handler.name), handler.objectRoot, func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr == nil && !entry.IsDir() && strings.HasPrefix(path.Base(objectPath), "current.yaml.tmp.") {
			currentTemps = append(currentTemps, objectPath)
		}
		return walkErr
	})
	require.NoError(t, err)
	require.Empty(t, currentTemps)

	outcome, err := handler.RefreshRootTask(context.Background(), "root")
	require.NoError(t, err)
	require.Equal(t, "published", outcome.ReasonCode)
	snapshot := handler.rootSnapshot("root")
	require.NotNil(t, snapshot)
	require.Contains(t, snapshot.Metadata, "meta/index.txt")
	require.Contains(t, snapshot.Metadata, "meta/extra.txt")
	_, current = handler.durableCurrentGeneration(context.Background(), "root")
	require.True(t, current)
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

func heapAllocAfterGC() uint64 {
	for i := 0; i < 3; i++ {
		goruntime.GC()
		debug.FreeOSMemory()
	}
	var stats goruntime.MemStats
	goruntime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func requireHeapGrowthWithin(t *testing.T, before, after, limit uint64) {
	t.Helper()
	if after <= before {
		return
	}
	require.LessOrEqual(t, after-before, limit)
}

func TestRefreshPersistsCleanupIndexAndCleanupUsesStoredPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var indexVersion atomic.Int32
	indexVersion.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = fmt.Fprintf(w, "index-%d", indexVersion.Load())
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			paths.Add("pool/pkg.deb.sig")
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: 1,
			}, nil
		},
	)

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	current := handler.rootSnapshot("root")
	require.NotNil(t, current)
	indexReader, err := store.OpenObject(ctx, "repo", handler.cleanupIndexPath("root", current.Generation))
	require.NoError(t, err)
	indexData, err := io.ReadAll(indexReader)
	require.NoError(t, err)
	require.NoError(t, indexReader.Close())
	require.Equal(t, strings.Join([]string{
		"pool/pkg.deb",
		"pool/pkg.deb.sig",
	}, "\n")+"\n", string(indexData))

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, 1, statuses[0].ArtifactCount)

	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.contentPath(ResourceArtifact, "pool/pkg.deb"))), 0o755))
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.contentPath(ResourceSidecar, "pool/pkg.deb.sig"))), 0o755))
	old := map[string]string{"fetched-at": time.Now().Add(-1000 * time.Hour).UTC().Format(time.RFC3339Nano)}
	_, err = store.Put(ctx, "repo", handler.contentPath(ResourceArtifact, "pool/pkg.deb"), strings.NewReader("keep"), old)
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.contentPath(ResourceArtifact, "pool/old.deb"), strings.NewReader("drop"), old)
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.contentPath(ResourceSidecar, "pool/pkg.deb.sig"), strings.NewReader("keep"), old)
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.contentPath(ResourceSidecar, "pool/old.deb.sig"), strings.NewReader("drop"), old)
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, config.DefaultCleanupConfig()))

	reader, err := store.OpenObject(ctx, "repo", handler.contentPath(ResourceArtifact, "pool/pkg.deb"))
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	_, err = store.OpenObject(ctx, "repo", handler.contentPath(ResourceArtifact, "pool/old.deb"))
	require.Error(t, err)
	reader, err = store.OpenObject(ctx, "repo", handler.contentPath(ResourceSidecar, "pool/pkg.deb.sig"))
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	_, err = store.OpenObject(ctx, "repo", handler.contentPath(ResourceSidecar, "pool/old.deb.sig"))
	require.Error(t, err)
}

func TestRefreshStressDoesNotRetainCleanupPathSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const (
		pathCount = 30000
		rounds    = 6
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "index")
	}))
	defer server.Close()
	store := newTestStore(t)
	builderCalls := 0
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			builderCalls++
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			if err != nil {
				return nil, err
			}
			for i := 0; i < pathCount; i++ {
				paths.Add(fmt.Sprintf("pool/round-%02d/pkg-%05d.deb", builderCalls, i))
			}
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: pathCount,
			}, nil
		},
	)

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	baseline := heapAllocAfterGC()
	for i := 1; i < rounds; i++ {
		require.NoError(t, handler.RefreshRoot(ctx, "root"))
	}
	after := heapAllocAfterGC()

	handler.mu.RLock()
	require.Len(t, handler.rootSnapshots, 1)
	require.Len(t, handler.currentView, 1)
	for cleanPath, current := range handler.currentView {
		require.Equal(t, ResourceMetadata, current.Class, cleanPath)
	}
	handler.mu.RUnlock()

	current := handler.rootSnapshot("root")
	require.NotNil(t, current)
	_, err := store.StatObject(ctx, "repo", handler.cleanupIndexPath("root", current.Generation))
	require.NoError(t, err)
	require.Equal(t, rounds, builderCalls)
	t.Logf("refresh stress heap_alloc baseline=%d after=%d delta=%d", baseline, after, int64(after)-int64(baseline))
	requireHeapGrowthWithin(t, baseline, after, 12<<20)
}

func TestCleanupDoesNotRetainLoadedCleanupIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const (
		pathCount = 40000
		rounds    = 8
	)

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)

	var index strings.Builder
	for i := 0; i < pathCount; i++ {
		_, _ = fmt.Fprintf(&index, "pool/pkg-%05d.deb\n", i)
	}
	indexPath := handler.cleanupIndexPath("root", "gen1")
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(indexPath)), 0o755))
	indexData := []byte(index.String())
	_, err := store.Put(ctx, "repo", indexPath, bytes.NewReader(indexData), nil)
	require.NoError(t, err)
	handler.setRootSnapshot("root", &LiveSnapshot{
		RootID: "root", Generation: "gen1", CleanupIndexDigest: digestBytes(indexData),
	})

	require.NoError(t, handler.Cleanup(ctx, config.DefaultCleanupConfig()))
	baseline := heapAllocAfterGC()
	for i := 1; i < rounds; i++ {
		require.NoError(t, handler.Cleanup(ctx, config.DefaultCleanupConfig()))
	}
	after := heapAllocAfterGC()

	handler.mu.RLock()
	require.Len(t, handler.rootSnapshots, 1)
	require.Empty(t, handler.currentView)
	handler.mu.RUnlock()
	t.Logf("cleanup stress heap_alloc baseline=%d after=%d delta=%d", baseline, after, int64(after)-int64(baseline))
	requireHeapGrowthWithin(t, baseline, after, 8<<20)
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
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
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

func TestServeHTTPRejectsMetadataMissingFromCurrentWithoutUpstreamFallback(t *testing.T) {
	ctx := context.Background()
	var upstreamRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests.Add(1)
		_, _ = io.WriteString(w, "upstream metadata")
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL}, nil)
	handler.mu.Lock()
	handler.roots["root"].root.Path = "meta"
	handler.mu.Unlock()
	handler.setRootSnapshot("root", &LiveSnapshot{
		Version: snapshotSchemaVersion, RootID: "root", RootPath: "meta", Generation: "gen1",
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	})

	req := httptest.NewRequest(http.MethodGet, "/meta/filelists.xml.gz", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Zero(t, upstreamRequests.Load())
}

func TestServeHTTPAllowsMetadataPassthroughForKnownRootWithoutCurrent(t *testing.T) {
	var upstreamRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests.Add(1)
		_, _ = io.WriteString(w, "bootstrap metadata")
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL}, nil)
	handler.mu.Lock()
	handler.roots["root"].root.Path = "meta"
	handler.mu.Unlock()
	req := httptest.NewRequest(http.MethodGet, "/meta/filelists.xml.gz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "bootstrap metadata", rec.Body.String())
	require.EqualValues(t, 1, upstreamRequests.Load())
}

func TestGenerationResolverKeepsStableContentPathAcrossRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var indexVersion atomic.Int32
	indexVersion.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = fmt.Fprintf(w, "index-%d", indexVersion.Load())
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: 1,
			}, nil
		},
	)
	handler.mu.Lock()
	handler.roots["root"].root.Path = ""
	handler.mu.Unlock()

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	firstRoute, err := (&generationResolver{handler: handler, policy: &Policy{}}).Resolve(
		httptest.NewRequest(http.MethodGet, "/pool/pkg.deb", nil),
	)
	require.NoError(t, err)
	require.Contains(t, firstRoute.ObjectPath, "/.content/artifacts/pool/pkg.deb")

	time.Sleep(time.Nanosecond)
	indexVersion.Store(2)
	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	secondRoute, err := (&generationResolver{handler: handler, policy: &Policy{}}).Resolve(
		httptest.NewRequest(http.MethodGet, "/pool/pkg.deb", nil),
	)
	require.NoError(t, err)
	require.Equal(t, firstRoute.ObjectPath, secondRoute.ObjectPath)
}

func TestValidatePassHeadersRejectsManagedUserAgent(t *testing.T) {
	require.NoError(t, ValidatePassHeaders([]string{"X-Custom"}))
	require.ErrorContains(t, ValidatePassHeaders([]string{"user-agent"}), "transport.ua")
}

func TestStartReconcilesMetadataTasksWithoutSchedulerState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	initial := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	initial.AddRepository(testRepositoryRoot("root", "meta/index.txt"))
	initial.saveState(ctx)

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	sched := scheduler.New(store, prometheus.NewRegistry())
	sched.RegisterFactory(scheduler.TaskFactory{
		Instance:        "repo",
		RefreshInterval: time.Hour,
		GCInterval:      6 * time.Hour,
		NewRefresh:      func(string) scheduler.TaskHandler { return noopSchedulerTask },
		NewGC:           func(string) scheduler.TaskHandler { return noopSchedulerTask },
		CurrentRoots:    restored.currentRootIDs,
	})

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
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
		health.New("repo", "test", health.DefaultConfig(), []string{"https://upstream.example"}, nil),
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
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
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

func TestExistingRootMetadataUpdateRequestsRefresh(t *testing.T) {
	store := newTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "extra")
	}))
	defer server.Close()

	handler := newTestHandler(t, store, []string{server.URL}, nil)
	refreshes := make(chan string, 1)
	handler.SetRefreshTrigger(func(rootID string) { refreshes <- rootID })
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
	handler.setRootSnapshot("root", &LiveSnapshot{RootID: "root", Generation: "gen1"})

	req := httptest.NewRequest(http.MethodGet, "/meta/extra.txt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Equal(t, "gen1", rec.Header().Get("X-Cache-Generation"))

	select {
	case rootID := <-refreshes:
		require.Equal(t, "root", rootID)
	case <-time.After(time.Second):
		require.FailNow(t, "expected refresh request for updated root")
	}
	handler.mu.RLock()
	require.Len(t, handler.roots["root"].root.Targets, 2)
	handler.mu.RUnlock()
}

func TestExistingRootMetadataCreateExpandsClosureBeforeRefresh(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, path.Base(r.URL.Path))
	}))
	defer server.Close()

	handler := newTestHandler(t, newTestStore(t), []string{server.URL}, func(ctx context.Context, session *RefreshSession, _ *PathIndexBuilder) (*LiveSnapshot, error) {
		metadata := make(map[string]MetadataObject, len(session.Targets()))
		for _, target := range session.Targets() {
			blob, err := session.FetchAnchor(ctx, target)
			if err != nil {
				return nil, err
			}
			metadata[blob.Path] = MetadataObject{Path: blob.Path, Required: true}
		}
		return &LiveSnapshot{Metadata: metadata}, nil
	})
	handler.inspector = staticInspector(func(cleanPath string) DiscoveryResult {
		root := testRepositoryRoot("root", "meta/index.txt")
		root.Targets = append(root.Targets, MetadataTarget{URL: cleanPath})
		return DiscoveryResult{Class: ResourceMetadata, Role: DiscoveryCreateRoot, Root: root}
	})
	handler.setRootSnapshot("root", &LiveSnapshot{
		Version: snapshotSchemaVersion, RootID: "root", RootPath: "meta", Generation: "gen1",
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/meta/extra.txt", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)

	handler.mu.RLock()
	require.Equal(t, []MetadataTarget{{URL: "meta/index.txt"}, {URL: "meta/extra.txt"}}, handler.roots["root"].root.Targets)
	handler.mu.RUnlock()
	require.NoError(t, handler.RefreshRoot(context.Background(), "root"))

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/meta/extra.txt", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "GENERATION", recorder.Header().Get("X-Cache"))
	require.Equal(t, "extra.txt", recorder.Body.String())
}

func TestUndiscoveredCompanionMetadataPassesThroughWithoutDiscovery(t *testing.T) {
	var upstreamRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamRequests.Add(1)
		switch req.URL.Path {
		case "/repo/index.sig":
			_, _ = io.WriteString(w, "signature")
		case "/repo/forbidden.sig":
			w.WriteHeader(http.StatusForbidden)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	store := newTestStore(t)
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		staticInspector(func(string) DiscoveryResult {
			return DiscoveryResult{Class: ResourceMetadata, Role: DiscoveryIgnore}
		}),
		[]string{server.URL}, nil, config.Expiration(time.Hour), &Policy{}, nil, store, stats,
		health.New("repo", "test", health.DefaultConfig(), []string{server.URL}, stats), nil,
	)
	refreshes := atomic.Int64{}
	handler.SetRefreshTrigger(func(string) { refreshes.Add(1) })

	for _, test := range []struct {
		requestPath string
		status      int
	}{
		{requestPath: "/repo/index.sig", status: http.StatusOK},
		{requestPath: "/repo/forbidden.sig", status: http.StatusForbidden},
		{requestPath: "/repo/missing.sig", status: http.StatusNotFound},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.requestPath, nil))
		require.Equal(t, test.status, recorder.Code)
	}
	require.Equal(t, int64(3), upstreamRequests.Load())
	require.Zero(t, refreshes.Load())
	require.Empty(t, handler.currentRootIDs())
}

func TestStalePrimaryMetadataServesCurrentAndRequestsRefresh(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "index")
	}))
	defer server.Close()
	handler := newTestHandler(t, newTestStore(t), []string{server.URL}, func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
		blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
		if err != nil {
			return nil, err
		}
		return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
	})
	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	current := handler.rootSnapshot("root")
	current.Published = time.Now().Add(-time.Hour)
	handler.setRootSnapshot("root", current)
	handler.SetMetadataFreshFor(config.Freshness(time.Minute))
	refreshes := make(chan string, 1)
	handler.SetRefreshTrigger(func(rootID string) { refreshes <- rootID })

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "GENERATION", rec.Header().Get("X-Cache"))
	require.Equal(t, "index", rec.Body.String())
	select {
	case rootID := <-refreshes:
		require.Equal(t, "root", rootID)
	case <-time.After(time.Second):
		require.FailNow(t, "expected refresh request for stale primary metadata")
	}
}

func TestMetadataUpdateRootDoesNotMergeAfterMissingPassthrough(t *testing.T) {
	store := newTestStore(t)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	handler := newTestHandler(t, store, []string{server.URL}, nil)
	handler.inspector = staticInspector(func(string) DiscoveryResult {
		return DiscoveryResult{
			Class: ResourceMetadata,
			Role:  DiscoveryUpdateRoot,
			Root: RepositoryRoot{
				ID:            "root",
				Path:          "root",
				Architectures: []string{"arm64"},
				Attributes:    []RepositoryAttribute{{LabelKey: "architecture", Value: "arm64"}},
			},
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/meta/arm64.txt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	handler.mu.RLock()
	root := handler.roots["root"].root
	handler.mu.RUnlock()
	require.Empty(t, root.Architectures)
	require.Equal(t, []RepositoryAttribute{{LabelKey: "repo_path", Value: "root"}}, root.Attributes)
}

func TestMetadataUpdateRootMergesAfterSuccessfulPassthrough(t *testing.T) {
	store := newTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "index")
	}))
	defer server.Close()

	handler := newTestHandler(t, store, []string{server.URL}, nil)
	handler.inspector = staticInspector(func(string) DiscoveryResult {
		return DiscoveryResult{
			Class: ResourceMetadata,
			Role:  DiscoveryUpdateRoot,
			Root: RepositoryRoot{
				ID:            "root",
				Path:          "root",
				Architectures: []string{"amd64"},
				Attributes:    []RepositoryAttribute{{LabelKey: "architecture", Value: "amd64"}},
			},
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/meta/amd64.txt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	handler.mu.RLock()
	root := handler.roots["root"].root
	handler.mu.RUnlock()
	require.Equal(t, []string{"amd64"}, root.Architectures)
	require.Equal(t, []RepositoryAttribute{{LabelKey: "architecture", Value: "amd64"}}, root.Attributes)
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
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
		health.New("repo", "test", health.DefaultConfig(), []string{"https://upstream.example"}, nil),
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
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	handler.AddRepository(testRepositoryRoot("root", "meta/index.txt"))
	handler.saveState(ctx)

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	require.NoError(t, restored.restoreRoots(ctx))

	statuses := restored.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, "root", statuses[0].ID)
	require.False(t, statuses[0].HasCurrent)

	restored.mu.RLock()
	entry := restored.roots["root"]
	restored.mu.RUnlock()
	require.NotNil(t, entry)
	require.Len(t, entry.root.Targets, 1)
	require.Equal(t, "meta/index.txt", entry.root.Targets[0].URL)
}

func TestStateWriterFlushesConcurrentRootUpdates(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	require.NoError(t, handler.Start(ctx))

	var updates sync.WaitGroup
	for i := range 32 {
		updates.Add(1)
		go func(index int) {
			defer updates.Done()
			handler.registerRoot(DiscoveryResult{
				Class: ResourceMetadata,
				Role:  DiscoveryUpdateRoot,
				Root: RepositoryRoot{
					ID: "root", Path: "root",
					Targets: []MetadataTarget{{URL: fmt.Sprintf("meta/index-%02d", index)}},
				},
			})
			handler.touchRoot("root")
		}(i)
	}
	updates.Wait()
	require.NoError(t, handler.Stop(ctx))

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	state, ok, err := restored.loadState(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotZero(t, state.Revision)
	require.Len(t, state.Roots, 1)
	require.Len(t, state.Roots[0].Root.Targets, 33)
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
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			return &LiveSnapshot{
				Metadata:      map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
				ArtifactCount: 1,
			}, nil
		},
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
	)

	require.Error(t, handler.RefreshRoot(ctx, "root"))
	require.False(t, handler.hasAnyRootSnapshot())

	stats := handler.stats.Snapshot()
	require.Equal(t, "booting", stats.Instances["repo"].MetadataState)
}

func TestRefreshFailureRemovesUnpublishedGeneration(t *testing.T) {
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
			_, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return nil, errors.New("parse index: invalid metadata")
		},
	)

	require.Error(t, handler.RefreshRoot(ctx, "root"))
	requireGenerationDirCount(t, store, "repo", handler.objectRoot, "root", 0)
}

func TestRefreshFailureKeepsCurrentGeneration(t *testing.T) {
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
	refreshes := 0
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			refreshes++
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			if refreshes == 2 {
				return nil, errors.New("parse index: invalid metadata")
			}
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
	)

	require.NoError(t, handler.RefreshRoot(ctx, "root"))
	current := handler.rootSnapshot("root")
	require.NotNil(t, current)
	require.Error(t, handler.RefreshRoot(ctx, "root"))
	require.Equal(t, current.Generation, handler.rootSnapshot("root").Generation)

	reader, err := store.OpenObject(ctx, "repo", current.Metadata["meta/index.txt"].StorePath)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	requireGenerationDirCount(t, store, "repo", handler.objectRoot, "root", 1)
}

func TestRestoreGenerationsMarksRecoveredRootActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)

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

	writeCurrentSnapshot(t, ctx, store, handler, snapshot)

	handler.restoreGenerations(ctx)

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.True(t, statuses[0].HasCurrent)
	require.Equal(t, "active", statuses[0].State)

	stats := handler.stats.Snapshot()
	require.Equal(t, "ready", stats.Instances["repo"].MetadataState)

}

func TestRestoreGenerationDoesNotFallbackFromInvalidCurrentReference(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	original := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	snapshot := &LiveSnapshot{
		RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().UTC(),
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	}
	writeCurrentSnapshot(t, ctx, store, original, snapshot)
	_, err := store.Put(ctx, original.name, original.currentPath("root"), strings.NewReader("invalid: ["), nil)
	require.NoError(t, err)

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	restored.restoreGenerations(ctx)
	require.False(t, restored.hasAnyRootSnapshot())
	_, err = store.StatObject(ctx, restored.name, restored.snapshotPath("root", "gen1"))
	require.NoError(t, err, "uncommitted snapshot may remain for GC but must not become current")
}

func TestRestoreGenerationRejectsMissingCleanupIndex(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	original := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	snapshot := &LiveSnapshot{
		RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().UTC(),
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	}
	writeCurrentSnapshot(t, ctx, store, original, snapshot)
	require.NoError(t, store.DeleteObject(ctx, original.name, original.cleanupIndexPath("root", "gen1")))

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	restored.restoreGenerations(ctx)
	require.False(t, restored.hasAnyRootSnapshot())
}

func TestRestoreGenerationRejectsPersistedDigestMismatch(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	original := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	snapshot := &LiveSnapshot{
		RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().UTC(),
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	}
	writeCurrentSnapshot(t, ctx, store, original, snapshot)
	objectPath := original.generationMetadataPath("root", "gen1", "meta/index.txt")
	_, err := store.Put(ctx, original.name, objectPath, strings.NewReader("corrupt"), nil)
	require.NoError(t, err)

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	restored.restoreGenerations(ctx)
	require.False(t, restored.hasAnyRootSnapshot())
}

func TestRestoreGenerationRejectsOldSnapshotSchemaReferencedByCurrent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	original := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	snapshot := &LiveSnapshot{
		RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().UTC(),
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	}
	writeCurrentSnapshot(t, ctx, store, original, snapshot)
	snapshot.Version = snapshotSchemaVersion - 1
	data, err := yaml.Marshal(snapshot)
	require.NoError(t, err)
	_, err = store.Put(ctx, original.name, original.snapshotPath("root", "gen1"), bytes.NewReader(data), nil)
	require.NoError(t, err)

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	restored.restoreGenerations(ctx)
	require.False(t, restored.hasAnyRootSnapshot())
}

func TestRestoreGenerationRejectsMissingPersistedObject(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	original := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	snapshot := &LiveSnapshot{
		RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().UTC(),
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	}
	writeCurrentSnapshot(t, ctx, store, original, snapshot)
	require.NoError(t, store.DeleteObject(ctx, original.name, original.generationMetadataPath("root", "gen1", "meta/index.txt")))

	restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	restored.restoreGenerations(ctx)
	require.False(t, restored.hasAnyRootSnapshot())
}

func TestRestoreGenerationRequiresCompletedCurrentCommit(t *testing.T) {
	tests := []struct {
		name        string
		breakCommit func(t *testing.T, ctx context.Context, store *blobfs.Store, handler *IndexedHandler)
	}{
		{
			name: "cleanup index only",
			breakCommit: func(t *testing.T, ctx context.Context, store *blobfs.Store, handler *IndexedHandler) {
				require.NoError(t, store.DeleteObject(ctx, handler.name, handler.snapshotPath("root", "gen1")))
				require.NoError(t, store.DeleteObject(ctx, handler.name, handler.currentPath("root")))
			},
		},
		{
			name: "snapshot without current",
			breakCommit: func(t *testing.T, ctx context.Context, store *blobfs.Store, handler *IndexedHandler) {
				require.NoError(t, store.DeleteObject(ctx, handler.name, handler.currentPath("root")))
			},
		},
		{
			name: "temporary current before rename",
			breakCommit: func(t *testing.T, _ context.Context, store *blobfs.Store, handler *IndexedHandler) {
				current := handler.currentPath("root")
				temporary := current + ".tmp.gen1"
				require.NoError(t, store.Rename(path.Join(handler.name, current), path.Join(handler.name, temporary)))
			},
		},
		{
			name: "current references missing snapshot",
			breakCommit: func(t *testing.T, ctx context.Context, store *blobfs.Store, handler *IndexedHandler) {
				require.NoError(t, store.DeleteObject(ctx, handler.name, handler.snapshotPath("root", "gen1")))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			original := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
			snapshot := &LiveSnapshot{
				RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().UTC(),
				Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
			}
			writeCurrentSnapshot(t, ctx, store, original, snapshot)
			tt.breakCommit(t, ctx, store, original)

			restored := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
			restored.cleanCurrentRefTemps(ctx)
			restored.restoreGenerations(ctx)
			require.False(t, restored.hasAnyRootSnapshot())
		})
	}
}

func writeCurrentSnapshot(
	t *testing.T,
	ctx context.Context,
	store *blobfs.Store,
	handler *IndexedHandler,
	snapshot *LiveSnapshot,
) {
	t.Helper()
	snapshot.Version = snapshotSchemaVersion
	if snapshot.Upstream == "" {
		snapshot.Upstream = "https://upstream.example"
	}
	if snapshot.Published.IsZero() {
		snapshot.Published = time.Now().UTC()
	}
	for cleanPath, object := range snapshot.Metadata {
		if object.Path == "" {
			object.Path = cleanPath
		}
		object.StorePath = handler.generationMetadataPath(snapshot.RootID, snapshot.Generation, object.Path)
		require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(object.StorePath)), 0o755))
		_, err := store.Put(ctx, "repo", object.StorePath, strings.NewReader("metadata:"+object.Path), nil)
		require.NoError(t, err)
		object.Digest, err = handler.metadataObjectDigest(ctx, object.StorePath)
		require.NoError(t, err)
		info, err := store.StatObject(ctx, "repo", object.StorePath)
		require.NoError(t, err)
		object.State = MetadataPresent
		object.Size = info.Size
		snapshot.Metadata[cleanPath] = object
	}
	anchorPath := "meta/index.txt"
	if len(snapshot.Targets) > 0 {
		anchorPath = snapshot.Targets[0].URL
	}
	anchorObject, ok := snapshot.Metadata[anchorPath]
	if !ok {
		for _, object := range snapshot.Metadata {
			anchorPath = object.Path
			anchorObject = object
			break
		}
	}
	snapshot.Anchors = []MetadataAnchor{{Path: anchorPath, State: MetadataPresent, Size: anchorObject.Size, Digest: anchorObject.Digest}}
	anchorDigest, err := metadataAnchorsDigest(snapshot.Anchors)
	require.NoError(t, err)
	snapshot.AnchorSetDigest = anchorDigest
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.snapshotPath(snapshot.RootID, snapshot.Generation))), 0o755))
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.currentPath(snapshot.RootID))), 0o755))
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(handler.cleanupIndexPath(snapshot.RootID, snapshot.Generation))), 0o755))
	cleanupData := []byte{}
	_, err = store.Put(ctx, "repo", handler.cleanupIndexPath(snapshot.RootID, snapshot.Generation), bytes.NewReader(cleanupData), nil)
	require.NoError(t, err)
	snapshot.CleanupIndexDigest = digestBytes(cleanupData)

	data, err := yaml.Marshal(snapshot)
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.snapshotPath(snapshot.RootID, snapshot.Generation), bytes.NewReader(data), nil)
	require.NoError(t, err)

	refData, err := yaml.Marshal(currentReference{
		Version: 2, RootID: snapshot.RootID, Generation: snapshot.Generation,
		SnapshotDigest: digestBytes(data), CleanupIndexDigest: snapshot.CleanupIndexDigest,
	})
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", handler.currentPath(snapshot.RootID), bytes.NewReader(refData), nil)
	require.NoError(t, err)
}

func rewriteCommittedCleanupIndex(
	t *testing.T,
	ctx context.Context,
	store *blobfs.Store,
	handler *IndexedHandler,
	snapshot *LiveSnapshot,
	cleanupData []byte,
) {
	t.Helper()
	_, err := store.Put(ctx, handler.name, handler.cleanupIndexPath(snapshot.RootID, snapshot.Generation), bytes.NewReader(cleanupData), nil)
	require.NoError(t, err)
	snapshot.CleanupIndexDigest = digestBytes(cleanupData)
	snapshotData, err := yaml.Marshal(snapshot)
	require.NoError(t, err)
	_, err = store.Put(ctx, handler.name, handler.snapshotPath(snapshot.RootID, snapshot.Generation), bytes.NewReader(snapshotData), nil)
	require.NoError(t, err)
	refData, err := yaml.Marshal(currentReference{
		Version: 2, RootID: snapshot.RootID, Generation: snapshot.Generation,
		SnapshotDigest: digestBytes(snapshotData), CleanupIndexDigest: snapshot.CleanupIndexDigest,
	})
	require.NoError(t, err)
	_, err = store.Put(ctx, handler.name, handler.currentPath(snapshot.RootID), bytes.NewReader(refData), nil)
	require.NoError(t, err)
	handler.setRootSnapshot(snapshot.RootID, snapshot)
}

func writeGenerationSnapshot(
	t *testing.T,
	ctx context.Context,
	store *blobfs.Store,
	handler *IndexedHandler,
	rootID, generation string,
	published time.Time,
) string {
	t.Helper()
	objectPath := handler.generationMetadataPath(rootID, generation, "meta/index.txt")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(objectPath)), 0o755))
	_, err := store.Put(ctx, handler.name, objectPath, strings.NewReader(generation), nil)
	require.NoError(t, err)
	digest, err := handler.metadataObjectDigest(ctx, objectPath)
	require.NoError(t, err)
	cleanupData := []byte{}
	cleanupPath := handler.cleanupIndexPath(rootID, generation)
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(cleanupPath)), 0o755))
	_, err = store.Put(ctx, handler.name, cleanupPath, bytes.NewReader(cleanupData), nil)
	require.NoError(t, err)
	anchors := []MetadataAnchor{{Path: "meta/index.txt", State: MetadataPresent, Size: int64(len(generation)), Digest: digest}}
	anchorDigest, err := metadataAnchorsDigest(anchors)
	require.NoError(t, err)
	snapshot := LiveSnapshot{
		Version: snapshotSchemaVersion, RootID: rootID, RootPath: "", Generation: generation,
		Upstream: "https://upstream.example", Published: published.UTC(), Anchors: anchors,
		AnchorSetDigest: anchorDigest, CleanupIndexDigest: digestBytes(cleanupData),
		Targets: []MetadataTarget{{URL: "meta/index.txt"}},
		Metadata: map[string]MetadataObject{
			"meta/index.txt": {
				Path: "meta/index.txt", State: MetadataPresent, Required: true,
				Size: int64(len(generation)), Digest: digest,
			},
		},
	}
	data, err := yaml.Marshal(snapshot)
	require.NoError(t, err)
	snapshotPath := handler.snapshotPath(rootID, generation)
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(snapshotPath)), 0o755))
	_, err = store.Put(ctx, handler.name, snapshotPath, bytes.NewReader(data), nil)
	require.NoError(t, err)
	return objectPath
}

func requireGenerationDirCount(t *testing.T, store *blobfs.Store, tenant, objectRoot, rootID string, expected int) {
	t.Helper()
	generationsRoot := path.Join(objectRoot, ".roots", pathEscapeKey(rootID), "generations")
	count := 0
	err := fs.WalkDir(
		store.TenantFS(tenant),
		generationsRoot,
		func(objectPath string, entry fs.DirEntry, err error) error {
			if err != nil || !entry.IsDir() || objectPath == generationsRoot {
				return nil
			}
			rel := strings.TrimPrefix(objectPath, generationsRoot+"/")
			if strings.Contains(rel, "/") {
				return fs.SkipDir
			}
			count++
			return nil
		},
	)
	if err != nil && !errors.Is(err, fs.ErrNotExist) && !strings.Contains(err.Error(), "not exist") {
		require.NoError(t, err)
	}
	require.Equal(t, expected, count)
}

func TestPathIndexBuilderFinalizesSortedUniquePaths(t *testing.T) {
	builder := &PathIndexBuilder{}
	t.Cleanup(func() { require.NoError(t, builder.Close()) })
	builder.Add("pool/b.deb")
	builder.Add("pool/a.deb")
	builder.Add("pool/b.deb")
	builder.Add("pool/a.deb.sig")

	require.Equal(t, []string{
		"pool/a.deb",
		"pool/a.deb.sig",
		"pool/b.deb",
	}, builder.Finalize())
}

func TestRefreshKeepsCurrentWhenRebuiltMetadataIsUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"meta-v1"`)
		_, _ = io.WriteString(w, "index")
	}))
	defer server.Close()

	store := newTestStore(t)
	builderCalls := 0
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			builderCalls++
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
	)

	outcome, err := handler.RefreshRootTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, "updated", outcome.Result)
	require.Equal(t, "published", outcome.ReasonCode)
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

	outcome, err = handler.RefreshRootTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, "unchanged", outcome.Result)
	require.Equal(t, "same_as_current", outcome.ReasonCode)
	require.Equal(t, 2, builderCalls)
	stats := handler.stats.Snapshot()
	require.Equal(t, "ready", stats.Instances["repo"].MetadataState)
	require.Equal(t, uint64(4), stats.Instances["repo"].UpstreamRequests)
	require.Equal(t, "200", stats.Instances["repo"].Upstreams[server.URL].LastStatus)
	require.Zero(t, stats.Instances["repo"].Upstreams[server.URL].ErrorRate)
}

func TestSnapshotsMetadataEqualAcceptsMatchingNegativeCompanions(t *testing.T) {
	present := MetadataObject{
		Path: "meta/index", State: MetadataPresent, Required: true,
		Digest: "sha256:" + strings.Repeat("a", 64), Size: 10,
	}
	current := &LiveSnapshot{Metadata: map[string]MetadataObject{
		"meta/index":     present,
		"meta/index.sig": {Path: "meta/index.sig", State: MetadataNotFound},
	}}
	next := &LiveSnapshot{Metadata: map[string]MetadataObject{
		"meta/index":     present,
		"meta/index.sig": {Path: "meta/index.sig", State: MetadataNotFound},
	}}
	require.True(t, snapshotsMetadataEqual(current, next))
	next.Metadata["meta/index.sig"] = MetadataObject{Path: "meta/index.sig", State: MetadataForbidden}
	require.False(t, snapshotsMetadataEqual(current, next))
}

func TestRefreshRateLimitSchedulesRetryAndSuppressesNextRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var requests atomic.Int64
	var fallbackRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "60")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackRequests.Add(1)
		_, _ = io.WriteString(w, "fallback")
	}))
	defer fallback.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL, fallback.URL},
		func(ctx context.Context, session *RefreshSession, _ *PathIndexBuilder) (*LiveSnapshot, error) {
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			if err != nil {
				return nil, err
			}
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
	)
	handler.upstreamGate = httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{
		MaxActive: 8, MaxActivePerHost: 4,
	})

	_, err := handler.RefreshRootTask(ctx, "root")
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)
	require.True(t, retry.At.After(time.Now().Add(55*time.Second)))
	require.EqualValues(t, 1, requests.Load())
	require.Zero(t, fallbackRequests.Load())

	_, err = handler.RefreshRootTask(ctx, "root")
	require.ErrorAs(t, err, &retry)
	require.EqualValues(t, 1, requests.Load())
	require.Zero(t, fallbackRequests.Load())
}

func TestRefreshTransfersOnlyGatewayFailures(t *testing.T) {
	for _, test := range []struct {
		name            string
		status          int
		expectSuccess   bool
		expectFallbacks int64
	}{
		{name: "internal server error stops", status: http.StatusInternalServerError},
		{name: "service unavailable transfers", status: http.StatusServiceUnavailable, expectSuccess: true, expectFallbacks: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, http.StatusText(test.status), test.status)
			}))
			defer first.Close()
			var fallbackRequests atomic.Int64
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackRequests.Add(1)
				_, _ = io.WriteString(w, "metadata")
			}))
			defer fallback.Close()

			handler := newTestHandler(t, newTestStore(t), []string{first.URL, fallback.URL},
				func(ctx context.Context, session *RefreshSession, _ *PathIndexBuilder) (*LiveSnapshot, error) {
					blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
					if err != nil {
						return nil, err
					}
					return &LiveSnapshot{Metadata: map[string]MetadataObject{
						blob.Path: {Path: blob.Path, Required: true},
					}}, nil
				},
			)
			outcome, err := handler.RefreshRootTask(context.Background(), "root")
			if test.expectSuccess {
				require.NoError(t, err)
				require.Equal(t, "updated", outcome.Result)
			} else {
				require.Error(t, err)
				require.Nil(t, outcome)
			}
			require.Equal(t, test.expectFallbacks, fallbackRequests.Load())
		})
	}
}

func TestRefreshCompletesThroughAdmissionQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, "metadata")
	}))
	defer server.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, _ *PathIndexBuilder) (*LiveSnapshot, error) {
			metadata := make(map[string]MetadataObject, 9)
			for i := range 9 {
				blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: fmt.Sprintf("meta/index-%d", i)})
				if err != nil {
					return nil, err
				}
				metadata[blob.Path] = MetadataObject{Path: blob.Path, Required: true}
			}
			return &LiveSnapshot{Metadata: metadata}, nil
		},
	)
	handler.upstreamGate = httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{
		MaxActive: 2, MaxActivePerHost: 1,
	})

	outcome, err := handler.RefreshRootTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, "updated", outcome.Result)
	require.EqualValues(t, 18, requests.Load())
}

func TestCleanCurrentRefTempsRemovesStalePublishRefs(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	tmpPath := path.Join("repo", ".roots", pathEscapeKey("root"), "current.yaml.tmp.old")
	require.NoError(t, store.MkdirAll(path.Join("repo", path.Dir(tmpPath)), 0o755))
	_, err := store.Put(ctx, "repo", tmpPath, strings.NewReader("old"), nil)
	require.NoError(t, err)

	handler.cleanCurrentRefTemps(ctx)

	_, err = store.StatObject(ctx, "repo", tmpPath)
	require.Error(t, err)
}

func TestRefreshRebuildsWhenCleanupIndexMissing(t *testing.T) {
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
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			paths.Add("pool/pkg.deb")
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
	)

	outcome, err := handler.RefreshRootTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, "updated", outcome.Result)
	first := handler.rootSnapshot("root")
	require.NotNil(t, first)
	require.NoError(t, store.DeleteObject(ctx, "repo", handler.cleanupIndexPath("root", first.Generation)))

	outcome, err = handler.RefreshRootTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, "updated", outcome.Result)
	require.Equal(t, 2, builderCalls)
	require.Equal(t, 0, headRequests)
	current := handler.rootSnapshot("root")
	require.NotNil(t, current)
	_, err = store.StatObject(ctx, "repo", handler.cleanupIndexPath("root", current.Generation))
	require.NoError(t, err)
}

func TestCleanupPreservesPersistedRepositoryRoots(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	handler.saveState(ctx)
	_, err := store.Put(ctx, handler.name, path.Join(handler.objectRoot, "expired.cache"), strings.NewReader("expired"), nil)
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, config.DefaultCleanupConfig()))
	state, _, err := handler.loadState(ctx)
	require.NoError(t, err)
	require.Len(t, state.Roots, 1)
	handler.mu.Lock()
	handler.roots = map[string]*rootEntry{}
	handler.mu.Unlock()
	require.NoError(t, handler.restoreRoots(ctx))
	require.Equal(t, []string{"root"}, handler.currentRootIDs(), "restored roots must remain available to scheduler reconciliation")
	_, err = store.StatObject(ctx, handler.name, path.Join(handler.objectRoot, "expired.cache"))
	require.NoError(t, err, "unknown files are not positively classified cleanup objects")
}

func TestCleanupTaskSchedulesShortContinuationWhenBatchIsTruncated(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	for i := range 3 {
		objectPath := path.Join(handler.objectRoot, ".content", "artifacts", fmt.Sprintf("pool/pkg-%d.deb", i))
		require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(objectPath)), 0o755))
		_, err := store.Put(ctx, handler.name, objectPath, strings.NewReader("package"), map[string]string{
			"fetched-at": time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339),
		})
		require.NoError(t, err)
	}
	opts := config.DefaultCleanupConfig()
	opts.BatchSize = 1
	outcome, err := handler.CleanupTask(ctx, opts)
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, "partial", outcome.Result)
	require.Equal(t, "content_cleanup", outcome.ReasonCode)
	require.WithinDuration(t, time.Now().Add(maintenanceContinuationDelay), retry.At, time.Second)
	require.Equal(t, uint64(1), handler.stats.Snapshot().Instances[handler.name].Maintenance["content_cleanup.truncated"])
}

func TestMetadataGCTaskSchedulesShortContinuationWhenBatchIsTruncated(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	for i := range 2 {
		objectPath := handler.generationMetadataPath("root", "old", fmt.Sprintf("meta/index-%d", i))
		require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(objectPath)), 0o755))
		_, err := store.Put(ctx, handler.name, objectPath, strings.NewReader("metadata"), nil)
		require.NoError(t, err)
	}
	opts := config.DefaultCleanupConfig()
	opts.BatchSize = 1
	outcome, err := handler.CleanupRootTask(ctx, "root", opts)
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, "partial", outcome.Result)
	require.Equal(t, "metadata_gc", outcome.ReasonCode)
	require.WithinDuration(t, time.Now().Add(maintenanceContinuationDelay), retry.At, time.Second)
	require.Equal(t, uint64(1), handler.stats.Snapshot().Instances[handler.name].Maintenance["metadata_gc.truncated"])
}

func TestMetadataGCPreservesValidRefreshStagingGeneration(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	staging := refreshStagingState{
		RootID: "root", Generation: "staging-gen", Upstream: "https://upstream.example", CreatedAt: time.Now().UTC(),
		AnchorSetDigest: "sha256:" + strings.Repeat("a", 64),
		Anchors:         []MetadataAnchor{{Path: "meta/index.txt", State: MetadataPresent, Digest: strings.Repeat("a", 64)}},
	}
	require.NoError(t, handler.saveRefreshStaging(ctx, staging))
	objectPath := handler.generationMetadataPath("root", staging.Generation, "meta/partial")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(objectPath)), 0o755))
	_, err := store.Put(ctx, handler.name, objectPath, strings.NewReader("partial"), nil)
	require.NoError(t, err)

	require.NoError(t, handler.CleanupRoot(ctx, "root", config.DefaultCleanupConfig()))
	_, err = store.StatObject(ctx, handler.name, objectPath)
	require.NoError(t, err, "valid resumable staging must be pinned across metadata GC turns")
}

func TestRefreshRejectsCorruptPersistedCleanupFragment(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "metadata")
	}))
	defer server.Close()

	store := newTestStore(t)
	var builds atomic.Int32
	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
		blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
		if err != nil {
			return nil, err
		}
		paths.Add("pool/pkg.deb")
		if builds.Add(1) == 1 {
			return nil, errMetadataRefreshContinuation
		}
		return &LiveSnapshot{Metadata: map[string]MetadataObject{
			blob.Path: {Path: blob.Path, Required: true},
		}}, nil
	})
	handler.mode = config.ModeDEB

	_, err := handler.RefreshRootTask(ctx, "root")
	var retry scheduler.RetryAtError
	require.ErrorAs(t, err, &retry)
	staging, ok, err := handler.loadRefreshStaging(ctx, "root")
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, staging.CleanupFragments, 1)
	fragmentPath := handler.cleanupFragmentPath("root", staging.Generation, staging.CleanupFragments[0].Name)
	_, err = store.Put(ctx, handler.name, fragmentPath, strings.NewReader("corrupt\n"), nil)
	require.NoError(t, err)

	_, err = handler.RefreshRootTask(ctx, "root")
	require.ErrorContains(t, err, "cleanup fragment digest mismatch")
	require.Nil(t, handler.rootSnapshot("root"))
	_, ok, err = handler.loadRefreshStaging(ctx, "root")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestInvalidStagingWithoutRootIdentityIsDiscarded(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	stagingPath := handler.stagingStatePath("root")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(stagingPath)), 0o755))
	_, err := store.Put(ctx, handler.name, stagingPath, strings.NewReader("invalid: ["), nil)
	require.NoError(t, err)

	_, ok, err := handler.loadRefreshStaging(ctx, "root")
	require.NoError(t, err)
	require.False(t, ok)
	_, err = store.StatObject(ctx, handler.name, stagingPath)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestCleanupFailsClosedWhenCurrentCleanupIndexIsMissing(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	handler.setRootSnapshot("root", &LiveSnapshot{RootID: "root", Generation: "current"})
	objectPath := path.Join(handler.objectRoot, ".content", "artifacts", "pool/pkg.deb")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(objectPath)), 0o755))
	_, err := store.Put(ctx, handler.name, objectPath, strings.NewReader("package"), map[string]string{
		"fetched-at": time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339),
	})
	require.NoError(t, err)

	outcome, err := handler.CleanupTask(ctx, config.DefaultCleanupConfig())
	require.ErrorContains(t, err, "load cleanup index")
	require.Nil(t, outcome)
	_, err = store.StatObject(ctx, handler.name, objectPath)
	require.NoError(t, err, "missing protection data must prevent deletion")
	require.Equal(t, uint64(1), handler.stats.Snapshot().Instances[handler.name].Maintenance["content_cleanup.index_error"])
}

func TestInactiveRootRetirementRemovesGenerationIndexesAndState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	snapshot := &LiveSnapshot{
		RootID: "root", RootPath: "root", Generation: "gen1", Published: time.Now().Add(-2 * time.Hour).UTC(),
		Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt", Required: true}},
	}
	writeCurrentSnapshot(t, ctx, store, handler, snapshot)
	handler.setRootSnapshot("root", snapshot)
	rewriteCommittedCleanupIndex(t, ctx, store, handler, snapshot, []byte("pool/pkg.deb\n"))
	artifactPath := path.Join(handler.objectRoot, ".content", "artifacts", "pool/pkg.deb")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(artifactPath)), 0o755))
	_, err := store.Put(ctx, handler.name, artifactPath, strings.NewReader("package"), map[string]string{
		"fetched-at": time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339),
	})
	require.NoError(t, err)
	handler.SetRootExpireAfter(config.Expiration(time.Hour))
	handler.mu.Lock()
	handler.roots["root"].lastSeenAt = time.Now().Add(-2 * time.Hour)
	handler.mu.Unlock()

	require.NoError(t, handler.beginRootRetirement(ctx, "root"))
	_, err = handler.CleanupTask(ctx, config.DefaultCleanupConfig())
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, artifactPath)
	require.NoError(t, err, "retirement cleanup index must protect content until generation GC completes")

	outcome, err := handler.CleanupRootTask(ctx, "root", config.DefaultCleanupConfig())
	require.NoError(t, err)
	require.Equal(t, "retired", outcome.Result)
	require.Equal(t, "root_inactive", outcome.ReasonCode)
	require.Empty(t, handler.currentRootIDs())
	state, _, err := handler.loadState(ctx)
	require.NoError(t, err)
	require.Empty(t, state.Roots)
	_, err = store.StatObject(ctx, handler.name, handler.cleanupIndexPath("root", "gen1"))
	require.Error(t, err)
	require.Equal(t, uint64(1), handler.stats.Snapshot().Instances[handler.name].Maintenance["root_retirement.complete"])
	_, err = handler.CleanupTask(ctx, config.DefaultCleanupConfig())
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, artifactPath)
	require.Error(t, err, "content may be reclaimed only after root retirement completes")
}

func TestCleanupRootRetainsNewestPreviousAndRemovesOlderGeneration(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	handler.setRootSnapshot("root", &LiveSnapshot{RootID: "root", Generation: "current"})

	currentIndex := handler.cleanupIndexPath("root", "current")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(currentIndex)), 0o755))
	_, err := store.Put(ctx, handler.name, currentIndex, strings.NewReader("pool/current.pkg\n"), nil)
	require.NoError(t, err)
	previousObject := writeGenerationSnapshot(t, ctx, store, handler, "root", "previous", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	previousSnapshot := handler.snapshotPath("root", "previous")

	_ = writeGenerationSnapshot(t, ctx, store, handler, "root", "older", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	olderSnapshot := handler.snapshotPath("root", "older")

	opts := config.DefaultCleanupConfig()
	opts.BatchSize = 1
	require.NoError(t, handler.CleanupRoot(ctx, "root", opts))
	_, err = store.StatObject(ctx, handler.name, olderSnapshot)
	require.NoError(t, err, "snapshot must remain until a later batch confirms the generation is empty")

	require.NoError(t, handler.CleanupRoot(ctx, "root", opts))
	_, err = store.StatObject(ctx, handler.name, olderSnapshot)
	if err == nil {
		require.NoError(t, handler.CleanupRoot(ctx, "root", opts))
		_, err = store.StatObject(ctx, handler.name, olderSnapshot)
	}
	require.Error(t, err)
	_, err = store.StatObject(ctx, handler.name, previousObject)
	require.NoError(t, err)
	_, err = store.StatObject(ctx, handler.name, previousSnapshot)
	require.NoError(t, err)
}

func TestCleanupRootRetainsGenerationWithActiveReader(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)

	oldObject := handler.generationMetadataPath("root", "old", "meta/index.txt")
	require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(oldObject)), 0o755))
	_, err := store.Put(ctx, handler.name, oldObject, strings.NewReader("old metadata"), nil)
	require.NoError(t, err)
	for generation, published := range map[string]time.Time{
		"old":      time.Now().Add(-2 * time.Hour),
		"previous": time.Now().Add(-time.Hour),
	} {
		snapshot := &LiveSnapshot{
			Version: snapshotSchemaVersion, RootID: "root", Generation: generation, Published: published.UTC(),
		}
		data, marshalErr := yaml.Marshal(snapshot)
		require.NoError(t, marshalErr)
		snapshotPath := handler.snapshotPath("root", generation)
		require.NoError(t, store.MkdirAll(path.Join(handler.name, path.Dir(snapshotPath)), 0o755))
		_, err = store.Put(ctx, handler.name, snapshotPath, bytes.NewReader(data), nil)
		require.NoError(t, err)
	}

	handler.setRootSnapshot("root", &LiveSnapshot{
		Version: snapshotSchemaVersion, RootID: "root", Generation: "old",
		Metadata: map[string]MetadataObject{
			"meta/index.txt": {Path: "meta/index.txt", Required: true, StorePath: oldObject},
		},
	})
	_, release, ok := handler.lookupCurrentRequest("meta/index.txt")
	require.True(t, ok)
	handler.setRootSnapshot("root", &LiveSnapshot{Version: snapshotSchemaVersion, RootID: "root", Generation: "current"})

	require.NoError(t, handler.CleanupRoot(ctx, "root", config.DefaultCleanupConfig()))
	_, err = store.StatObject(ctx, handler.name, oldObject)
	require.NoError(t, err, "an active response reader must pin its generation")

	release()
	require.NoError(t, handler.CleanupRoot(ctx, "root", config.DefaultCleanupConfig()))
	_, err = store.StatObject(ctx, handler.name, oldObject)
	require.Error(t, err)
}

func TestRefreshRebuildsMetadataBeforeComparingSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"meta-v2"`)
		_, _ = io.WriteString(w, "index")
	}))
	defer server.Close()

	store := newTestStore(t)
	builderCalls := 0
	handler := newTestHandler(t, store, []string{server.URL},
		func(ctx context.Context, session *RefreshSession, paths *PathIndexBuilder) (*LiveSnapshot, error) {
			builderCalls++
			blob, err := session.FetchAnchor(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
		},
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
