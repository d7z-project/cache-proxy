package filerepo

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type testDiscoverer struct{}

func (testDiscoverer) Discover(cleanPath string) (RootSpec, bool) {
	if cleanPath != "meta/index.txt" {
		return nil, false
	}
	return staticRootSpec{
		key:     "meta",
		targets: []MetadataTarget{{URL: "meta/index.txt"}},
	}, true
}

func newTestHealth(t *testing.T, stats *httpcache.Stats) *health.ServiceHealth {
	return health.New("repo", "test", health.Config{}, nil, stats, "cache-proxy-test")
}

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string, discover Discoverer, seeds []RootSpec, builder SnapshotBuilder) *IndexedHandler {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	return NewIndexedHandler(
		"repo",
		"test",
		"repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			if strings.HasSuffix(cleanPath, ".sig") {
				return ResourceAuxiliary
			}
			return ResourceArtifact
		},
		upstreams,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		discover,
		seeds,
		builder,
		store,
		stats,
		newTestHealth(t, stats),
	)
}

func TestIndexedHandlerRefreshInvalidatesArtifactIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	currentIdentity := "v1"
	currentBody := "artifact-v1"
	var upstreamHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "meta")
		case "/pkg.tar.zst":
			_, _ = io.WriteString(w, currentBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		nil,
		[]RootSpec{staticRootSpec{
			key:     "meta",
			targets: []MetadataTarget{{URL: "meta/index.txt"}},
		}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{
				Metadata:  map[string]struct{}{"meta/index.txt": {}},
				Artifacts: map[string]string{"pkg.tar.zst": currentIdentity},
				Auxiliary: map[string]string{},
			}, nil
		},
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/pkg.tar.zst", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "artifact-v1", rec.Body.String())

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/pkg.tar.zst", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "artifact-v1", rec.Body.String())
	require.Equal(t, int64(2), upstreamHits.Load())

	currentIdentity = "v2"
	currentBody = "artifact-v2"
	require.NoError(t, handler.Refresh(ctx))

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/pkg.tar.zst", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "artifact-v2", rec.Body.String())
	require.Equal(t, int64(3), upstreamHits.Load())

	snapshot := stats.Snapshot().Instances["repo"]
	require.Equal(t, "ready", snapshot.MetadataState)
	require.True(t, snapshot.SnapshotReady)
	require.Equal(t, uint64(2), snapshot.Refreshes)
}

func TestResolveMetadataRefreshPolicyValues(t *testing.T) {
	require.Equal(t, time.Hour, ResolveMetadataRefreshInterval(0, time.Hour))
	require.Equal(t, 45*time.Second, ResolveMetadataRefreshInterval(config.Duration(45*time.Second), time.Hour))
}

func TestIndexedHandlerRefreshWithoutRootsStaysBooting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		config.Freshness(time.Minute),
		func(string) ResourceClass { return ResourceMetadata },
		nil,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		nil,
		nil,
		func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
			return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
		},
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))
	instance := stats.Snapshot().Instances["repo"]
	require.Equal(t, "booting", instance.MetadataState)
	require.False(t, instance.SnapshotReady)
}

func TestIndexedHandlerCleanupRemovesOrphanArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, nil, nil, nil, nil)
	handler.snapshot = &LiveSnapshot{
		Metadata:  map[string]struct{}{"meta/index.txt": {}},
		Artifacts: map[string]string{"keep.pkg": "v1"},
		Auxiliary: map[string]string{},
	}

	require.NoError(t, store.MkdirAll("repo/repo", 0o755))
	_, err = store.Put(ctx, "repo", "repo/keep.pkg", strings.NewReader("keep"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)
	_, err = store.Put(ctx, "repo", "repo/drop.pkg", strings.NewReader("drop"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx))

	_, err = store.OpenObject(ctx, "repo", "repo/keep.pkg")
	require.NoError(t, err)
	_, err = store.OpenObject(ctx, "repo", "repo/drop.pkg")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestIndexedHandlerStartRefreshesSeedRoots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	var calls atomic.Int64
	handler := newTestHandler(t, store, nil, nil, []RootSpec{staticRootSpec{
		key:     "meta",
		targets: []MetadataTarget{{URL: "meta/index.txt"}},
	}}, func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
		calls.Add(1)
		return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
	})

	require.NoError(t, handler.Start(ctx))
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, 10*time.Millisecond)
	require.NoError(t, handler.Stop(context.Background()))
}

func TestIndexedHandlerFailedRefreshKeepsPreviousSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	fail := atomic.Bool{}
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		config.Freshness(time.Minute),
		func(string) ResourceClass { return ResourceArtifact },
		nil,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		nil,
		[]RootSpec{staticRootSpec{
			key:     "meta",
			targets: []MetadataTarget{{URL: "meta/index.txt"}},
		}},
		func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
			if fail.Load() {
				return nil, errors.New("broken metadata")
			}
			return &LiveSnapshot{Artifacts: map[string]string{"keep.pkg": "v1"}}, nil
		},
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))
	fail.Store(true)
	require.Error(t, handler.Refresh(ctx))

	snapshot := handler.currentSnapshot()
	require.Equal(t, "v1", snapshot.Artifacts["keep.pkg"])
	instance := stats.Snapshot().Instances["repo"]
	require.True(t, instance.SnapshotReady)
	require.Equal(t, uint64(2), instance.Refreshes)
	require.Equal(t, uint64(1), instance.RefreshFailures)
	require.Equal(t, "parse_error", instance.LastRefresh)
}

func TestIndexedHandlerMetadataRequestDiscoversAndRefreshesRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "meta")
		case "/pkg.tar.zst":
			_, _ = io.WriteString(w, "artifact")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: 0},
		testDiscoverer{},
		nil,
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{
				Metadata:  map[string]struct{}{"meta/index.txt": {}},
				Artifacts: map[string]string{"pkg.tar.zst": "v1"},
			}, nil
		},
		store,
		stats,
		svcHealth,
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, rec.Code)

	_, ok := handler.targets["meta"]
	require.True(t, ok)
	require.Eventually(t, func() bool {
		snap := handler.currentSnapshot()
		if snap == nil {
			return false
		}
		_, ok := snap.Metadata["meta/index.txt"]
		return ok
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, "v1", handler.currentSnapshot().Artifacts["pkg.tar.zst"])
}

func TestDiscoverRootTriggersAsyncRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "meta")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	var refreshCount atomic.Int64
	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Minute},
		testDiscoverer{},
		nil,
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			refreshCount.Add(1)
			return &LiveSnapshot{Metadata: map[string]struct{}{"meta/index.txt": {}}}, nil
		},
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Start(ctx))
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = handler.Stop(stopCtx)
	}()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, rec.Code)
	_, ok := handler.targets["meta"]
	require.True(t, ok, "root discovered and added")

	require.Eventually(t, func() bool { return refreshCount.Load() >= 1 }, 2*time.Second, 10*time.Millisecond)
	require.Contains(t, handler.currentSnapshot().Metadata, "meta/index.txt")
}

func TestIndexedHandlerArtifactRequestDoesNotDiscoverRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, testDiscoverer{}, nil, func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
		return &LiveSnapshot{}, nil
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/pkg.tar.zst", nil).WithContext(ctx))
	require.Equal(t, http.StatusNotFound, rec.Code)
	_, ok := handler.targets["meta"]
	require.False(t, ok)
}

func TestIndexedHandlerEmptyPathBypassesUpstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "root")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, testDiscoverer{}, nil, func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
		return &LiveSnapshot{}, nil
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "root", rec.Body.String())
	_, ok := handler.targets["meta"]
	require.False(t, ok)
}

func TestIndexedHandlerUnknownPathBypassesUpstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "other")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if cleanPath == "meta/index.txt" {
				return ResourceMetadata
			}
			return ResourceUnknown
		},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		testDiscoverer{},
		nil,
		func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
			return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
		},
		store,
		stats,
		svcHealth,
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/robots.txt", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "other", rec.Body.String())
	require.Empty(t, handler.targets)
}

func TestIndexedHandlerOnlyMetadataRequestsCanDiscoverRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
		config.Freshness(time.Minute),
		func(string) ResourceClass { return ResourceArtifact },
		nil,
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		testDiscoverer{},
		nil,
		func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
			return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
		},
		store,
		stats,
		svcHealth,
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil).WithContext(ctx))
	require.Empty(t, handler.targets)
}

func TestIndexedHandlerCleanupKeepsTrackedMetadataCompanions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, nil, nil, nil, nil)
	handler.snapshot = &LiveSnapshot{
		Metadata: map[string]struct{}{
			"meta/index.txt":     {},
			"meta/index.txt.sig": {},
		},
		Artifacts: map[string]string{},
		Auxiliary: map[string]string{},
	}

	require.NoError(t, store.MkdirAll("repo/repo/meta", 0o755))
	for _, name := range []string{"repo/meta/index.txt", "repo/meta/index.txt.sig"} {
		_, err = store.Put(ctx, "repo", name, strings.NewReader("data"), map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)})
		require.NoError(t, err)
	}

	require.NoError(t, handler.Cleanup(ctx))
	for _, name := range []string{"repo/meta/index.txt", "repo/meta/index.txt.sig"} {
		_, err = store.OpenObject(ctx, "repo", name)
		require.NoError(t, err)
	}
}

func TestIndexedHandlerRemovesRootAfterRepeatedMetadataNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	missing := atomic.Bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/meta/index.txt" || missing.Load() {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, "meta")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := health.New("repo", "test", health.Config{
		RemovalThreshold: 2,
		MinNotFoundAge:   0,
	}, []string{server.URL}, stats, "cache-proxy-test")
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(0),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		nil,
		[]RootSpec{staticRootSpec{
			key:     "meta",
			targets: []MetadataTarget{{URL: "meta/index.txt"}},
		}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			if _, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"}); err != nil {
				return nil, err
			}
			return &LiveSnapshot{
				Metadata:  map[string]struct{}{"meta/index.txt": {}},
				Artifacts: map[string]string{"pkg.tar.zst": "v1"},
			}, nil
		},
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))
	require.NotNil(t, handler.currentSnapshot())

	missing.Store(true)
	err = handler.Refresh(ctx)
	require.ErrorIs(t, err, errMetadataNotFound)

	state, ok := svcHealth.ResourceState("meta")
	require.True(t, ok)
	require.Equal(t, health.RSuspect, state)
	require.NotNil(t, handler.currentSnapshot())

	err = handler.Refresh(ctx)
	require.ErrorIs(t, err, errMetadataNotFound)

	state, ok = svcHealth.ResourceState("meta")
	require.False(t, ok, "root must be deleted after removal threshold")
	require.Empty(t, handler.currentSnapshot().Artifacts)
}

func TestIndexedHandlerConcurrentRefreshSkipsSecondRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	var calls atomic.Int64
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := newTestHandler(t, store, nil, nil, []RootSpec{staticRootSpec{
		key:     "meta",
		targets: []MetadataTarget{{URL: "meta/index.txt"}},
	}}, func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		handler.runRefreshCycle(ctx)
	}()
	<-started
	go func() {
		defer wg.Done()
		handler.runRefreshCycle(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	require.Equal(t, int64(1), calls.Load())
}

func TestRefreshSessionFetchFallsBackToCandidatePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var upstreamHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		switch r.URL.Path {
		case "/dists/bookworm/InRelease":
			http.NotFound(w, r)
		case "/dists/bookworm/Release":
			_, _ = io.WriteString(w, "release")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, nil, nil, nil)
	session := &RefreshSession{
		handler: handler,
		blobs:   map[string]MetadataBlob{},
	}
	blob, err := session.Fetch(ctx, MetadataTarget{
		URL:        "dists/bookworm/InRelease",
		Candidates: []string{"dists/bookworm/Release"},
	})
	require.NoError(t, err)
	require.Equal(t, "dists/bookworm/Release", blob.Path)
	require.Equal(t, int64(2), upstreamHits.Load())
}

func TestRestoreRootsDiscoversFromCachedMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "meta")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	h1 := newTestHandler(t, store, []string{server.URL}, testDiscoverer{},
		[]RootSpec{staticRootSpec{key: "meta", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]struct{}{"meta/index.txt": {}}}, nil
		})
	require.NoError(t, h1.Refresh(ctx))
	_, ok := h1.targets["meta"]
	require.True(t, ok)

	h2 := newTestHandler(t, store, []string{server.URL}, testDiscoverer{}, nil, nil)
	h2.restoreRoots(ctx)

	_, ok = h2.targets["meta"]
	require.True(t, ok)
}

func TestRefreshUsesCachedMetadataWhenFresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var upstreamHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.WriteString(w, "meta")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, nil,
		[]RootSpec{staticRootSpec{key: "meta", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]struct{}{"meta/index.txt": {}}}, nil
		})

	require.NoError(t, handler.Refresh(ctx))
	require.Equal(t, int64(1), upstreamHits.Load())

	require.NoError(t, handler.Refresh(ctx))
	require.Equal(t, int64(1), upstreamHits.Load(), "second refresh within fresh window must not hit upstream")
}

func TestRefreshFallsBackToCacheOnUpstream500(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fail := atomic.Bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "meta")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, nil,
		[]RootSpec{staticRootSpec{key: "meta", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]struct{}{"meta/index.txt": {}}, Artifacts: map[string]string{"pkg": "v1"}}, nil
		})
	require.NoError(t, handler.Refresh(ctx))
	require.NotNil(t, handler.currentSnapshot())

	fail.Store(true)
	require.NoError(t, handler.Refresh(ctx))
	require.NotNil(t, handler.currentSnapshot(), "snapshot must survive upstream 500 via cache fallback")
	_, ok := handler.targets["meta"]
	require.True(t, ok)
}

func TestStateRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(string) ResourceClass { return ResourceMetadata },
		nil, nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		testDiscoverer{},
		[]RootSpec{staticRootSpec{key: "meta", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		nil,
		store,
		stats,
		svcHealth,
	)

	handler.addRoot("meta", []MetadataTarget{{URL: "meta/index.txt"}})
	handler.saveState(ctx)

	state := handler.loadState(ctx)
	require.Equal(t, 1, state.Version)
	require.Len(t, state.Roots, 1)
	require.Equal(t, "meta", state.Roots[0].Path)
	require.Equal(t, "pending", state.Roots[0].State)
}

func TestStartRestoresStateThenRefreshes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var upstreamHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.WriteString(w, "meta")
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	makeHandler := func() *IndexedHandler {
		stats := httpcache.NewStats(prometheus.NewRegistry())
		return NewIndexedHandler(
			"repo", "test", "repo",
			config.Freshness(time.Minute),
			func(cleanPath string) ResourceClass {
				if strings.HasPrefix(cleanPath, "meta/") {
					return ResourceMetadata
				}
				return ResourceArtifact
			},
			[]string{server.URL},
			nil,
			config.Expiration(time.Hour),
			&Policy{},
			RefreshPolicy{Interval: time.Hour},
			testDiscoverer{},
			[]RootSpec{staticRootSpec{key: "meta", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
			func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
				_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
				if err != nil {
					return nil, err
				}
				return &LiveSnapshot{Metadata: map[string]struct{}{"meta/index.txt": {}}}, nil
			},
			store,
			stats,
			newTestHealth(t, stats),
		)
	}

	h1 := makeHandler()
	require.NoError(t, h1.Start(ctx))
	_, ok := h1.targets["meta"]
	require.True(t, ok)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_ = h1.Stop(stopCtx)

	h2 := makeHandler()
	h2.restoreRoots(ctx)

	_, ok = h2.targets["meta"]
	require.True(t, ok)
}

func TestRefreshConditionalGET304(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var upstreamHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		etag := r.Header.Get("If-None-Match")
		if etag == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Write([]byte("meta-content"))
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(0),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		nil,
		[]RootSpec{staticRootSpec{key: "meta", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			require.NoError(t, err)
			return &LiveSnapshot{Metadata: map[string]struct{}{"meta/index.txt": {}}}, nil
		},
		store,
		stats,
		svcHealth,
	)

	require.NoError(t, handler.Refresh(ctx))
	require.Equal(t, int64(1), upstreamHits.Load(), "first refresh fetches upstream")

	require.NoError(t, handler.Refresh(ctx))
	require.Equal(t, int64(2), upstreamHits.Load(), "second refresh sends conditional GET (304)")
}

func TestRestoreRootsPreservesMergeForDEB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("deb-meta"))
	}))
	defer upstream.Close()

	debDiscoverer := discovererFunc(func(cleanPath string) (RootSpec, bool) {
		parts := strings.Split(strings.Trim(cleanPath, "/"), "/")
		if len(parts) != 2 {
			return nil, false
		}
		return staticRootSpec{
			key:     parts[0],
			targets: []MetadataTarget{{URL: parts[0] + "/" + parts[1]}},
		}, true
	})

	stats := httpcache.NewStats(prometheus.NewRegistry())
	svcHealth := newTestHealth(t, stats)
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "bookworm/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{upstream.URL},
		nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		debDiscoverer,
		[]RootSpec{staticRootSpec{key: "bookworm", targets: []MetadataTarget{{URL: "bookworm/main"}}}},
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			_, _ = session.Fetch(ctx, MetadataTarget{URL: "bookworm/main"})
			return &LiveSnapshot{Metadata: map[string]struct{}{"bookworm/main": {}}}, nil
		},
		store,
		stats,
		svcHealth,
	)
	require.NoError(t, handler.Refresh(ctx))

	_, ok := handler.targets["bookworm"]
	require.True(t, ok)

	handler.saveState(ctx)

	stats2 := httpcache.NewStats(prometheus.NewRegistry())
	h2 := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "bookworm/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		nil, nil,
		config.Expiration(time.Hour),
		&Policy{},
		RefreshPolicy{Interval: time.Hour},
		debDiscoverer,
		nil,
		nil,
		store,
		stats2,
		newTestHealth(t, stats2),
	)
	h2.restoreRoots(ctx)

	_, ok = h2.targets["bookworm"]
	require.True(t, ok)
}

type discovererFunc func(string) (RootSpec, bool)

func (f discovererFunc) Discover(path string) (RootSpec, bool) { return f(path) }
