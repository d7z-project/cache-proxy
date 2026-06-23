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

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string, discover Discoverer, seeds []RootSpec, builder SnapshotBuilder) *IndexedHandler {
	t.Helper()
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
		RefreshPolicy{Interval: time.Hour, Timeout: time.Second},
		discover,
		seeds,
		builder,
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
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
		RefreshPolicy{Interval: time.Hour, Timeout: time.Second},
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
	require.Equal(t, int64(4), upstreamHits.Load())

	snapshot := stats.Snapshot().Instances["repo"]
	require.Equal(t, "ready", snapshot.MetadataState)
	require.True(t, snapshot.SnapshotReady)
	require.Equal(t, uint64(2), snapshot.Refreshes)
}

func TestResolveMetadataRefreshPolicyValues(t *testing.T) {
	require.Equal(t, 2*time.Minute, ResolveMetadataRefreshTimeout(0))
	require.Equal(t, 30*time.Second, ResolveMetadataRefreshTimeout(config.Duration(30*time.Second)))
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
		RefreshPolicy{Interval: time.Hour, Timeout: time.Second},
		nil,
		nil,
		func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
			return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
		},
		store,
		stats,
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
		RefreshPolicy{Interval: time.Hour, Timeout: time.Second},
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

	handler := newTestHandler(t, store, []string{server.URL}, testDiscoverer{}, nil, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		return &LiveSnapshot{
			Metadata:  map[string]struct{}{"meta/index.txt": {}},
			Artifacts: map[string]string{"pkg.tar.zst": "v1"},
		}, nil
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil).WithContext(ctx))
	require.Equal(t, http.StatusOK, rec.Code)

	record := handler.roots["meta"]
	require.NotNil(t, record)
	require.Equal(t, RepositoryStateActive, record.State)
	require.Contains(t, handler.currentSnapshot().Metadata, "meta/index.txt")
	require.Equal(t, "v1", handler.currentSnapshot().Artifacts["pkg.tar.zst"])
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
	require.Empty(t, handler.roots)
}

func TestIndexedHandlerOnlyMetadataRequestsCanDiscoverRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

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
		RefreshPolicy{Interval: time.Hour, Timeout: time.Second},
		testDiscoverer{},
		nil,
		func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
			return &LiveSnapshot{Metadata: map[string]struct{}{}}, nil
		},
		store,
		httpcache.NewStats(prometheus.NewRegistry()),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil).WithContext(ctx))
	require.Empty(t, handler.roots)
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

	handler := newTestHandler(t, store, []string{server.URL}, nil, []RootSpec{staticRootSpec{
		key:     "meta",
		targets: []MetadataTarget{{URL: "meta/index.txt"}},
	}}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		if _, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"}); err != nil {
			return nil, err
		}
		return &LiveSnapshot{
			Metadata:  map[string]struct{}{"meta/index.txt": {}},
			Artifacts: map[string]string{"pkg.tar.zst": "v1"},
		}, nil
	})
	handler.removal = RemovalPolicy{ConsecutiveNotFound: 2, MinNotFoundAge: 0}

	require.NoError(t, handler.Refresh(ctx))
	require.NotNil(t, handler.currentSnapshot())

	missing.Store(true)
	err = handler.Refresh(ctx)
	require.ErrorIs(t, err, errMetadataNotFound)
	record := handler.roots["meta"]
	require.Equal(t, RepositoryStateSuspect, record.State)
	require.NotNil(t, record.Snapshot)

	err = handler.Refresh(ctx)
	require.ErrorIs(t, err, errMetadataNotFound)
	require.Equal(t, RepositoryStateRemoved, record.State)
	require.Nil(t, record.Snapshot)
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
