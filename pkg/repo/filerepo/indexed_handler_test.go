package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string, builder SnapshotBuilder) *IndexedHandler {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo",
		"test",
		"repo",
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
		nil,
		builder,
		store,
		stats,
		health.New("repo", "test", health.DefaultConfig(), upstreams, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRoot("root", []MetadataTarget{{URL: "meta/index.txt"}})
	return handler
}

func TestRefreshUsesSingleUpstreamForGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = io.WriteString(w, "bad-index")
			return
		}
		http.NotFound(w, r)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "good-index")
		case "/meta/index.txt.sig":
			_, _ = io.WriteString(w, "good-sig")
		default:
			http.NotFound(w, r)
		}
	}))
	defer good.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{bad.URL, good.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		_, err = session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt.sig"})
		if err != nil {
			return nil, err
		}
		return &LiveSnapshot{
			Metadata: map[string]MetadataObject{
				blob.Path:            {Path: blob.Path, Required: true},
				"meta/index.txt.sig": {Path: "meta/index.txt.sig", Required: true},
			},
			Artifacts: map[string]RepoObject{"pkg.tar": {Path: "pkg.tar", Identity: sha256String("pkg")}},
		}, nil
	})

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	snapshot := handler.currentSnapshot()
	require.NotNil(t, snapshot)
	obj, ok := handler.currentRepoObject("pkg.tar", ResourceArtifact)
	require.True(t, ok)
	require.Equal(t, good.URL, obj.Upstream)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "good-index", rec.Body.String())
}

func TestRefreshDoesNotPublishWhenRequiredMetadataMissing(t *testing.T) {
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

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		_, err = session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt.sig"})
		if err != nil {
			return nil, err
		}
		return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}}}, nil
	})

	require.Error(t, handler.RefreshSubPath(ctx, "root"))
	require.Nil(t, handler.currentSnapshot())
}

func TestArtifactChecksumMismatchIsNotCached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "index")
		case "/pkg.tar":
			_, _ = io.WriteString(w, "wrong")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		return &LiveSnapshot{
			Metadata:  map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}},
			Artifacts: map[string]RepoObject{"pkg.tar": {Path: "pkg.tar", Identity: sha256String("expected"), Digest: SHA256Digest(sha256String("expected"))}},
		}, nil
	})
	require.NoError(t, handler.RefreshSubPath(ctx, "root"))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/pkg.tar", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Eventually(t, func() bool {
		_, err := store.OpenObject(ctx, "repo", "repo/pkg.tar")
		return err != nil
	}, time.Second, 10*time.Millisecond)
}

func sha256String(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestMetadataStateAfterSuccessfulRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "index")
		case "/meta/index.txt.sig":
			_, _ = io.WriteString(w, "sig")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		_, err = session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt.sig"})
		if err != nil {
			return nil, err
		}
		return &LiveSnapshot{
			Metadata: map[string]MetadataObject{
				blob.Path:            {Path: blob.Path, Required: true},
				"meta/index.txt.sig": {Path: "meta/index.txt.sig", Required: true},
			},
		}, nil
	})

	require.Nil(t, handler.currentSnapshot())
	snap := handler.stats.Snapshot()
	require.Equal(t, "booting", snap.Instances["repo"].MetadataState)

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))
	require.NotNil(t, handler.currentSnapshot())

	snap = handler.stats.Snapshot()
	require.Equal(t, "ready", snap.Instances["repo"].MetadataState)
}

func TestMetadataStateAfterFailedRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		if err != nil {
			return nil, err
		}
		return &LiveSnapshot{Metadata: map[string]MetadataObject{}}, nil
	})

	require.Nil(t, handler.currentSnapshot())
	require.Error(t, handler.RefreshSubPath(ctx, "root"))
	require.Nil(t, handler.currentSnapshot())
	state, ok := handler.sh.ResourceState("root")
	require.True(t, ok)
	require.Equal(t, health.RSuspect, state)

	snap := handler.stats.Snapshot()
	require.Equal(t, "booting", snap.Instances["repo"].MetadataState)
}

func TestRefreshPromotesResourceToActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "index")
		case "/meta/index.txt.sig":
			_, _ = io.WriteString(w, "sig")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		_, err = session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt.sig"})
		require.NoError(t, err)
		return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}, "meta/index.txt.sig": {Path: "meta/index.txt.sig", Required: true}}}, nil
	})

	state, ok := handler.sh.ResourceState("root")
	require.True(t, ok)
	require.Equal(t, health.RPending, state)

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))

	state, ok = handler.sh.ResourceState("root")
	require.True(t, ok)
	require.Equal(t, health.RActive, state)
}

func TestRefreshSkipsWhenSameSubPathAlreadyRefreshing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/meta/index.txt":
			_, _ = io.WriteString(w, "index")
		case "/meta/index.txt.sig":
			_, _ = io.WriteString(w, "sig")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	builderCalls := make(chan struct{}, 2)
	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		builderCalls <- struct{}{}
		select {
		case started <- struct{}{}:
		default:
		}
		<-unblock
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		_, err = session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt.sig"})
		require.NoError(t, err)
		return &LiveSnapshot{Metadata: map[string]MetadataObject{blob.Path: {Path: blob.Path, Required: true}, "meta/index.txt.sig": {Path: "meta/index.txt.sig", Required: true}}}, nil
	})

	refreshErr := make(chan error, 1)
	go func() {
		refreshErr <- handler.RefreshSubPath(ctx, "root")
	}()
	<-started
	require.ErrorIs(t, handler.RefreshSubPath(ctx, "root"), scheduler.ErrTaskSkipped)
	close(unblock)
	require.NoError(t, <-refreshErr)
	require.Len(t, builderCalls, 1)
}

func TestFailedRefreshCleansMetadataTempFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = io.WriteString(w, strings.Repeat("index\n", 1024))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		_, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		require.NoError(t, err)
		return nil, errors.New("builder failed")
	})

	require.Error(t, handler.RefreshSubPath(ctx, "root"))
	entries, err := os.ReadDir(tmp)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestRestoreGenerationsHealthTransition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		panic("should not be called during restore")
	})

	require.Nil(t, handler.currentSnapshot())

	handler.mu.Lock()
	handler.roots["root"].targets = nil
	handler.mu.Unlock()

	err = handler.publishSnapshot(ctx, &LiveSnapshot{
		RootKey:    "root",
		Generation: "1",
		Upstream:   server.URL,
		Published:  time.Now().UTC(),
		Metadata:   map[string]MetadataObject{},
		Targets:    []MetadataTarget{{URL: "meta/index.txt"}, {URL: "meta/debian.tar"}},
	})
	require.NoError(t, err)

	preState, preOk := handler.sh.ResourceState("root")
	require.True(t, preOk)
	require.Equal(t, health.RPending, preState)

	handler.restoreGenerations(ctx)

	snap := handler.currentSnapshot()
	require.NotNil(t, snap)

	handler.mu.RLock()
	rootSnap, ok := handler.rootSnapshots["root"]
	handler.mu.RUnlock()
	require.True(t, ok)
	require.Equal(t, "root", rootSnap.RootKey)

	handler.mu.RLock()
	entry, found := handler.roots["root"]
	handler.mu.RUnlock()
	require.True(t, found)
	require.Len(t, entry.targets, 2)
	require.Equal(t, "meta/index.txt", entry.targets[0].URL)

	state, ok := handler.sh.ResourceState("root")
	require.True(t, ok)
	require.Equal(t, health.RPending, state)

	psnap := handler.stats.Snapshot()
	require.Equal(t, "degraded", psnap.Instances["repo"].MetadataState)

	_, err = store.Put(ctx, "repo", handler.currentPath("root"), bytes.NewReader([]byte("generation: missing\n")), nil)
	require.NoError(t, err)
	restored := newTestHandler(t, store, []string{server.URL}, nil)
	restored.restoreGenerations(ctx)
	require.NotNil(t, restored.currentSnapshot())
	restored.mu.RLock()
	restoredRoot := restored.rootSnapshots["root"]
	restored.mu.RUnlock()
	require.NotNil(t, restoredRoot)
	require.Equal(t, "1", restoredRoot.Generation)
}

func TestAggregateSnapshotDoesNotCopyArtifacts(t *testing.T) {
	handler := &IndexedHandler{
		rootSnapshots: map[string]*LiveSnapshot{
			"root": {
				Metadata: map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt"}},
				Artifacts: map[string]RepoObject{
					"pkg.tar": {Path: "pkg.tar", Upstream: "https://upstream.example"},
				},
			},
		},
	}

	handler.mu.Lock()
	handler.rebuildAggregateLocked()
	handler.mu.Unlock()

	snapshot := handler.currentSnapshot()
	require.NotNil(t, snapshot)
	require.Contains(t, snapshot.Metadata, "meta/index.txt")
	require.Empty(t, snapshot.Artifacts)

	obj, ok := handler.currentRepoObject("pkg.tar", ResourceArtifact)
	require.True(t, ok)
	require.Equal(t, "https://upstream.example", obj.Upstream)
}

func TestMetadataRequestWithoutCurrentGenerationReturnsUnavailable(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{"https://upstream.example"}, nil,
		config.Expiration(time.Hour), &Policy{},
		testDiscoverer{spec: staticRootSpec{key: "root", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		nil,
		store, stats,
		health.New("repo", "test", health.DefaultConfig(), []string{"https://upstream.example"}, stats, "test"),
		nil,
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
}

func TestArtifactWithoutCurrentGenerationUsesUnindexedPlan(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{"https://upstream.example"}, nil,
		config.Expiration(time.Hour), &Policy{},
		nil, nil,
		store, stats,
		health.New("repo", "test", health.DefaultConfig(), []string{"https://upstream.example"}, stats, "test"),
		nil,
	)

	route, err := (&generationResolver{handler: handler, policy: &Policy{ArtifactPolicy: config.PolicyImmutable}}).Resolve(httptest.NewRequest(http.MethodGet, "/pool/pkg.deb", nil))
	require.NoError(t, err)
	require.Equal(t, "repo/pool/pkg.deb", route.ObjectPath)
	require.Empty(t, route.TargetURL)
	require.Empty(t, route.PreferredUpstream)
}

func TestIndexedArtifactAndAuxiliaryUseGenerationUpstream(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		func(cleanPath string) ResourceClass {
			if strings.HasSuffix(cleanPath, ".sig") {
				return ResourceAuxiliary
			}
			return ResourceArtifact
		},
		[]string{"https://first.example", "https://second.example"}, nil,
		config.Expiration(time.Hour), &Policy{},
		nil, nil,
		store, stats,
		health.New("repo", "test", health.DefaultConfig(), []string{"https://first.example", "https://second.example"}, stats, "test"),
		nil,
	)
	handler.mu.Lock()
	handler.rootSnapshots["root"] = &LiveSnapshot{
		Metadata: map[string]MetadataObject{},
		Artifacts: map[string]RepoObject{
			"pkg.tar": {
				Path:       "pkg.tar",
				Identity:   "id",
				Upstream:   "https://second.example",
				RootKey:    "root",
				Generation: "7",
			},
		},
	}
	handler.mu.Unlock()
	policy := &Policy{ArtifactPolicy: config.PolicyImmutable, AuxiliaryPolicy: config.PolicyImmutable}

	route, err := (&generationResolver{handler: handler, policy: policy}).Resolve(httptest.NewRequest(http.MethodGet, "/pkg.tar", nil))
	require.NoError(t, err)
	require.Equal(t, "https://second.example/pkg.tar", route.TargetURL)
	require.Equal(t, "https://second.example", route.PreferredUpstream)

	route, err = (&generationResolver{handler: handler, policy: policy}).Resolve(httptest.NewRequest(http.MethodGet, "/pkg.tar.sig", nil))
	require.NoError(t, err)
	require.Equal(t, "https://second.example/pkg.tar.sig", route.TargetURL)
	require.Equal(t, "https://second.example", route.PreferredUpstream)
}

func TestRootReleasesWithoutHealthDoesNotPanic(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := NewIndexedHandler(
		"repo", "test", "repo",
		func(string) ResourceClass { return ResourceArtifact },
		[]string{"https://upstream.example"}, nil,
		config.Expiration(time.Hour), &Policy{},
		nil, nil,
		store, httpcache.NewStats(prometheus.NewRegistry()),
		nil,
		nil,
	)
	handler.rootSnapshots["root"] = &LiveSnapshot{
		RootKey:    "root",
		Generation: "7",
		Upstream:   "https://upstream.example",
		Published:  time.Now(),
		Metadata:   map[string]MetadataObject{"meta/index.txt": {Path: "meta/index.txt"}},
		Artifacts:  map[string]RepoObject{"pkg.tar": {Path: "pkg.tar"}},
	}

	releases := handler.RootReleases()
	require.Len(t, releases, 1)
	require.Equal(t, "root", releases[0].Key)
	require.Equal(t, "7", releases[0].Generation)
	require.Empty(t, releases[0].State)
}

func TestCleanupKeepsUnindexedObjects(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{"https://upstream.example"}, func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
		return &LiveSnapshot{
			Metadata:  map[string]MetadataObject{},
			Artifacts: map[string]RepoObject{},
		}, nil
	})
	handler.mu.Lock()
	handler.rootSnapshots["root"] = &LiveSnapshot{Metadata: map[string]MetadataObject{}, Artifacts: map[string]RepoObject{}}
	handler.mu.Unlock()

	require.NoError(t, store.MkdirAll("repo/repo", 0o755))
	_, err = store.Put(ctx, "repo", "repo/pkg.tar", strings.NewReader("cached"), map[string]string{"cache": "MISS"})
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, config.CleanupConfig{}))
	reader, err := store.OpenObject(ctx, "repo", "repo/pkg.tar")
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestCleanupDeletesStaleIndexedObjects(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{"https://upstream.example"}, func(context.Context, *RefreshSession) (*LiveSnapshot, error) {
		return &LiveSnapshot{
			Metadata:  map[string]MetadataObject{},
			Artifacts: map[string]RepoObject{},
		}, nil
	})
	handler.mu.Lock()
	handler.rootSnapshots["root"] = &LiveSnapshot{Metadata: map[string]MetadataObject{}, Artifacts: map[string]RepoObject{}}
	handler.mu.Unlock()

	require.NoError(t, store.MkdirAll("repo/repo", 0o755))
	_, err = store.Put(ctx, "repo", "repo/pkg.tar", strings.NewReader("cached"), map[string]string{
		"indexed":          "true",
		"indexed-identity": "old",
	})
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, config.CleanupConfig{}))
	_, err = store.OpenObject(ctx, "repo", "repo/pkg.tar")
	require.Error(t, err)
}

func TestMetadataRequestServesAfterRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = io.WriteString(w, "generated")
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newTestHandler(t, store, []string{server.URL}, func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
		blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
		if err != nil {
			return nil, err
		}
		return &LiveSnapshot{
			Metadata: map[string]MetadataObject{
				blob.Path: {Path: blob.Path, Required: true},
			},
		}, nil
	})
	require.NoError(t, handler.Start(ctx))
	defer func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, handler.Stop(stopCtx))
	}()

	require.NoError(t, handler.RefreshSubPath(ctx, "root"))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "generated", rec.Body.String())
}

type testDiscoverer struct {
	spec RootSpec
}

func (d testDiscoverer) Discover(string) (RootSpec, bool) {
	return d.spec, true
}

func TestRefreshSubPathNoTargets(t *testing.T) {
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"http://noop"}, nil)
	handler.SetBus(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler.Start(ctx)
	defer handler.Stop(ctx)

	handler.AddRoot("empty", nil)

	err := handler.RefreshSubPath(ctx, "empty")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no targets")
}

func TestRefreshSubPathNonExistent(t *testing.T) {
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"http://noop"}, nil)
	handler.SetBus(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler.Start(ctx)
	defer handler.Stop(ctx)

	err := handler.RefreshSubPath(ctx, "nonexistent")
	require.Error(t, err)
}

func TestCleanupSubPathNoGenerations(t *testing.T) {
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"http://noop"}, nil)
	handler.SetBus(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler.Start(ctx)
	defer handler.Stop(ctx)

	err := handler.CleanupSubPath(ctx, "nonexistent")
	require.NoError(t, err)
}

func TestServeHTTPEmptyPathPassthrough(t *testing.T) {
	store := newTestStore(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("passthrough-ok"))
	}))
	defer upstream.Close()

	handler := newTestHandler(t, store, []string{upstream.URL}, nil)
	handler.SetBus(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler.Start(ctx)
	defer handler.Stop(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "passthrough-ok", rec.Body.String())
}

func TestMetadataBypassWhenNotCached(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("bypass-meta"))
	}))
	defer upstream.Close()

	store := newTestStore(t)
	handler := newTestHandlerWithDiscoverer(t, store, []string{upstream.URL}, nil)
	b := bus.New()
	handler.SetBus(b)
	ch := b.Subscribe(bus.EventMetadataDiscovered)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler.Start(ctx)
	defer handler.Stop(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "bypass-meta", rec.Body.String())

	select {
	case evt := <-ch:
		require.Equal(t, bus.EventMetadataDiscovered, evt.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("discovery event not published on metadata miss")
	}
}

func newTestHandlerWithDiscoverer(t *testing.T, store *blobfs.Store, upstreams []string, builder SnapshotBuilder) *IndexedHandler {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			if strings.HasSuffix(cleanPath, ".sig") {
				return ResourceAuxiliary
			}
			return ResourceArtifact
		},
		upstreams, nil, config.Expiration(time.Hour), &Policy{},
		testDiscoverer{spec: staticRootSpec{key: "root", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		builder, store, stats,
		health.New("repo", "test", health.DefaultConfig(), upstreams, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRoot("root", []MetadataTarget{{URL: "meta/index.txt"}})
	return handler
}

func TestDiscoverSubPathNilDiscoverer(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	handler := NewIndexedHandler("test", "test", "test",
		nil, []string{"http://x"}, nil, config.Expiration(time.Hour), &Policy{},
		nil, nil, store,
		httpcache.NewStats(prometheus.NewRegistry()),
		health.New("test", "test", health.DefaultConfig(), []string{"http://x"}, nil, "ua"),
		nil,
	)

	subPath, discovered := handler.discoverSubPath("anything")
	require.False(t, discovered)
	require.Equal(t, "", subPath)
}

func TestDiscoverSubPathReturnsBusEventData(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()
	handler := buildHandler(t, store, func(cleanPath string) ResourceClass {
		if strings.HasPrefix(cleanPath, "meta/") {
			return ResourceMetadata
		}
		return ResourceArtifact
	}, nil)
	handler.SetBus(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler.Start(ctx)
	defer handler.Stop(ctx)

	spec := staticRootSpec{key: "rootkey", targets: []MetadataTarget{{URL: "meta/data.json"}}}
	handler.discover = testDiscoverer{spec: spec}

	subPath, discovered := handler.discoverSubPath("meta/data.json")
	require.True(t, discovered)
	require.Equal(t, "rootkey", subPath)
}

func buildHandler(t *testing.T, store *blobfs.Store, classifier func(string) ResourceClass, builder SnapshotBuilder) *IndexedHandler {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	return NewIndexedHandler("test", "test", "test", classifier,
		[]string{"http://noop"}, nil, config.Expiration(time.Hour), &Policy{},
		nil, builder, store, stats,
		health.New("test", "test", health.DefaultConfig(), []string{"http://noop"}, stats, "ua"),
		nil,
	)
}

func newTestStore(t *testing.T) *blobfs.Store {
	t.Helper()
	dir := t.TempDir()
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}
