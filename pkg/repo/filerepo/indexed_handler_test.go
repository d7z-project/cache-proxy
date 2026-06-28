package filerepo

import (
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

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string, builder SnapshotBuilder) *IndexedHandler {
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
		nil,
		[]RootSpec{staticRootSpec{key: "root", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		builder,
		store,
		stats,
		health.New("repo", "test", health.DefaultConfig(), upstreams, stats, "cache-proxy-test"),
	)
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

	require.NoError(t, handler.Refresh(ctx))
	snapshot := handler.currentSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, good.URL, snapshot.Artifacts["pkg.tar"].Upstream)

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

	require.Error(t, handler.Refresh(ctx))
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
			Artifacts: map[string]RepoObject{"pkg.tar": {Path: "pkg.tar", Identity: sha256String("expected"), ContentHash: sha256String("expected")}},
		}, nil
	})
	require.NoError(t, handler.Refresh(ctx))

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

	require.NoError(t, handler.Refresh(ctx))
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
	require.Error(t, handler.Refresh(ctx))
	require.Nil(t, handler.currentSnapshot())

	snap := handler.stats.Snapshot()
	require.Equal(t, "booting", snap.Instances["repo"].MetadataState)
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

	require.Error(t, handler.Refresh(ctx))
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
	require.Equal(t, health.RActive, state)

	psnap := handler.stats.Snapshot()
	require.Equal(t, "ready", psnap.Instances["repo"].MetadataState)
}

func TestEnsureFirstRefreshWithNilBuilder(t *testing.T) {
	handler := &IndexedHandler{
		build: nil,
	}
	handler.ensureFirstRefresh("test-key")
}

func TestMetadataPassthroughWhenNoSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/meta/index.txt" {
			_, _ = io.WriteString(w, "from-upstream")
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL}, nil,
		config.Expiration(time.Hour), &Policy{},
		RefreshPolicy{Interval: time.Hour},
		nil, []RootSpec{}, nil,
		store, stats,
		health.New("repo", "test", health.DefaultConfig(), []string{server.URL}, stats, "test"),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "from-upstream", rec.Body.String())
}

func TestMetadataRequestWaitsForFirstRefresh(t *testing.T) {
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

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := NewIndexedHandler(
		"repo", "test", "repo",
		config.Freshness(time.Minute),
		func(cleanPath string) ResourceClass {
			if strings.HasPrefix(cleanPath, "meta/") {
				return ResourceMetadata
			}
			return ResourceArtifact
		},
		[]string{server.URL}, nil,
		config.Expiration(time.Hour), &Policy{},
		RefreshPolicy{Interval: time.Hour},
		testDiscoverer{spec: staticRootSpec{key: "root", targets: []MetadataTarget{{URL: "meta/index.txt"}}}},
		nil,
		func(ctx context.Context, session *RefreshSession) (*LiveSnapshot, error) {
			blob, err := session.Fetch(ctx, MetadataTarget{URL: "meta/index.txt"})
			if err != nil {
				return nil, err
			}
			return &LiveSnapshot{
				Metadata: map[string]MetadataObject{
					blob.Path: {Path: blob.Path, Required: true},
				},
			}, nil
		},
		store, stats,
		health.New("repo", "test", health.DefaultConfig(), []string{server.URL}, stats, "test"),
	)
	require.NoError(t, handler.Start(ctx))
	defer func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, handler.Stop(stopCtx))
	}()

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
