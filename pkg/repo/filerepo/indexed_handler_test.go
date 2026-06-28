package filerepo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
			Auxiliary: map[string]RepoObject{},
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
			Auxiliary: map[string]RepoObject{},
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
