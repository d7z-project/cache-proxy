package deb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	goruntime "runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func debHeapAllocAfterGC() uint64 {
	for i := 0; i < 3; i++ {
		goruntime.GC()
		debug.FreeOSMemory()
	}
	var stats goruntime.MemStats
	goruntime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func TestDEBBusinessStressRefreshCacheCleanupMemoryRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		artifactCount = 1800
		rounds        = 6
	)

	var generation atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := int(generation.Load())
		switch {
		case r.URL.Path == "/Packages":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, debPackagesIndex(current, artifactCount))
		case strings.HasPrefix(r.URL.Path, "/pool/"):
			w.Header().Set("Content-Type", "application/vnd.debian.binary-package")
			_, _ = io.WriteString(w, "deb artifact "+r.URL.Path)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"deb-business",
		config.ModeDEB,
		config.ModeDEB,
		inspector{},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		buildSnapshot,
		store,
		stats,
		health.New("deb-business", config.ModeDEB, health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRepository(filerepo.RepositoryRoot{
		ID:              filerepo.RepositoryID(filerepo.LayoutDebFlat, ""),
		DisplayName:     "/",
		Layout:          filerepo.LayoutDebFlat,
		PrimaryMetadata: []string{"Packages"},
		Targets:         []filerepo.MetadataTarget{{URL: "Packages", Kind: "packages"}},
	})

	generation.Store(1)
	require.NoError(t, handler.RefreshRoot(ctx, filerepo.RepositoryID(filerepo.LayoutDebFlat, "")))
	cacheDEBArtifacts(t, handler, 1, 6)
	require.NoError(t, handler.CleanupRoot(ctx, filerepo.RepositoryID(filerepo.LayoutDebFlat, ""), config.DefaultCleanupConfig()))
	baseline := debHeapAllocAfterGC()

	for round := 2; round <= rounds; round++ {
		generation.Store(int32(round))
		require.NoError(t, handler.RefreshRoot(ctx, filerepo.RepositoryID(filerepo.LayoutDebFlat, "")))
		cacheDEBArtifacts(t, handler, round, 6)
		require.NoError(t, handler.CleanupRoot(ctx, filerepo.RepositoryID(filerepo.LayoutDebFlat, ""), config.DefaultCleanupConfig()))
	}
	after := debHeapAllocAfterGC()

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, artifactCount, statuses[0].ArtifactCount)
	t.Logf("deb business stress heap_alloc baseline=%d after=%d delta=%d", baseline, after, int64(after)-int64(baseline))
	if after > baseline {
		require.LessOrEqual(t, after-baseline, uint64(12<<20))
	}
}

func debPackagesIndex(round, count int) string {
	var builder strings.Builder
	for i := 0; i < count; i++ {
		_, _ = fmt.Fprintf(&builder, "Package: pkg-%05d\nVersion: %d.0\nFilename: %s\n\n", i, round, debArtifactPath(round, i))
	}
	return builder.String()
}

func debArtifactPath(round, index int) string {
	return path.Join("pool/main", fmt.Sprintf("pkg-%02d-%05d_1.0_amd64.deb", round, index))
}

func cacheDEBArtifacts(t *testing.T, handler http.Handler, round, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		req := httptest.NewRequest(http.MethodGet, "/"+debArtifactPath(round, i), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}
}
