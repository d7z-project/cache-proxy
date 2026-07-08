package apk

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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

func apkHeapAllocAfterGC() uint64 {
	for i := 0; i < 3; i++ {
		goruntime.GC()
		debug.FreeOSMemory()
	}
	var stats goruntime.MemStats
	goruntime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func TestAPKBusinessStressRefreshCacheCleanupMemoryRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		artifactCount = 1800
		rounds        = 6
		repoRoot      = "v3.20/main/x86_64"
		rootID        = "apk:v3.20/main/x86_64"
	)

	var generation atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := int(generation.Load())
		switch {
		case r.URL.Path == "/"+path.Join(repoRoot, "APKINDEX.tar.gz"):
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(apkIndexArchive(t, current, artifactCount))
		case strings.HasPrefix(r.URL.Path, "/"+repoRoot+"/") && strings.HasSuffix(r.URL.Path, ".apk"):
			w.Header().Set("Content-Type", "application/vnd.alpine.apk")
			_, _ = io.WriteString(w, "apk artifact "+r.URL.Path)
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
		"apk-business",
		config.ModeAPK,
		config.ModeAPK,
		inspector{},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		buildSnapshot,
		store,
		stats,
		health.New("apk-business", config.ModeAPK, health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRepository(filerepo.RepositoryRoot{
		ID:              rootID,
		Path:            repoRoot,
		DisplayName:     repoRoot,
		Layout:          filerepo.LayoutAPK,
		PrimaryMetadata: []string{path.Join(repoRoot, "APKINDEX.tar.gz")},
		Targets:         []filerepo.MetadataTarget{{URL: path.Join(repoRoot, "APKINDEX.tar.gz")}},
	})

	generation.Store(1)
	require.NoError(t, handler.RefreshRoot(ctx, rootID))
	cacheAPKArtifacts(t, handler, repoRoot, 1, 6)
	require.NoError(t, handler.CleanupRoot(ctx, rootID, config.DefaultCleanupConfig()))
	baseline := apkHeapAllocAfterGC()

	for round := 2; round <= rounds; round++ {
		generation.Store(int32(round))
		require.NoError(t, handler.RefreshRoot(ctx, rootID))
		cacheAPKArtifacts(t, handler, repoRoot, round, 6)
		require.NoError(t, handler.CleanupRoot(ctx, rootID, config.DefaultCleanupConfig()))
	}
	after := apkHeapAllocAfterGC()

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, artifactCount, statuses[0].ArtifactCount)
	t.Logf("apk business stress heap_alloc baseline=%d after=%d delta=%d", baseline, after, int64(after)-int64(baseline))
	if after > baseline {
		require.LessOrEqual(t, after-baseline, uint64(12<<20))
	}
}

func apkIndexArchive(t *testing.T, round, count int) []byte {
	t.Helper()
	var index strings.Builder
	for i := 0; i < count; i++ {
		_, _ = fmt.Fprintf(&index, "P:pkg-%05d\nV:%d.0-r0\n\n", i, round)
	}
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	data := []byte(index.String())
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: "APKINDEX", Mode: 0o644, Size: int64(len(data))}))
	_, err := tarWriter.Write(data)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	return output.Bytes()
}

func cacheAPKArtifacts(t *testing.T, handler http.Handler, repoRoot string, round, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		artifactPath := path.Join(repoRoot, fmt.Sprintf("pkg-%05d-%d.0-r0.apk", i, round))
		req := httptest.NewRequest(http.MethodGet, "/"+artifactPath, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}
}
