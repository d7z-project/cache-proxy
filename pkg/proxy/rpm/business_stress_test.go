package rpm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func rpmHeapAllocAfterGC() uint64 {
	for i := 0; i < 3; i++ {
		goruntime.GC()
		debug.FreeOSMemory()
	}
	var stats goruntime.MemStats
	goruntime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func TestRPMBusinessStressRefreshCacheCleanupMemoryRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		artifactCount = 1800
		rounds        = 6
		repoRoot      = "repo/os"
		rootID        = "rpm:repo/os"
	)

	var generation atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := int(generation.Load())
		primary := rpmPrimaryXML(current, artifactCount)
		switch r.URL.Path {
		case "/" + path.Join(repoRoot, "repodata/repomd.xml"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, rpmRepomdXML(primary))
		case "/" + path.Join(repoRoot, "repodata/primary.xml"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, primary)
		default:
			if strings.HasPrefix(r.URL.Path, "/"+repoRoot+"/Packages/") && strings.HasSuffix(r.URL.Path, ".rpm") {
				w.Header().Set("Content-Type", "application/x-rpm")
				_, _ = io.WriteString(w, "rpm artifact "+r.URL.Path)
				return
			}
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"rpm-business",
		config.ModeRPM,
		config.ModeRPM,
		inspector{},
		[]string{server.URL},
		nil,
		config.Expiration(time.Hour),
		&filerepo.Policy{},
		buildSnapshot,
		store,
		stats,
		health.New("rpm-business", config.ModeRPM, health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"),
		nil,
	)
	handler.AddRepository(filerepo.RepositoryRoot{
		ID:              rootID,
		Path:            repoRoot,
		DisplayName:     repoRoot,
		Layout:          filerepo.LayoutRPM,
		PrimaryMetadata: []string{path.Join(repoRoot, "repodata/repomd.xml")},
		Targets:         []filerepo.MetadataTarget{{URL: path.Join(repoRoot, "repodata/repomd.xml")}},
	})

	generation.Store(1)
	require.NoError(t, handler.RefreshRoot(ctx, rootID))
	cacheRPMArtifacts(t, handler, repoRoot, 1, 6)
	require.NoError(t, handler.CleanupRoot(ctx, rootID, config.DefaultCleanupConfig()))
	baseline := rpmHeapAllocAfterGC()

	for round := 2; round <= rounds; round++ {
		generation.Store(int32(round))
		require.NoError(t, handler.RefreshRoot(ctx, rootID))
		cacheRPMArtifacts(t, handler, repoRoot, round, 6)
		require.NoError(t, handler.CleanupRoot(ctx, rootID, config.DefaultCleanupConfig()))
	}
	after := rpmHeapAllocAfterGC()

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, artifactCount, statuses[0].ArtifactCount)
	t.Logf("rpm business stress heap_alloc baseline=%d after=%d delta=%d", baseline, after, int64(after)-int64(baseline))
	if after > baseline {
		require.LessOrEqual(t, after-baseline, uint64(12<<20))
	}
}

func rpmPrimaryXML(round, count int) string {
	var builder strings.Builder
	builder.WriteString("<metadata>")
	for i := 0; i < count; i++ {
		_, _ = fmt.Fprintf(&builder, `<package><location href="%s"/><checksum>sum</checksum></package>`, rpmArtifactRelPath(round, i))
	}
	builder.WriteString("</metadata>")
	return builder.String()
}

func rpmRepomdXML(primary string) string {
	sum := sha256.Sum256([]byte(primary))
	return fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%s</checksum><location href="repodata/primary.xml"/></data></repomd>`, hex.EncodeToString(sum[:]))
}

func rpmArtifactRelPath(round, index int) string {
	return path.Join("Packages", fmt.Sprintf("pkg-%02d-%05d-1.x86_64.rpm", round, index))
}

func cacheRPMArtifacts(t *testing.T, handler http.Handler, repoRoot string, round, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		req := httptest.NewRequest(http.MethodGet, "/"+path.Join(repoRoot, rpmArtifactRelPath(round, i)), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}
}
