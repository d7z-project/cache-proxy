package pacman

import (
	"archive/tar"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path"
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

func TestParseDescExtractsFilename(t *testing.T) {
	filename, err := parseDesc(strings.NewReader("%FILENAME%\nhello-1.0-1-any.pkg.tar.zst\n"))
	require.NoError(t, err)
	require.Equal(t, "hello-1.0-1-any.pkg.tar.zst", filename)
}

func TestAnalyzerDetectsPacmanRoot(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/core/os/x86_64/core.db")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, "pacman:mirror/core/os/x86_64", result.Root.ID)
	require.Equal(t, "core", result.Root.Repo)
	require.Equal(t, filerepo.LayoutPacman, result.Root.Layout)
}

func TestAnalyzerClassifiesPacmanFilesAsMetadataWithoutDiscovery(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/core/os/x86_64/core.files")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}

func TestAnalyzerSupportsCompressedDatabaseAtRoot(t *testing.T) {
	result := (inspector{}).InspectPath("core.db.tar.zst")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "pacman:/", result.Root.ID)
	require.Equal(t, []string{"core.db.tar.zst"}, result.Root.PrimaryMetadata)
}

func TestAnalyzerClassifiesCompressedFilesDatabaseAsMetadata(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/core.db.tar.zst")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)

	files := (inspector{}).InspectPath("mirror/core.files.tar.zst")
	require.Equal(t, filerepo.ResourceMetadata, files.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, files.Role)
}

func TestParseDescWithoutFilenameReturnsEmpty(t *testing.T) {
	filename, err := parseDesc(strings.NewReader("%NAME%\nmultilib-staging\n"))
	require.NoError(t, err)
	require.Empty(t, filename)
}

func TestPacmanCurrentGenerationDoesNotFallbackForMissingMetadata(t *testing.T) {
	var database bytes.Buffer
	tarWriter := tar.NewWriter(&database)
	desc := []byte("%FILENAME%\nhello-1.0-1-any.pkg.tar.zst\n")
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: "hello-1.0-1/desc", Mode: 0o644, Size: int64(len(desc))}))
	_, err := tarWriter.Write(desc)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())

	const repoRoot = "mirror/core/os/x86_64"
	var upstreamRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		if r.URL.Path == "/"+path.Join(repoRoot, "core.db") {
			_, _ = w.Write(database.Bytes())
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"pacman-test", config.ModePacman, config.ModePacman, inspector{}, []string{server.URL}, nil,
		config.Expiration(time.Hour), &filerepo.Policy{}, buildSnapshot, store, stats,
		health.New("pacman-test", config.ModePacman, health.DefaultConfig(), []string{server.URL}, stats, "cache-proxy-test"), nil,
	)
	rootID := filerepo.RepositoryID(filerepo.LayoutPacman, repoRoot)
	handler.AddRepository(filerepo.RepositoryRoot{
		ID: rootID, Path: repoRoot, Layout: filerepo.LayoutPacman,
		PrimaryMetadata: []string{path.Join(repoRoot, "core.db")},
		Targets:         []filerepo.MetadataTarget{{URL: path.Join(repoRoot, "core.db"), Repo: "core"}},
	})
	require.NoError(t, handler.RefreshRoot(context.Background(), rootID))

	requestsBeforeMissingMetadata := upstreamRequests.Load()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+path.Join(repoRoot, "core.files"), nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, requestsBeforeMissingMetadata, upstreamRequests.Load())
}
