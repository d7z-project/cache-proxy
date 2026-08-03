package rpm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func TestParsePrimaryBuildsCleanupPaths(t *testing.T) {
	input := `<metadata><package><location href="Packages/h/hello.rpm"/><checksum>abc</checksum></package></metadata>`
	paths := &filerepo.PathIndexBuilder{}
	count, err := parsePrimary(strings.NewReader(input), paths, "repo/os")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Contains(t, paths.Finalize(), "repo/os/Packages/h/hello.rpm")
}

func TestAnalyzerDetectsRPMRoot(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/repo/os/repodata/repomd.xml")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, "rpm:mirror/repo/os", result.Root.ID)
	require.Equal(t, filerepo.LayoutRPM, result.Root.Layout)
}

func TestAnalyzerClassifiesMirrorlistAsMetadataWithoutDiscovery(t *testing.T) {
	result := (inspector{}).InspectPath("mirror/repo/os/mirrorlist")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryIgnore, result.Role)
}

func TestAnalyzerSupportsRootLevelRPMRepository(t *testing.T) {
	result := (inspector{}).InspectPath("repodata/repomd.xml")
	require.Equal(t, filerepo.ResourceMetadata, result.Class)
	require.Equal(t, filerepo.DiscoveryCreateRoot, result.Role)
	require.Equal(t, "rpm:/", result.Root.ID)
	require.Empty(t, result.Root.Path)
	require.Equal(t, "/", result.Root.DisplayName)
}

func TestRPMRefreshRejectsMissingOrFailedRepomdObject(t *testing.T) {
	primary := []byte(`<metadata></metadata>`)
	filelists := []byte(`<filelists></filelists>`)
	repomd := rpmTestRepomd(map[string][]byte{"primary": primary, "filelists": filelists})

	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repo/repodata/repomd.xml":
					_, _ = io.WriteString(w, repomd)
				case "/repo/repodata/primary.xml":
					_, _ = w.Write(primary)
				case "/repo/repodata/filelists.xml":
					http.Error(w, http.StatusText(status), status)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			handler := newRPMMetadataTestHandler(t, server.URL, nil)
			err := handler.RefreshRoot(context.Background(), "rpm:repo")
			require.Error(t, err)
			statuses := handler.RepositoryStatuses()
			if len(statuses) > 0 {
				require.False(t, statuses[0].HasCurrent)
			}
		})
	}
}

func TestRPMRefreshCompanionAbsenceOnlyToleratesForbiddenAndNotFound(t *testing.T) {
	primary := []byte(`<metadata></metadata>`)
	repomd := rpmTestRepomd(map[string][]byte{"primary": primary})

	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/repo/repodata/repomd.xml":
					_, _ = io.WriteString(w, repomd)
				case r.URL.Path == "/repo/repodata/primary.xml":
					_, _ = w.Write(primary)
				case strings.HasPrefix(r.URL.Path, "/repo/repodata/repomd.xml."):
					http.Error(w, http.StatusText(status), status)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			handler := newRPMMetadataTestHandler(t, server.URL, nil)
			err := handler.RefreshRoot(context.Background(), "rpm:repo")
			if status == http.StatusForbidden || status == http.StatusNotFound {
				require.NoError(t, err)
				require.True(t, handler.RepositoryStatuses()[0].HasCurrent)
			} else {
				require.Error(t, err)
				statuses := handler.RepositoryStatuses()
				if len(statuses) > 0 {
					require.False(t, statuses[0].HasCurrent)
				}
			}
		})
	}
}

func TestRPMRefreshAdmissionCancellationDoesNotPublish(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "unused")
	}))
	defer server.Close()
	gate := httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), server.URL, httpcache.AdmissionRefresh)
	require.NoError(t, err)
	defer release()
	handler := newRPMMetadataTestHandler(t, server.URL, gate)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, handler.RefreshRoot(ctx, "rpm:repo"), context.DeadlineExceeded)
	statuses := handler.RepositoryStatuses()
	if len(statuses) > 0 {
		require.False(t, statuses[0].HasCurrent)
	}
}

func TestRPMSnapshotValidatorRejectsMissingRepomdLocation(t *testing.T) {
	primary := []byte(`<metadata></metadata>`)
	filelists := []byte(`<filelists></filelists>`)
	repomd := rpmTestRepomd(map[string][]byte{"primary": primary, "filelists": filelists})
	snapshot := &filerepo.LiveSnapshot{
		Targets: []filerepo.MetadataTarget{{URL: "repo/repodata/repomd.xml"}},
		Metadata: map[string]filerepo.MetadataObject{
			"repo/repodata/repomd.xml": {Path: "repo/repodata/repomd.xml", Required: true},
			"repo/repodata/primary.xml": {
				Path: "repo/repodata/primary.xml", Required: true, ChecksumType: "sha256", Checksum: rpmSHA256(primary),
			},
		}}
	opener := func(cleanPath string) (io.ReadCloser, error) {
		switch cleanPath {
		case "repo/repodata/repomd.xml":
			return io.NopCloser(strings.NewReader(repomd)), nil
		case "repo/repodata/primary.xml":
			return io.NopCloser(strings.NewReader(string(primary))), nil
		default:
			return nil, fmt.Errorf("missing %s", cleanPath)
		}
	}
	require.ErrorContains(t, (inspector{}).ValidateSnapshot(context.Background(), snapshot, opener), "filelists.xml")
}

func newRPMMetadataTestHandler(t *testing.T, upstream string, gate *httpcache.UpstreamGate) *filerepo.IndexedHandler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stats := httpcache.NewStats(prometheus.NewRegistry())
	handler := filerepo.NewIndexedHandler(
		"rpm-test", config.ModeRPM, config.ModeRPM, inspector{}, []string{upstream}, nil,
		config.Expiration(time.Hour), &filerepo.Policy{}, buildSnapshot, store, stats,
		health.New("rpm-test", config.ModeRPM, health.DefaultConfig(), []string{upstream}, stats, "cache-proxy-test"), gate,
	)
	handler.AddRepository(filerepo.RepositoryRoot{
		ID: "rpm:repo", Path: "repo", Layout: filerepo.LayoutRPM,
		PrimaryMetadata: []string{"repo/repodata/repomd.xml"},
		Targets:         []filerepo.MetadataTarget{{URL: "repo/repodata/repomd.xml"}},
	})
	return handler
}

func rpmTestRepomd(objects map[string][]byte) string {
	var result strings.Builder
	result.WriteString("<repomd>")
	for _, metadataType := range []string{"primary", "filelists"} {
		body, ok := objects[metadataType]
		if !ok {
			continue
		}
		_, _ = fmt.Fprintf(&result,
			`<data type="%s"><checksum type="sha256">%s</checksum><location href="repodata/%s.xml"/></data>`,
			metadataType, rpmSHA256(body), metadataType,
		)
	}
	result.WriteString("</repomd>")
	return result.String()
}

func rpmSHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
