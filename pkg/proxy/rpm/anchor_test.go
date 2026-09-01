package rpm

import (
	"context"
	"crypto/sha256"
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
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestAnchoredHandlerServesDeclaredRepomdObject(t *testing.T) {
	primary := []byte("primary metadata")
	digest := sha256.Sum256(primary)
	repomd := fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha256">%x</checksum><location href="repodata/primary.xml"/></data></repomd>`, digest)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/repo/repodata/repomd.xml":
			_, _ = io.WriteString(w, repomd)
		case "/repo/repodata/primary.xml":
			_, _ = w.Write(primary)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	handler := newAnchoredTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/repo/repodata/primary.xml", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, primary, recorder.Body.Bytes())
}

func TestAnchoredHandlerReturnsNotFoundForUndeclaredRepodata(t *testing.T) {
	requested := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/repo/repodata/repomd.xml" {
			_, _ = io.WriteString(w, `<repomd/>`)
			return
		}
		requested++
		_, _ = io.WriteString(w, "undeclared")
	}))
	defer server.Close()

	handler := newAnchoredTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/repo/repodata/filelists.xml", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Zero(t, requested)
}

func TestVerifyRepomdChecksumRejectsSizeMismatch(t *testing.T) {
	body := []byte("primary metadata")
	digest := sha256.Sum256(body)
	err := verifyRepomdChecksumReader("repodata/primary.xml", "sha256", fmt.Sprintf("%x", digest), int64(len(body)+1), strings.NewReader(string(body)))
	require.ErrorContains(t, err, "size mismatch")
}

func TestBuildRepomdItemsRejectsConflictingLocation(t *testing.T) {
	_, err := buildRepomdItems("repo", []repomdItem{
		{Location: "repodata/primary.xml", Checksum: strings.Repeat("1", 64), SumType: "sha256", Size: 1},
		{Location: "repodata/primary.xml", Checksum: strings.Repeat("2", 64), SumType: "sha256", Size: 1},
	})
	require.ErrorContains(t, err, "conflicting repomd entries")
}

func newAnchoredTestHandler(t *testing.T, upstream string) *anchoredHandler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	base := httpcache.NewHandler("rpm-test", httpcache.RuntimeConfig{
		Mode: config.ModeRPM, ExpireAfter: config.Expiration(time.Hour), MetadataTTL: time.Minute,
		Upstreams: []string{upstream}, VerifyFunc: verifyAnchoredObject,
	}, store, anchoredResolver{}, httpcache.NewStats(prometheus.NewRegistry()))
	t.Cleanup(func() {
		require.NoError(t, base.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return &anchoredHandler{base: base}
}
