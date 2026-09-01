package deb

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestAnchoredHandlerServesDeclaredMetadata(t *testing.T) {
	metadata := []byte("package index")
	digest := sha256.Sum256(metadata)
	release := fmt.Sprintf("SHA256:\n %x %d main/binary-amd64/Packages.xz\n", digest, len(metadata))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case "/dists/trixie/main/binary-amd64/Packages.xz":
			_, _ = w.Write(metadata)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	handler := newAnchoredTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/binary-amd64/Packages.xz", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, metadata, recorder.Body.Bytes())
}

func TestAnchoredHandlerUsesByHashForCanonicalMetadata(t *testing.T) {
	metadata := []byte("anchored package index")
	digest := sha256.Sum256(metadata)
	canonicalPath := "/dists/trixie/main/binary-amd64/Packages.xz"
	byHashPath := fmt.Sprintf("/dists/trixie/main/binary-amd64/by-hash/SHA256/%x", digest)
	release := fmt.Sprintf(
		"Acquire-By-Hash: yes\nSHA256:\n %x %d main/binary-amd64/Packages.xz\n",
		digest,
		len(metadata),
	)
	canonicalRequests := 0
	byHashRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/dists/trixie/InRelease":
			_, _ = io.WriteString(w, release)
		case canonicalPath:
			canonicalRequests++
			_, _ = io.WriteString(w, "metadata from a newer Release")
		case byHashPath:
			byHashRequests++
			_, _ = w.Write(metadata)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	handler := newAnchoredTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, canonicalPath, nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, metadata, recorder.Body.Bytes())
	require.Zero(t, canonicalRequests)
	require.Equal(t, 1, byHashRequests)
}

func TestAnchoredHandlerReturnsNotFoundForUndeclaredMetadata(t *testing.T) {
	requested := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/dists/trixie/InRelease" {
			_, _ = io.WriteString(w, "SHA256:\n")
			return
		}
		requested++
		_, _ = io.WriteString(w, "unsigned")
	}))
	defer server.Close()

	handler := newAnchoredTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Zero(t, requested)
}

func TestAnchoredHandlerRefreshesAnchorOnceForNewMetadata(t *testing.T) {
	metadata := []byte("new metadata")
	digest := sha256.Sum256(metadata)
	anchorRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/dists/trixie/InRelease":
			anchorRequests++
			if anchorRequests == 1 {
				_, _ = io.WriteString(w, "SHA256:\n")
				return
			}
			_, _ = fmt.Fprintf(w, "SHA256:\n %x %d main/Contents-all\n", digest, len(metadata))
		case "/dists/trixie/main/Contents-all":
			_, _ = w.Write(metadata)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	handler := newAnchoredTestHandler(t, server.URL)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dists/trixie/main/Contents-all", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, metadata, recorder.Body.Bytes())
	require.Equal(t, 2, anchorRequests)
}

func newAnchoredTestHandler(t *testing.T, upstream string) *anchoredHandler {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	stats := httpcache.NewStats(prometheus.NewRegistry())
	base := httpcache.NewHandler("deb-test", httpcache.RuntimeConfig{
		Mode: config.ModeDEB, ExpireAfter: config.Expiration(time.Hour), MetadataTTL: time.Minute,
		Upstreams: []string{upstream}, VerifyFunc: verifyAnchoredObject,
	}, store, anchoredResolver{}, stats)
	t.Cleanup(func() {
		require.NoError(t, base.CloseContext(context.Background()))
		require.NoError(t, store.Close())
	})
	return &anchoredHandler{base: base}
}
