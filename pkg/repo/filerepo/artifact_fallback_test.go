package filerepo

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setArtifactGenerationOrigin(handler *IndexedHandler, upstream string) string {
	artifactPath := "root/pool/pkg.deb"
	handler.setRootSnapshot("root", &LiveSnapshot{
		RootID:     "root",
		RootPath:   "root",
		Generation: "gen1",
		Upstream:   upstream,
		Published:  time.Now(),
	})
	return artifactPath
}

func TestArtifactPreferredOriginIsStableDespitePassiveFailures(t *testing.T) {
	var preferredRequests atomic.Int64
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		preferredRequests.Add(1)
		_, _ = io.WriteString(w, "preferred")
	}))
	defer preferred.Close()
	var otherRequests atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		otherRequests.Add(1)
		_, _ = io.WriteString(w, "other")
	}))
	defer other.Close()

	handler := newTestHandler(t, newTestStore(t), []string{other.URL, preferred.URL}, nil)
	for range 10 {
		handler.sh.RecordResult(preferred.URL, http.StatusServiceUnavailable, time.Millisecond)
	}
	artifactPath := setArtifactGenerationOrigin(handler, preferred.URL)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/"+artifactPath, nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "preferred", rec.Body.String())
	require.EqualValues(t, 1, preferredRequests.Load())
	require.Zero(t, otherRequests.Load())
}

func TestArtifactNotFoundDoesNotTransferToAnotherMirror(t *testing.T) {
	var fallbackRequests atomic.Int64
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer preferred.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackRequests.Add(1)
		_, _ = io.WriteString(w, "fallback")
	}))
	defer fallback.Close()

	handler := newTestHandler(t, newTestStore(t), []string{preferred.URL, fallback.URL}, nil)
	artifactPath := setArtifactGenerationOrigin(handler, preferred.URL)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/"+artifactPath, nil))

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Zero(t, fallbackRequests.Load())
}

func TestArtifactGatewayFailureTransfersToNextConfiguredMirror(t *testing.T) {
	var fallbackRequests atomic.Int64
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer preferred.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackRequests.Add(1)
		_, _ = io.WriteString(w, "fallback")
	}))
	defer fallback.Close()

	handler := newTestHandler(t, newTestStore(t), []string{preferred.URL, fallback.URL}, nil)
	artifactPath := setArtifactGenerationOrigin(handler, preferred.URL)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/"+artifactPath, nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "fallback", rec.Body.String())
	require.EqualValues(t, 1, fallbackRequests.Load())
}
