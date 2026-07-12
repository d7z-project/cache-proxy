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

	"gopkg.d7z.net/cache-proxy/pkg/health"
)

func TestArtifactMirrorFallbackKeepsHealthyPreferredUpstreamAndCaches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var preferredRequests atomic.Int64
	var fallbackRequests atomic.Int64
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		preferredRequests.Add(1)
		_, _ = io.WriteString(w, "preferred")
	}))
	defer preferred.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackRequests.Add(1)
		_, _ = io.WriteString(w, "fallback")
	}))
	defer fallback.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{preferred.URL, fallback.URL}, nil)
	artifactPath := "root/pool/pkg.deb"
	handler.setRootSnapshot("root", &LiveSnapshot{
		RootID:     "root",
		RootPath:   "root",
		Generation: "gen1",
		Upstream:   preferred.URL,
		Published:  time.Now(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+artifactPath, nil)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "preferred", rec.Body.String())
	require.Equal(t, "MISS", rec.Header().Get("X-Cache"))
	require.Empty(t, rec.Header().Get("source-upstream"))
	require.Equal(t, int64(1), preferredRequests.Load())
	require.Equal(t, int64(0), fallbackRequests.Load())

	reader, err := store.OpenObject(
		ctx,
		"repo",
		handler.generationContentPath("root", "gen1", ResourceArtifact, artifactPath),
	)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "preferred", string(body))
}

func TestArtifactMirrorFallbackUsesHealthyUpstreamWithoutCachingWhenPreferredDegraded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var preferredRequests atomic.Int64
	var fallbackRequests atomic.Int64
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		preferredRequests.Add(1)
		_, _ = io.WriteString(w, "preferred")
	}))
	defer preferred.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackRequests.Add(1)
		_, _ = io.WriteString(w, "rescue")
	}))
	defer fallback.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{preferred.URL, fallback.URL}, nil)
	for i := 0; i < 8; i++ {
		handler.sh.RecordResult(preferred.URL, http.StatusOK, time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		handler.sh.RecordResult(preferred.URL, http.StatusInternalServerError, time.Millisecond)
	}
	state, ok := handler.sh.UpstreamState(preferred.URL)
	require.True(t, ok)
	require.Equal(t, health.SDegraded, state)

	artifactPath := "root/pool/pkg.deb"
	handler.setRootSnapshot("root", &LiveSnapshot{
		RootID:     "root",
		RootPath:   "root",
		Generation: "gen1",
		Upstream:   preferred.URL,
		Published:  time.Now(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+artifactPath, nil)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "rescue", rec.Body.String())
	require.Equal(t, "RESCUE", rec.Header().Get("X-Cache"))
	require.Equal(t, int64(0), preferredRequests.Load())
	require.Equal(t, int64(1), fallbackRequests.Load())
	_, err := store.StatObject(ctx, "repo", handler.generationContentPath("root", "gen1", ResourceArtifact, artifactPath))
	require.Error(t, err)
}

func TestArtifactMirrorFallbackRetriesNonPreferredNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var preferredRequests atomic.Int64
	var missingRequests atomic.Int64
	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		preferredRequests.Add(1)
		_, _ = io.WriteString(w, "preferred")
	}))
	defer preferred.Close()
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		missingRequests.Add(1)
		http.NotFound(w, r)
	}))
	defer missing.Close()

	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{preferred.URL, missing.URL}, nil)
	for i := 0; i < 8; i++ {
		handler.sh.RecordResult(preferred.URL, http.StatusOK, time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		handler.sh.RecordResult(preferred.URL, http.StatusInternalServerError, time.Millisecond)
	}
	artifactPath := "root/pool/pkg.deb"
	handler.setRootSnapshot("root", &LiveSnapshot{
		RootID:     "root",
		RootPath:   "root",
		Generation: "gen1",
		Upstream:   preferred.URL,
		Published:  time.Now(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+artifactPath, nil)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "preferred", rec.Body.String())
	require.Equal(t, int64(1), missingRequests.Load())
	require.Equal(t, int64(1), preferredRequests.Load())
}

func TestArtifactMirrorFallbackFlagOnlyAppliesToArtifactRoutes(t *testing.T) {
	store := newTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"}, nil)
	handler.setRootSnapshot("root", &LiveSnapshot{
		RootID:     "root",
		RootPath:   "root",
		Generation: "gen1",
		Upstream:   "https://upstream.example",
		Published:  time.Now(),
		Metadata: map[string]MetadataObject{
			"meta/index.txt": {Path: "meta/index.txt", StorePath: "repo/meta/index.txt", Required: true},
		},
	})
	resolver := &generationResolver{handler: handler, policy: &Policy{}}

	metadataReq := httptest.NewRequest(http.MethodGet, "/meta/index.txt", nil)
	metadataRoute, err := resolver.Resolve(metadataReq)
	require.NoError(t, err)
	require.False(t, metadataRoute.ArtifactMirrorFallback)

	artifactReq := httptest.NewRequest(http.MethodGet, "/root/pool/pkg.deb", nil)
	artifactRoute, err := resolver.Resolve(artifactReq)
	require.NoError(t, err)
	require.True(t, artifactRoute.ArtifactMirrorFallback)
}
