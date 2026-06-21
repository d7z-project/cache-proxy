package oci

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestParseOCIRef(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "repo only", input: "library/alpine", want: "v2/library/alpine/tags/list"},
		{name: "repo with tag", input: "library/alpine:latest", want: "v2/library/alpine/manifests/latest"},
		{name: "simple repo", input: "nginx", want: "v2/nginx/tags/list"},
		{name: "simple repo with tag", input: "nginx:1.25", want: "v2/nginx/manifests/1.25"},
		{name: "nested repo", input: "org/project/image:v1", want: "v2/org/project/image/manifests/v1"},
		{name: "empty input", input: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOCIRef(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveRequestMatchesRepoPolicy(t *testing.T) {
	cfg := &Policy{
		DefaultPolicy: config.PolicyRevalidate,
		BusyPolicy:    config.BusyPolicyStale,
		Rules: []Rule{
			{Match: "library/*", Policy: config.PolicyImmutable, ExpireAfter: config.Expiration(2 * time.Hour)},
			{Match: "org/**", Policy: config.PolicyBypass},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	resolved, err := resolveRequest(req, cfg)
	require.NoError(t, err)
	require.Equal(t, requestManifest, resolved.kind)
	require.Equal(t, "library/alpine", resolved.repo)
	require.Equal(t, "latest", resolved.ref)
	require.Equal(t, config.PolicyImmutable, resolved.match.policy)
	require.Equal(t, config.BusyPolicyStale, resolved.match.busyPolicy)
	require.Equal(t, config.Expiration(2*time.Hour), resolved.match.expireAfter)
}

func TestOCIBearerTokenSingleflight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tokenRequests int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenRequests, 1)
		require.Equal(t, "registry.example", r.URL.Query().Get("service"))
		require.Equal(t, "repository:library/alpine:pull", r.URL.Query().Get("scope"))
		require.Equal(t, "Basic dXNlcjpwYXNz", r.Header.Get("Authorization"))
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, `{"token":"token-`+strconv.Itoa(int(atomic.LoadInt32(&tokenRequests)))+`","expires_in":60}`)
	}))
	defer tokenServer.Close()

	handler := newHandler("oci", Block{
		Upstream: "https://registry.example",
		Policy: Policy{
			Auth: &AuthConfig{Type: "basic", Username: "user", Password: "pass"},
		},
	}, config.DefaultExpireAfter, nil, httpcache.NewStats(prometheus.NewRegistry()))
	challenge := ociChallenge{
		scheme: "Bearer",
		realm:  tokenServer.URL + "/token",
		params: map[string]string{"service": "registry.example", "scope": "repository:library/alpine:pull"},
	}

	const workers = 16
	var wg sync.WaitGroup
	results := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := handler.ociBearerToken(ctx, challenge)
			require.NoError(t, err)
			results <- token
		}()
	}
	wg.Wait()
	close(results)
	for token := range results {
		require.Equal(t, "token-1", token)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&tokenRequests))
}

func TestOCIManifestKeepsBlobUntilRefExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manifestBody := `{"schemaVersion":2,"config":{"digest":"sha256:cfg"},"layers":[{"digest":"sha256:layer"}]}`
	blobBody := "blob-data"
	var manifestRequests atomic.Int64
	var blobRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			manifestRequests.Add(1)
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:manifest")
			_, _ = io.WriteString(w, manifestBody)
		case "/v2/library/alpine/blobs/sha256:layer":
			blobRequests.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, blobBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
		Policy: Policy{
			DefaultPolicy: config.PolicyImmutable,
			BusyPolicy:    config.BusyPolicyStale,
		},
	}, config.Expiration(time.Hour), store, httpcache.NewStats(prometheus.NewRegistry()))

	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, manifestBody, rec.Body.String())

	req = httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/sha256:layer", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, blobBody, rec.Body.String())

	require.Equal(t, int64(1), manifestRequests.Load())
	require.Equal(t, int64(1), blobRequests.Load())

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/sha256:layer", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "HIT", rec.Header().Get("X-Cache"))
	require.Equal(t, int64(1), blobRequests.Load())

	statePath := handler.refStatePath("library/alpine", "latest")
	state, err := handler.readState(ctx, statePath)
	require.NoError(t, err)
	state.FetchedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, handler.writeState(ctx, state))

	require.NoError(t, handler.Cleanup(ctx))

	_, err = store.OpenObject(ctx, handler.name, handler.refBlobPath("library/alpine", "latest", "sha256:layer"))
	require.Error(t, err)
}

func TestOCIBypassesBlobWithoutActiveRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var blobRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blobRequests.Add(1)
		_, _ = io.WriteString(w, "blob-data")
	}))
	defer upstream.Close()

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	defer store.Close()

	handler := newHandler("oci", Block{
		Upstream: upstream.URL,
		Policy: Policy{
			DefaultPolicy: config.PolicyImmutable,
		},
	}, config.Expiration(time.Hour), store, httpcache.NewStats(prometheus.NewRegistry()))

	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/sha256:layer", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
	require.Equal(t, int64(1), blobRequests.Load())

	_, err = store.OpenObject(ctx, handler.name, handler.refBlobPath("library/alpine", "latest", "sha256:layer"))
	require.Error(t, err)
	require.False(t, strings.Contains(rec.Body.String(), "Bad Gateway"))
}
