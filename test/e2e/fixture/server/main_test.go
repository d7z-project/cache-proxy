package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFixtureHeaderDelayCancellationAndRemoval(t *testing.T) {
	s := &fixtureServer{root: t.TempDir(), counts: make(map[string]int), headers: make(map[string]http.Header)}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/__e2e/fault?path=/rpm/repodata/repomd.xml&status=delay", nil))
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, 3*time.Second, s.faults["/rpm/repodata/repomd.xml"].headerDelay)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/rpm/repodata/repomd.xml", nil).WithContext(ctx))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delay ignored request cancellation")
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/__e2e/fault?path=/rpm/repodata/repomd.xml", nil))
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Empty(t, s.faults)
}

func TestFixtureUpdateChangesValidatorsAtomically(t *testing.T) {
	root := t.TempDir()
	for state, body := range map[string]string{"initial": "payload-initial", "updated": "payload-updated"} {
		directory := filepath.Join(root, state, "file")
		require.NoError(t, os.MkdirAll(directory, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(directory, "payload.txt"), []byte(body), 0o644))
	}
	server := &fixtureServer{
		root:      root,
		publicURL: "http://fixture.test",
		counts:    make(map[string]int),
		headers:   make(map[string]http.Header),
	}
	first := httptest.NewRecorder()
	server.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/file/payload.txt", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "payload-initial", first.Body.String())
	initialETag := first.Header().Get("ETag")
	require.NotEmpty(t, initialETag)

	switchResponse := httptest.NewRecorder()
	server.ServeHTTP(switchResponse, httptest.NewRequest(http.MethodPost, "/__e2e/state?value=updated", nil))
	require.Equal(t, http.StatusNoContent, switchResponse.Code)

	updatedRequest := httptest.NewRequest(http.MethodGet, "/file/payload.txt", nil)
	updatedRequest.Header.Set("If-None-Match", initialETag)
	updated := httptest.NewRecorder()
	server.ServeHTTP(updated, updatedRequest)
	require.Equal(t, http.StatusOK, updated.Code)
	require.Equal(t, "payload-updated", updated.Body.String())
	require.NotEqual(t, initialETag, updated.Header().Get("ETag"))

	currentRequest := httptest.NewRequest(http.MethodGet, "/file/payload.txt", nil)
	currentRequest.Header.Set("If-None-Match", updated.Header().Get("ETag"))
	current := httptest.NewRecorder()
	server.ServeHTTP(current, currentRequest)
	require.Equal(t, http.StatusNotModified, current.Code)
}

func TestTransparentPathProbeEchoesExactRequestURI(t *testing.T) {
	server := &fixtureServer{
		root: t.TempDir(), counts: make(map[string]int), headers: make(map[string]http.Header),
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/apk/__e2e_path__/asset.css?theme=dark", nil))

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "/apk/__e2e_path__/asset.css?theme=dark", response.Body.String())
	require.Equal(t, 1, server.counts["GET /apk/__e2e_path__/asset.css"])
}

func TestFixtureFaultTargetsOneExactPathAndResetClearsIt(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "initial", "deb", "dists", "stable")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "Release"), []byte("release"), 0o644))
	server := &fixtureServer{
		root: root, counts: make(map[string]int), headers: make(map[string]http.Header), faults: make(map[string]fixtureFault),
	}

	fault := httptest.NewRecorder()
	server.ServeHTTP(fault, httptest.NewRequest(http.MethodPost, "/__e2e/fault?path=%2Fdeb%2Fdists%2Fstable%2FRelease&status=404", nil))
	require.Equal(t, http.StatusNoContent, fault.Code)

	unavailable := httptest.NewRecorder()
	server.ServeHTTP(unavailable, httptest.NewRequest(http.MethodGet, "/deb/dists/stable/Release", nil))
	require.Equal(t, http.StatusNotFound, unavailable.Code)

	reset := httptest.NewRecorder()
	server.ServeHTTP(reset, httptest.NewRequest(http.MethodPost, "/__e2e/reset", nil))
	require.Equal(t, http.StatusNoContent, reset.Code)

	available := httptest.NewRecorder()
	server.ServeHTTP(available, httptest.NewRequest(http.MethodGet, "/deb/dists/stable/Release", nil))
	require.Equal(t, http.StatusOK, available.Code)
	require.Equal(t, "release", available.Body.String())
}

func TestFixtureCacheAgeControlChangesHeadersAndResetRestoresDefault(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "initial", "file")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "payload.txt"), []byte("payload"), 0o644))
	server := &fixtureServer{
		root: root, counts: make(map[string]int), headers: make(map[string]http.Header), faults: make(map[string]fixtureFault),
	}

	setAge := httptest.NewRecorder()
	server.ServeHTTP(setAge, httptest.NewRequest(http.MethodPost, "/__e2e/cache-age?path=%2Ffile%2Fpayload.txt&seconds=1", nil))
	require.Equal(t, http.StatusNoContent, setAge.Code)

	short := httptest.NewRecorder()
	server.ServeHTTP(short, httptest.NewRequest(http.MethodGet, "/file/payload.txt", nil))
	require.Equal(t, http.StatusOK, short.Code)
	require.Equal(t, "public, max-age=1", short.Header().Get("Cache-Control"))

	require.NoError(t, os.WriteFile(filepath.Join(directory, "artifact.deb"), []byte("artifact"), 0o644))
	unrelated := httptest.NewRecorder()
	server.ServeHTTP(unrelated, httptest.NewRequest(http.MethodGet, "/file/artifact.deb", nil))
	require.Equal(t, http.StatusOK, unrelated.Code)
	require.Equal(t, "public, max-age=60", unrelated.Header().Get("Cache-Control"))

	invalidPath := httptest.NewRecorder()
	server.ServeHTTP(invalidPath, httptest.NewRequest(http.MethodPost, "/__e2e/cache-age?path=relative&seconds=1", nil))
	require.Equal(t, http.StatusBadRequest, invalidPath.Code)
	invalidControlPath := httptest.NewRecorder()
	server.ServeHTTP(invalidControlPath, httptest.NewRequest(http.MethodPost, "/__e2e/cache-age?path=%2F__e2e%2Fready&seconds=1", nil))
	require.Equal(t, http.StatusBadRequest, invalidControlPath.Code)

	reset := httptest.NewRecorder()
	server.ServeHTTP(reset, httptest.NewRequest(http.MethodPost, "/__e2e/reset", nil))
	require.Equal(t, http.StatusNoContent, reset.Code)

	defaultAge := httptest.NewRecorder()
	server.ServeHTTP(defaultAge, httptest.NewRequest(http.MethodGet, "/file/payload.txt", nil))
	require.Equal(t, http.StatusOK, defaultAge.Code)
	require.Equal(t, "public, max-age=60", defaultAge.Header().Get("Cache-Control"))
}

func TestFixtureCacheAgeAcceptsOnlyBoundedIntegerSeconds(t *testing.T) {
	for _, seconds := range []string{"0", "86400", "", "-1", "86401", "1s", "1.5"} {
		t.Run(seconds, func(t *testing.T) {
			server := &fixtureServer{}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/__e2e/cache-age?path=/repo/Release&seconds="+seconds, nil))
			if seconds == "0" || seconds == "86400" {
				require.Equal(t, http.StatusNoContent, response.Code)
				require.Len(t, server.cacheAges, 1)
			} else {
				require.Equal(t, http.StatusBadRequest, response.Code)
				require.Empty(t, server.cacheAges)
			}
		})
	}
}
