package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		root: root, counts: make(map[string]int), headers: make(map[string]http.Header), faults: make(map[string]int),
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
