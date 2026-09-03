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
