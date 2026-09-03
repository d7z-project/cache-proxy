package utils

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCleanStaleWorkFilesOnlyRemovesOwnedFiles(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "demo", "file", "work")
	require.NoError(t, os.MkdirAll(work, 0o755))
	old := filepath.Join(work, ".cache-proxy-tmp-stream-old")
	foreign := filepath.Join(work, "operator.data")
	state := filepath.Join(root, "demo", "file", "state", ".cache-proxy-tmp-stream-state")
	require.NoError(t, os.MkdirAll(filepath.Dir(state), 0o755))
	for _, name := range []string{old, foreign, state} {
		require.NoError(t, os.WriteFile(name, []byte("x"), 0o600))
		require.NoError(t, os.Chtimes(name, time.Now().Add(-48*time.Hour), time.Now().Add(-48*time.Hour)))
	}

	CleanStaleWorkFiles(root, 24*time.Hour)

	_, err := os.Stat(old)
	require.ErrorIs(t, err, os.ErrNotExist)
	for _, name := range []string{foreign, state} {
		_, err := os.Stat(name)
		require.NoError(t, err)
	}
}
