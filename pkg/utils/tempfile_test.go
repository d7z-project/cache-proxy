package utils

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCleanStaleTempFiles(t *testing.T) {
	fakeOldFile, err := os.CreateTemp("", "cache-proxy-old")
	require.NoError(t, err)
	require.NoError(t, fakeOldFile.Close())
	t.Cleanup(func() { _ = os.Remove(fakeOldFile.Name()) })

	oldTime := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(fakeOldFile.Name(), oldTime, oldTime))

	fakeNewFile, err := os.CreateTemp("", "cache-proxy-new")
	require.NoError(t, err)
	require.NoError(t, fakeNewFile.Close())
	t.Cleanup(func() { _ = os.Remove(fakeNewFile.Name()) })

	CleanStaleTempFiles(24 * time.Hour)

	_, err = os.Stat(fakeOldFile.Name())
	require.True(t, os.IsNotExist(err))

	_, err = os.Stat(fakeNewFile.Name())
	require.NoError(t, err)
}
