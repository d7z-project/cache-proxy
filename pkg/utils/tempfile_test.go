package utils

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTempFileFromReader(t *testing.T) {
	body := strings.Repeat("data", 1024)
	file, size, err := TempFileFromReader(strings.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), size)

	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, body, string(data))

	pos, err := file.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), pos)
}

func TestTempFileFromReaderRewind(t *testing.T) {
	body := "hello world"
	file, size, err := TempFileFromReader(strings.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), size)
	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	first, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, body, string(first))

	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	second, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, body, string(second))
}

func TestCleanStaleTempFiles(t *testing.T) {
	fakeOldFile, err := os.CreateTemp("", "cache-proxy-old")
	require.NoError(t, err)
	fakeOldFile.Close()
	t.Cleanup(func() { os.Remove(fakeOldFile.Name()) })

	oldTime := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(fakeOldFile.Name(), oldTime, oldTime))

	fakeNewFile, err := os.CreateTemp("", "cache-proxy-new")
	require.NoError(t, err)
	fakeNewFile.Close()
	t.Cleanup(func() { os.Remove(fakeNewFile.Name()) })

	CleanStaleTempFiles(24 * time.Hour)

	_, err = os.Stat(fakeOldFile.Name())
	require.True(t, os.IsNotExist(err))

	_, err = os.Stat(fakeNewFile.Name())
	require.NoError(t, err)

	entries, err := os.ReadDir(os.TempDir())
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.Name() == filepath.Base(fakeNewFile.Name()) {
			return
		}
	}
	t.Fatal("new temp file was accidentally cleaned")
}
