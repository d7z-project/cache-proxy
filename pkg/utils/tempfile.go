package utils

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func TempFileFromReader(src io.Reader) (*os.File, int64, error) {
	tmp, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		return nil, 0, err
	}
	written, err := io.Copy(tmp, src)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, 0, err
	}
	return tmp, written, nil
}

func CleanStaleTempFiles(maxAge time.Duration) {
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-maxAge)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "cache-proxy-") {
			continue
		}
		info, err := entry.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		os.Remove(filepath.Join(os.TempDir(), entry.Name()))
	}
}
