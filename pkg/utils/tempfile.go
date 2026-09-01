package utils

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
		_ = os.Remove(filepath.Join(os.TempDir(), entry.Name()))
	}
}
