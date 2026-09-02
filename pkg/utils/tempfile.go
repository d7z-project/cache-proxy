package utils

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CleanStaleWorkFiles removes abandoned downloads from per-instance work
// directories without touching protocol state or arbitrary operator files.
func CleanStaleWorkFiles(instancesRoot string, maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	_ = filepath.WalkDir(instancesRoot, func(name string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if entry.IsDir() || filepath.Base(filepath.Dir(name)) != "work" {
			return nil
		}
		if !strings.HasPrefix(entry.Name(), ".cache-proxy-tmp-") {
			return nil
		}
		info, err := entry.Info()
		if err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(name)
		}
		return nil
	})
}
