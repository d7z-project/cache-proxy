package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestCheckOrphansKeepsCurrentAndCleansOrphanedStorage(t *testing.T) {
	backend := t.TempDir()
	root := filepath.Join(backend, "instances")
	for _, path := range []string{
		filepath.Join(root, "active", "npm"),
		filepath.Join(root, "active", "file"),
		filepath.Join(root, "disabled", "file"),
		filepath.Join(root, "orphan", "deb"),
	} {
		require.NoError(t, os.MkdirAll(path, 0o755))
	}
	app := &App{
		config: &config.Document{
			Server:  config.ServerConfig{Backend: backend},
			Storage: config.StorageConfig{OrphanPolicy: "auto"},
		},
		entries: map[string]*proxyruntime.Entry{
			"active":   {Name: "active", Mode: config.ModeNPM, Enabled: true},
			"disabled": {Name: "disabled", Mode: config.ModeFile, Enabled: false},
		},
	}

	app.checkOrphans(context.Background())

	require.DirExists(t, filepath.Join(root, "active", "npm"))
	require.NoDirExists(t, filepath.Join(root, "active", "file"))
	require.NoDirExists(t, filepath.Join(root, "disabled"))
	require.NoDirExists(t, filepath.Join(root, "orphan"))
}
