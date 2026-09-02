package app

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
)

func (a *App) checkOrphans(ctx context.Context) {
	root := filepath.Join(a.config.Server.Backend, "instances")
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		configuredSchema := ""
		if configured, ok := a.entries[name]; ok && configured.Enabled {
			configuredSchema = configured.Mode + "-v4"
		}
		if configuredSchema != "" {
			a.checkOrphanSchemas(ctx, root, name, configuredSchema)
			continue
		}
		instancePath := filepath.Join(root, name)
		slog.Warn("orphan instance storage found", "instance", name, "path", instancePath)
		if a.config.Storage.OrphanPolicy != "auto" {
			continue
		}
		if err := os.RemoveAll(instancePath); err != nil {
			slog.Warn("failed to clean orphan instance storage", "instance", name, "err", err)
		} else {
			slog.Info("cleaned orphan instance storage", "instance", name)
		}
	}
}

func (a *App) checkOrphanSchemas(ctx context.Context, root, instance, currentSchema string) {
	instancePath := filepath.Join(root, instance)
	entries, err := os.ReadDir(instancePath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if !entry.IsDir() || entry.Name() == currentSchema {
			continue
		}
		path := filepath.Join(instancePath, entry.Name())
		slog.Warn("orphan instance schema found", "instance", instance, "schema", entry.Name(), "path", path)
		if a.config.Storage.OrphanPolicy != "auto" {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			slog.Warn("failed to clean orphan instance schema", "instance", instance, "schema", entry.Name(), "err", err)
		} else {
			slog.Info("cleaned orphan instance schema", "instance", instance, "schema", entry.Name())
		}
	}
}
