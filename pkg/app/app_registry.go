package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

const registryTenant = "_proxy_instances"

type registryManifest struct {
	Instances []string `json:"instances"`
	UpdatedAt string   `json:"updated_at"`
}

func saveRegistry(ctx context.Context, store *blobfs.Store, doc *config.Document) {
	var names []string
	for _, inst := range doc.Instances {
		names = append(names, inst.Name)
	}
	manifest := registryManifest{
		Instances: names,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.MkdirAll(registryTenant+"/", 0o755)
	data, _ := json.Marshal(manifest)
	_, err := store.Put(ctx, registryTenant, "manifest.json", strings.NewReader(string(data)), nil)
	if err != nil {
		slog.Warn("failed to save registry manifest", "err", err)
	}
}

func (a *App) checkOrphans(ctx context.Context) {
	dir, err := a.store.Open("/")
	if err != nil {
		return
	}
	defer func() { _ = dir.Close() }() // read-only dir, close error is harmless
	entries, err := dir.Readdir(-1)
	if err != nil {
		return
	}

	expected := make(map[string]struct{}, len(a.entries))
	for name := range a.entries {
		expected[name] = struct{}{}
	}
	expected["_proxy_instances"] = struct{}{}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if _, ok := expected[name]; ok {
			continue
		}
		usage := collectTenantUsage(ctx, []string{name}, a.store)
		slog.Warn("orphan tenant found", "tenant", name, "size_bytes", usage[name])

		if a.config.Storage.OrphanPolicy == "auto" {
			if err := a.store.DeleteTenant(ctx, name); err != nil {
				slog.Warn("failed to clean orphan tenant", "tenant", name, "err", err)
			} else {
				slog.Info("cleaned orphan tenant", "tenant", name)
			}
		}
	}

	saveRegistry(ctx, a.store, a.config)
}
