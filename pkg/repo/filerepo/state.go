package filerepo

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"strings"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/health"
)

const stateFileName = "_state.yaml"

type persistedState struct {
	Version int                       `yaml:"version"`
	Roots   []health.ResourceSnapshot `yaml:"roots"`
}

func (h *IndexedHandler) statePath() string {
	return path.Join(h.objectRoot, stateFileName)
}

func (h *IndexedHandler) saveState(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	state := persistedState{Version: 2}
	if h.sh != nil {
		state.Roots = h.sh.SnapshotResources()
	}
	data, err := yaml.Marshal(state)
	if err != nil {
		slog.Warn("indexed state marshal failed", "instance", h.name, "err", err)
		return
	}
	if err := h.store.MkdirAll(path.Join(h.name, h.objectRoot), 0o755); err != nil {
		slog.Warn("indexed state mkdir failed", "instance", h.name, "err", err)
		return
	}
	if _, err := h.store.Put(ctx, h.name, h.statePath(), bytes.NewReader(data), map[string]string{}); err != nil {
		slog.Warn("indexed state write failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) loadState(ctx context.Context) persistedState {
	reader, err := h.store.OpenObject(ctx, h.name, h.statePath())
	if err != nil {
		return persistedState{Version: 2}
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return persistedState{Version: 2}
	}
	var state persistedState
	if err := yaml.Unmarshal(data, &state); err != nil {
		slog.Warn("indexed state unmarshal failed", "instance", h.name, "err", err)
		return persistedState{Version: 2}
	}
	return state
}

func (h *IndexedHandler) restoreRoots(ctx context.Context) {
	persisted := h.loadState(ctx)
	for _, snap := range persisted.Roots {
		if snap.Path != "" {
			h.addRoot(snap.Path, nil)
		}
	}
}

func (h *IndexedHandler) restoreGenerations(ctx context.Context) {
	rootDir := path.Join(h.objectRoot, ".roots")
	err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || path.Base(objectPath) != "current.yaml" {
			return nil
		}
		reader, err := h.store.OpenObject(ctx, h.name, objectPath)
		if err != nil {
			return nil
		}
		defer reader.Close()
		data, err := io.ReadAll(reader)
		if err != nil {
			return nil
		}
		var snapshot LiveSnapshot
		if err := yaml.Unmarshal(data, &snapshot); err != nil || snapshot.RootKey == "" {
			return nil
		}
		for key, obj := range snapshot.Metadata {
			if obj.StorePath == "" {
				obj.StorePath = h.generationMetadataPath(snapshot.RootKey, snapshot.Generation, obj.Path)
				snapshot.Metadata[key] = obj
			}
		}
		h.mu.Lock()
		h.rootSnapshots[snapshot.RootKey] = &snapshot
		h.rebuildAggregateLocked()
		h.mu.Unlock()
		return nil
	})
	if err != nil && !strings.Contains(err.Error(), "not exist") {
		slog.Warn("indexed generation restore failed", "instance", h.name, "err", err)
	}
}
