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
	state := persistedState{Version: 1}
	if h.health != nil {
		state.Roots = h.health.SnapshotResources()
	}
	data, err := yaml.Marshal(state)
	if err != nil {
		slog.Warn("indexed state marshal failed", "instance", h.name, "err", err)
		return
	}
	stateObj := h.statePath()
	if err := h.store.MkdirAll(path.Join(h.name, h.objectRoot), 0o755); err != nil {
		slog.Warn("indexed state mkdir failed", "instance", h.name, "err", err)
		return
	}
	if _, err := h.store.Put(ctx, h.name, stateObj, bytes.NewReader(data), map[string]string{}); err != nil {
		slog.Warn("indexed state write failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) loadState(ctx context.Context) persistedState {
	stateObj := h.statePath()
	reader, err := h.store.OpenObject(ctx, h.name, stateObj)
	if err != nil {
		return persistedState{Version: 1}
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return persistedState{Version: 1}
	}
	var state persistedState
	if err := yaml.Unmarshal(data, &state); err != nil {
		slog.Warn("indexed state unmarshal failed", "instance", h.name, "err", err)
		return persistedState{Version: 1}
	}
	if state.Roots == nil {
		state.Roots = []health.ResourceSnapshot{}
	}
	return state
}

func (h *IndexedHandler) restoreRoots(ctx context.Context) {
	if h.discover == nil {
		return
	}
	prefix := h.objectRoot + "/"
	specByKey := map[string]RootSpec{}
	if err := fs.WalkDir(h.store.TenantFS(h.name), h.objectRoot, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || strings.HasPrefix(path.Base(objectPath), "_") {
			return nil
		}
		cleanPath := strings.TrimPrefix(objectPath, prefix)
		if h.classify(cleanPath) != ResourceMetadata {
			return nil
		}
		if spec, ok := h.discover.Discover(cleanPath); ok {
			specByKey[spec.Key()] = spec
		}
		return nil
	}); err != nil {
		slog.Warn("indexed state walk failed", "instance", h.name, "err", err)
	}

	for _, spec := range specByKey {
		key := spec.Key()
		targets := spec.Targets()
		h.addRoot(key, targets)
	}

	persisted := h.loadState(ctx)
	for _, snap := range persisted.Roots {
		if _, ok := specByKey[snap.Path]; !ok {
			h.addRoot(snap.Path, nil)
		}
	}
}
