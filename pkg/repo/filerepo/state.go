package filerepo

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"sort"
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
	if h.sh != nil {
		h.sh.RestoreResources(persisted.Roots)
	}
	for _, snap := range persisted.Roots {
		if snap.Path != "" {
			h.AddRoot(snap.Path, nil)
		}
	}
}

func (h *IndexedHandler) restoreGenerations(ctx context.Context) {
	rootDir := path.Join(h.objectRoot, ".roots")
	err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || path.Base(objectPath) != "current.yaml" {
			return nil
		}
		snapshot, ok := h.loadCurrentSnapshot(ctx, objectPath)
		if !ok {
			return nil
		}
		for key, obj := range snapshot.Metadata {
			if obj.StorePath == "" {
				obj.StorePath = h.generationMetadataPath(snapshot.RootKey, snapshot.Generation, obj.Path)
				snapshot.Metadata[key] = obj
			}
		}
		h.mu.Lock()
		h.rootSnapshots[snapshot.RootKey] = snapshot
		h.rebuildAggregateLocked()
		if entry, ok := h.roots[snapshot.RootKey]; ok && len(entry.targets) == 0 && len(snapshot.Targets) > 0 {
			entry.targets = snapshot.Targets
		}
		h.mu.Unlock()

		if h.sh != nil {
			h.sh.AddResource(snapshot.RootKey, targetsToProbe(snapshot.Targets), h.upstreams)
			h.sh.MarkResourceActive(snapshot.RootKey, targetsToProbe(snapshot.Targets))
		}
		h.reportMetadataState()
		return nil
	})
	if err != nil && !strings.Contains(err.Error(), "not exist") {
		slog.Warn("indexed generation restore failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) loadCurrentSnapshot(ctx context.Context, currentPath string) (*LiveSnapshot, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, currentPath)
	if err == nil {
		defer reader.Close()
		var ref struct {
			RootKey    string `yaml:"root_key"`
			Generation string `yaml:"generation"`
		}
		if yaml.NewDecoder(reader).Decode(&ref) == nil && ref.RootKey != "" && ref.Generation != "" {
			if snapshot, ok := h.loadSnapshot(ctx, h.snapshotPath(ref.RootKey, ref.Generation)); ok {
				return snapshot, true
			}
		}
	}
	return h.loadLatestSnapshot(ctx, path.Dir(currentPath))
}

func (h *IndexedHandler) loadLatestSnapshot(ctx context.Context, rootDir string) (*LiveSnapshot, bool) {
	dir := path.Join(rootDir, "snapshots")
	var snapshots []struct {
		path       string
		generation string
	}
	if err := fs.WalkDir(h.store.TenantFS(h.name), dir, func(objectPath string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(objectPath, ".yaml") {
			generation := strings.TrimSuffix(path.Base(objectPath), ".yaml")
			snapshots = append(snapshots, struct {
				path       string
				generation string
			}{path: objectPath, generation: generation})
		}
		return nil
	}); err != nil {
		return nil, false
	}
	sort.Slice(snapshots, func(i, j int) bool {
		left := snapshots[i].generation
		right := snapshots[j].generation
		if len(left) == len(right) {
			return left > right
		}
		return len(left) > len(right)
	})
	for _, item := range snapshots {
		if snapshot, ok := h.loadSnapshot(ctx, item.path); ok {
			return snapshot, true
		}
	}
	return nil, false
}

func (h *IndexedHandler) loadSnapshot(ctx context.Context, objectPath string) (*LiveSnapshot, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, false
	}
	defer reader.Close()
	var snapshot LiveSnapshot
	if err := yaml.NewDecoder(reader).Decode(&snapshot); err != nil || snapshot.RootKey == "" {
		return nil, false
	}
	return &snapshot, true
}
