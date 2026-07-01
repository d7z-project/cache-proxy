package filerepo

import (
	"bytes"
	"context"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/health"
)

const rootsStateFileName = "_roots.yaml"

type persistedRoot struct {
	Path    string                  `yaml:"path"`
	Targets []MetadataTarget        `yaml:"targets,omitempty"`
	State   health.ResourceSnapshot `yaml:"state"`
}

type persistedState struct {
	Version int             `yaml:"version"`
	Roots   []persistedRoot `yaml:"roots"`
}

func (h *IndexedHandler) statePath() string {
	return path.Join(h.objectRoot, rootsStateFileName)
}

func (h *IndexedHandler) saveState(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}

	h.mu.RLock()
	rootTargets := make(map[string][]MetadataTarget, len(h.roots))
	currentRoots := make(map[string]struct{}, len(h.rootSnapshots))
	for rootKey, entry := range h.roots {
		if entry == nil {
			continue
		}
		rootTargets[rootKey] = append([]MetadataTarget(nil), entry.targets...)
	}
	for rootKey := range h.rootSnapshots {
		currentRoots[rootKey] = struct{}{}
	}
	h.mu.RUnlock()

	resources := map[string]health.ResourceSnapshot{}
	if h.sh != nil {
		for _, item := range h.sh.SnapshotResources() {
			resources[item.Path] = item
		}
	}

	keys := make([]string, 0, len(rootTargets))
	for rootKey := range rootTargets {
		keys = append(keys, rootKey)
	}
	sort.Strings(keys)

	state := persistedState{Version: 1}
	for _, rootKey := range keys {
		snapshot, ok := resources[rootKey]
		if !ok {
			if _, keep := currentRoots[rootKey]; !keep {
				continue
			}
			snapshot = health.ResourceSnapshot{
				Path:         rootKey,
				State:        health.RActive.String(),
				UpstreamURLs: append([]string(nil), h.upstreams...),
			}
		}
		state.Roots = append(state.Roots, persistedRoot{
			Path:    rootKey,
			Targets: rootTargets[rootKey],
			State:   snapshot,
		})
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
	if _, err := h.store.Put(ctx, h.name, h.statePath(), bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		slog.Warn("indexed state write failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) loadState(ctx context.Context) persistedState {
	reader, err := h.store.OpenObject(ctx, h.name, h.statePath())
	if err != nil {
		return persistedState{Version: 1}
	}
	defer reader.Close()

	var state persistedState
	if err := yaml.NewDecoder(reader).Decode(&state); err != nil {
		slog.Warn("indexed state unmarshal failed", "instance", h.name, "err", err)
		return persistedState{Version: 1}
	}
	return state
}

func (h *IndexedHandler) restoreRoots(ctx context.Context) {
	persisted := h.loadState(ctx)
	var resources []health.ResourceSnapshot
	for _, root := range persisted.Roots {
		if root.Path == "" {
			continue
		}
		h.AddRoot(root.Path, root.Targets)
		if root.State.Path == "" {
			root.State.Path = root.Path
		}
		if len(root.State.UpstreamURLs) == 0 {
			root.State.UpstreamURLs = append([]string(nil), h.upstreams...)
		}
		resources = append(resources, root.State)
	}
	if h.sh != nil {
		h.sh.RestoreResources(resources)
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
			entry.targets = append([]MetadataTarget(nil), snapshot.Targets...)
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
