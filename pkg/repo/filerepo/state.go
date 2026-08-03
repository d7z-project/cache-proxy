package filerepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	Root  RepositoryRoot          `yaml:"root"`
	State health.ResourceSnapshot `yaml:"state"`
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
	roots := make(map[string]RepositoryRoot, len(h.roots))
	currentRoots := make(map[string]struct{}, len(h.rootSnapshots))
	for rootID, entry := range h.roots {
		if entry == nil {
			continue
		}
		roots[rootID] = entry.root
	}
	for rootID := range h.rootSnapshots {
		currentRoots[rootID] = struct{}{}
	}
	h.mu.RUnlock()

	resources := map[string]health.ResourceSnapshot{}
	if h.sh != nil {
		for _, item := range h.sh.SnapshotResources() {
			resources[item.Path] = item
		}
	}

	keys := make([]string, 0, len(roots))
	for rootID := range roots {
		keys = append(keys, rootID)
	}
	sort.Strings(keys)

	state := persistedState{Version: 1}
	for _, rootID := range keys {
		snapshot, ok := resources[rootID]
		if !ok {
			if _, keep := currentRoots[rootID]; !keep {
				continue
			}
			snapshot = health.ResourceSnapshot{
				Path:         rootID,
				State:        health.RActive.String(),
				UpstreamURLs: append([]string(nil), h.upstreams...),
			}
		}
		state.Roots = append(state.Roots, persistedRoot{
			Root:  roots[rootID],
			State: snapshot,
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
		if root.Root.ID == "" {
			continue
		}
		h.AddRepository(root.Root)
		if root.State.Path == "" {
			root.State.Path = root.Root.ID
		}
		if len(root.State.LastTargets) == 0 {
			root.State.LastTargets = targetsToResourceTargets(root.Root.Targets)
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
		h.setRootSnapshot(snapshot.RootID, snapshot)

		if h.sh != nil {
			restored := h.sh.AddResource(snapshot.RootID, targetsToResourceTargets(snapshot.Targets), h.upstreams)
			if restored.State == health.RPending && restored.LastSuccessAt.IsZero() {
				h.sh.MarkResourceActive(snapshot.RootID, targetsToResourceTargets(snapshot.Targets))
			}
		}
		h.reportMetadataState()
		return nil
	})
	if err != nil && !strings.Contains(err.Error(), "not exist") {
		slog.Warn("indexed generation restore failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) cleanCurrentRefTemps(ctx context.Context) {
	rootDir := path.Join(h.objectRoot, ".roots")
	err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if walkErr != nil || entry.IsDir() || !strings.HasPrefix(path.Base(objectPath), "current.yaml.tmp.") {
			return nil
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug(
				"indexed current temp cleanup failed",
				"instance", h.name,
				"path", objectPath,
				"err", err,
			)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, context.Canceled) {
		slog.Debug("indexed current temp scan failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) loadCurrentSnapshot(ctx context.Context, currentPath string) (*LiveSnapshot, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, currentPath)
	if err != nil {
		return nil, false
	}
	defer reader.Close()
	var ref struct {
		RootID     string `yaml:"root_id"`
		Generation string `yaml:"generation"`
	}
	if err := yaml.NewDecoder(reader).Decode(&ref); err != nil || ref.RootID == "" || ref.Generation == "" {
		return nil, false
	}
	reject := func(err error) {
		slog.Warn("committed metadata generation rejected", "instance", h.name, "root_id", ref.RootID, "generation", ref.Generation, "err", err)
		if h.sh != nil {
			resource := h.sh.AddResource(ref.RootID, nil, h.upstreams)
			h.sh.FinishRefresh(
				ref.RootID,
				resource.Generation,
				fmt.Errorf("%w: invalid committed metadata generation: %v", health.ErrResourceTransient, err),
				nil,
			)
		}
	}
	snapshot, ok := h.loadSnapshot(ctx, h.snapshotPath(ref.RootID, ref.Generation))
	if !ok || snapshot.RootID != ref.RootID || snapshot.Generation != ref.Generation {
		reject(errors.New("referenced snapshot is missing or has mismatched identity"))
		return nil, false
	}
	if err := h.validateSnapshot(ctx, snapshot, true); err != nil {
		reject(err)
		return nil, false
	}
	return snapshot, true
}

func (h *IndexedHandler) durableCurrentGeneration(ctx context.Context, rootID string) (string, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, h.currentPath(rootID))
	if err != nil {
		return "", false
	}
	defer reader.Close()
	var ref struct {
		RootID     string `yaml:"root_id"`
		Generation string `yaml:"generation"`
	}
	if yaml.NewDecoder(reader).Decode(&ref) != nil || ref.RootID != rootID || ref.Generation == "" {
		return "", false
	}
	return ref.Generation, true
}

func (h *IndexedHandler) loadSnapshot(ctx context.Context, objectPath string) (*LiveSnapshot, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, false
	}
	defer reader.Close()

	var snapshot LiveSnapshot
	if err := yaml.NewDecoder(reader).Decode(&snapshot); err != nil || snapshot.RootID == "" {
		return nil, false
	}
	return &snapshot, true
}
