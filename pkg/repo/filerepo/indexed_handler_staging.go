package filerepo

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"path"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	refreshSliceDuration  = 20 * time.Second
	refreshSliceTransfers = 32
	refreshStagingMaxAge  = 24 * time.Hour
)

type refreshStagingState struct {
	Version      int       `yaml:"version"`
	RootID       string    `yaml:"root_id"`
	Generation   string    `yaml:"generation"`
	Upstream     string    `yaml:"upstream"`
	CreatedAt    time.Time `yaml:"created_at"`
	AnchorPath   string    `yaml:"anchor_path,omitempty"`
	AnchorDigest string    `yaml:"anchor_sha256,omitempty"`
}

func (h *IndexedHandler) stagingStatePath(rootID string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "staging.yaml")
}

func (h *IndexedHandler) loadRefreshStaging(ctx context.Context, rootID string) (refreshStagingState, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, h.stagingStatePath(rootID))
	if err != nil {
		return refreshStagingState{}, false
	}
	defer func() { _ = reader.Close() }()
	var state refreshStagingState
	if yaml.NewDecoder(reader).Decode(&state) != nil || state.Version != 1 || state.RootID != rootID ||
		state.Generation == "" || state.Upstream == "" || state.CreatedAt.IsZero() || time.Since(state.CreatedAt) > refreshStagingMaxAge {
		return refreshStagingState{}, false
	}
	return state, true
}

func (h *IndexedHandler) saveRefreshStaging(ctx context.Context, state refreshStagingState) error {
	state.Version = 1
	data, err := yaml.Marshal(state)
	if err != nil {
		return err
	}
	statePath := h.stagingStatePath(state.RootID)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(statePath)), 0o755); err != nil {
		return err
	}
	tmpPath := statePath + ".tmp"
	if _, err := h.store.Put(ctx, h.name, tmpPath, bytes.NewReader(data), map[string]string{"mode": h.mode}); err != nil {
		return err
	}
	return h.store.Rename(path.Join(h.name, tmpPath), path.Join(h.name, statePath))
}

func (h *IndexedHandler) deleteRefreshStaging(ctx context.Context, rootID string) error {
	err := h.store.DeleteObject(ctx, h.name, h.stagingStatePath(rootID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (h *IndexedHandler) discardRefreshStaging(ctx context.Context, state refreshStagingState) {
	if state.RootID != "" && state.Generation != "" {
		h.cleanupFailedGeneration(state.RootID, state.Generation)
	}
	_ = h.deleteRefreshStaging(ctx, state.RootID)
}
