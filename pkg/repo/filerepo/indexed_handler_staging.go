package filerepo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	refreshSliceDuration  = 20 * time.Second
	refreshSliceTransfers = 32
	refreshStagingMaxAge  = 24 * time.Hour
)

type refreshStagingState struct {
	Version          int               `yaml:"version"`
	RootID           string            `yaml:"root_id"`
	Generation       string            `yaml:"generation"`
	Upstream         string            `yaml:"upstream"`
	CreatedAt        time.Time         `yaml:"created_at"`
	Phase            string            `yaml:"phase"`
	EntryCursor      int               `yaml:"entry_cursor"`
	ParseCursor      int               `yaml:"parse_cursor"`
	AnchorSetDigest  string            `yaml:"anchor_set_sha256,omitempty"`
	Anchors          []MetadataAnchor  `yaml:"anchors,omitempty"`
	CleanupFragments []cleanupFragment `yaml:"cleanup_fragments,omitempty"`
}

type cleanupFragment struct {
	Name   string `yaml:"name"`
	Digest string `yaml:"sha256"`
}

func (h *IndexedHandler) stagingStatePath(rootID string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "staging.yaml")
}

func (h *IndexedHandler) loadRefreshStaging(ctx context.Context, rootID string) (refreshStagingState, bool, error) {
	reader, err := h.store.OpenObject(ctx, h.name, h.stagingStatePath(rootID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return refreshStagingState{}, false, nil
		}
		return refreshStagingState{}, false, fmt.Errorf("open metadata refresh staging: %w", err)
	}
	data, readErr := readBoundedState(reader, 4<<20)
	readErr = errors.Join(readErr, reader.Close())
	if readErr != nil && !errors.Is(readErr, errPersistedStateTooLarge) {
		return refreshStagingState{}, false, fmt.Errorf("read metadata refresh staging: %w", readErr)
	}
	var state refreshStagingState
	decodeErr := strictYAML(data, &state)
	if readErr != nil || decodeErr != nil || state.Version != 2 || state.RootID != rootID ||
		state.Generation == "" || state.Upstream == "" || state.CreatedAt.IsZero() || time.Since(state.CreatedAt) > refreshStagingMaxAge {
		return refreshStagingState{}, false, h.discardRefreshStaging(ctx, rootID, state)
	}
	if len(state.Anchors) > 0 {
		digest, err := metadataAnchorsDigest(state.Anchors)
		if err != nil || digest != state.AnchorSetDigest {
			return refreshStagingState{}, false, h.discardRefreshStaging(ctx, rootID, state)
		}
	}
	for _, fragment := range state.CleanupFragments {
		if path.Base(fragment.Name) != fragment.Name || !strings.HasSuffix(fragment.Name, ".txt") ||
			len(fragment.Digest) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(fragment.Digest, "sha256:") {
			return refreshStagingState{}, false, h.discardRefreshStaging(ctx, rootID, state)
		}
	}
	return state, true, nil
}

func (h *IndexedHandler) saveRefreshStaging(ctx context.Context, state refreshStagingState) error {
	state.Version = 2
	if state.Phase == "" {
		state.Phase = "metadata_fetch"
	}
	if len(state.Anchors) > 0 {
		var err error
		state.AnchorSetDigest, err = metadataAnchorsDigest(state.Anchors)
		if err != nil {
			return err
		}
	}
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

func (h *IndexedHandler) discardRefreshStaging(ctx context.Context, rootID string, state refreshStagingState) error {
	var cleanupErr error
	if state.RootID == rootID && state.Generation != "" {
		cleanupErr = h.removeCandidateGeneration(rootID, state.Generation)
	}
	return errors.Join(cleanupErr, h.deleteRefreshStaging(ctx, rootID))
}

func (h *IndexedHandler) cleanupFragmentPath(rootID, generation, name string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "generations", generation, "cleanup", "fragments", name)
}

func (h *IndexedHandler) persistCleanupFragment(
	ctx context.Context,
	state refreshStagingState,
	index *PathIndexBuilder,
) (cleanupFragment, error) {
	reader, err := index.rewind()
	if err != nil {
		return cleanupFragment{}, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return cleanupFragment{}, err
	}
	fragment := cleanupFragment{
		Name:   fmt.Sprintf("%08d.txt", len(state.CleanupFragments)),
		Digest: "sha256:" + hex.EncodeToString(hash.Sum(nil)),
	}
	reader, err = index.rewind()
	if err != nil {
		return cleanupFragment{}, err
	}
	objectPath := h.cleanupFragmentPath(state.RootID, state.Generation, fragment.Name)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(objectPath)), 0o755); err != nil {
		return cleanupFragment{}, err
	}
	if _, err := h.store.Put(ctx, h.name, objectPath, reader, map[string]string{
		"content-type": "text/plain; charset=utf-8", "mode": h.mode,
	}); err != nil {
		return cleanupFragment{}, err
	}
	return fragment, nil
}

func (h *IndexedHandler) mergeCleanupFragments(
	ctx context.Context,
	state refreshStagingState,
	current *PathIndexBuilder,
) (*PathIndexBuilder, error) {
	merged := &PathIndexBuilder{}
	appendIndex := func(reader io.Reader, expectedDigest string) error {
		hash := sha256.New()
		scanner := bufio.NewScanner(io.TeeReader(reader, hash))
		scanner.Buffer(make([]byte, 4096), 8192)
		for scanner.Scan() {
			merged.Add(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		if expectedDigest != "" && "sha256:"+hex.EncodeToString(hash.Sum(nil)) != expectedDigest {
			return errors.New("cleanup fragment digest mismatch")
		}
		return merged.err
	}
	for _, fragment := range state.CleanupFragments {
		reader, err := h.store.OpenObject(ctx, h.name, h.cleanupFragmentPath(state.RootID, state.Generation, fragment.Name))
		if err != nil {
			_ = merged.Close()
			return nil, fmt.Errorf("open cleanup fragment %s: %w", fragment.Name, err)
		}
		err = appendIndex(reader, fragment.Digest)
		closeErr := reader.Close()
		if err != nil || closeErr != nil {
			_ = merged.Close()
			return nil, errors.Join(err, closeErr)
		}
	}
	reader, err := current.rewind()
	if err == nil {
		err = appendIndex(reader, "")
	}
	if err != nil {
		_ = merged.Close()
		return nil, err
	}
	return merged, nil
}
