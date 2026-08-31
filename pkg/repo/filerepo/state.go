package filerepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const rootsStateFileName = "_roots.yaml"

const (
	maxRootsStateSize   = 16 << 20
	maxCurrentStateSize = 1 << 20
	maxSnapshotSize     = 16 << 20
)

var errPersistedStateTooLarge = errors.New("persisted state exceeds size limit")

type persistedRoot struct {
	Root              RepositoryRoot `yaml:"root"`
	LastSeenAt        time.Time      `yaml:"last_seen_at"`
	LastValidatedAt   time.Time      `yaml:"last_validated_at,omitempty"`
	Retired           bool           `yaml:"retired,omitempty"`
	CleanupGeneration string         `yaml:"cleanup_generation,omitempty"`
	CleanupDigest     string         `yaml:"cleanup_index_sha256,omitempty"`
}

type persistedState struct {
	Version  int             `yaml:"version"`
	Revision uint64          `yaml:"revision"`
	Roots    []persistedRoot `yaml:"roots"`
}

func (h *IndexedHandler) statePath() string {
	return path.Join(h.objectRoot, rootsStateFileName)
}

func (h *IndexedHandler) saveState(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	h.stateLifecycleMu.Lock()
	signal := h.stateSignal
	stopped := h.stateStopped
	h.stateLifecycleMu.Unlock()
	if stopped {
		return
	}
	if signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
		return
	}
	if err := h.writeState(ctx); err != nil {
		slog.Warn("indexed state write failed", "instance", h.name, "err", err)
	}
}

func (h *IndexedHandler) writeState(ctx context.Context) error {

	h.stateWriteMu.Lock()
	defer h.stateWriteMu.Unlock()
	h.mu.Lock()
	h.stateRevision++
	revision := h.stateRevision
	roots := make(map[string]rootEntry, len(h.roots))
	for rootID, entry := range h.roots {
		if entry == nil {
			continue
		}
		roots[rootID] = *entry
	}
	h.mu.Unlock()

	keys := make([]string, 0, len(roots))
	for rootID := range roots {
		keys = append(keys, rootID)
	}
	sort.Strings(keys)

	state := persistedState{Version: 2, Revision: revision}
	for _, rootID := range keys {
		state.Roots = append(state.Roots, persistedRoot{
			Root:              roots[rootID].root,
			LastSeenAt:        roots[rootID].lastSeenAt,
			LastValidatedAt:   roots[rootID].lastValidatedAt,
			Retired:           roots[rootID].retired,
			CleanupGeneration: roots[rootID].retirementCleanupGeneration,
			CleanupDigest:     roots[rootID].retirementCleanupDigest,
		})
	}

	data, err := yaml.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal indexed state: %w", err)
	}
	if err := h.store.MkdirAll(path.Join(h.name, h.objectRoot), 0o755); err != nil {
		return fmt.Errorf("create indexed state directory: %w", err)
	}
	tmpPath := h.statePath() + ".tmp"
	if _, err := h.store.Put(ctx, h.name, tmpPath, bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		return fmt.Errorf("write indexed state temporary object: %w", err)
	}
	if err := h.store.Rename(path.Join(h.name, tmpPath), path.Join(h.name, h.statePath())); err != nil {
		return fmt.Errorf("commit indexed state: %w", err)
	}
	return nil
}

func (h *IndexedHandler) startStateWriter() {
	h.stateLifecycleMu.Lock()
	defer h.stateLifecycleMu.Unlock()
	if h.stateSignal != nil || h.stateStopped {
		return
	}
	h.stateSignal = make(chan struct{}, 1)
	h.stateStop = make(chan chan error)
	h.stateWriterDone = make(chan struct{})
	go h.runStateWriter(h.stateSignal, h.stateStop, h.stateWriterDone)
}

func (h *IndexedHandler) runStateWriter(signal <-chan struct{}, stop <-chan chan error, done chan<- struct{}) {
	defer close(done)
	var timer *time.Timer
	var timerC <-chan time.Time
	dirty := false
	flush := func() error {
		if !dirty {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := h.writeState(ctx); err != nil {
			return err
		}
		dirty = false
		return nil
	}
	for {
		select {
		case <-signal:
			dirty = true
			if timer == nil {
				timer = time.NewTimer(100 * time.Millisecond)
				timerC = timer.C
			}
		case <-timerC:
			if err := flush(); err != nil {
				slog.Warn("indexed state write failed", "instance", h.name, "err", err)
				timer.Reset(time.Second)
				timerC = timer.C
				continue
			}
			timer = nil
			timerC = nil
		case response := <-stop:
			if timer != nil {
				timer.Stop()
			}
			response <- flush()
			return
		}
	}
}

func (h *IndexedHandler) stopStateWriter(ctx context.Context) error {
	h.stateLifecycleMu.Lock()
	if h.stateSignal == nil {
		h.stateStopped = true
		h.stateLifecycleMu.Unlock()
		return nil
	}
	stop := h.stateStop
	done := h.stateWriterDone
	h.stateSignal = nil
	h.stateStop = nil
	h.stateWriterDone = nil
	h.stateStopped = true
	h.stateLifecycleMu.Unlock()

	response := make(chan error, 1)
	select {
	case stop <- response:
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *IndexedHandler) loadState(ctx context.Context) (persistedState, bool, error) {
	reader, err := h.store.OpenObject(ctx, h.name, h.statePath())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return persistedState{}, false, nil
		}
		return persistedState{}, false, fmt.Errorf("open indexed state: %w", err)
	}
	data, readErr := readBoundedState(reader, maxRootsStateSize)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		if errors.Is(err, errPersistedStateTooLarge) {
			slog.Warn("indexed state is oversized", "instance", h.name, "err", err)
			return persistedState{}, false, nil
		}
		return persistedState{}, false, fmt.Errorf("read indexed state: %w", err)
	}
	var state persistedState
	decodeErr := strictYAML(data, &state)
	if decodeErr != nil || state.Version != 2 || state.Revision == 0 {
		stateErr := decodeErr
		if stateErr == nil {
			stateErr = fmt.Errorf("unsupported roots state version %d or revision %d", state.Version, state.Revision)
		}
		slog.Warn("indexed state unmarshal failed", "instance", h.name, "err", stateErr)
		return persistedState{}, false, nil
	}
	return state, true, nil
}

func (h *IndexedHandler) restoreRoots(ctx context.Context) error {
	persisted, ok, err := h.loadState(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	h.mu.Lock()
	h.stateRevision = persisted.Revision
	h.mu.Unlock()
	for _, root := range persisted.Roots {
		if root.Root.ID == "" {
			continue
		}
		h.AddRepository(root.Root)
		h.mu.Lock()
		if entry := h.roots[root.Root.ID]; entry != nil {
			entry.lastSeenAt = root.LastSeenAt
			if entry.lastSeenAt.IsZero() {
				entry.lastSeenAt = time.Now().UTC()
			}
			entry.lastSeenSavedAt = entry.lastSeenAt
			entry.lastValidatedAt = root.LastValidatedAt
			entry.retired = root.Retired
			entry.retirementCleanupGeneration = root.CleanupGeneration
			entry.retirementCleanupDigest = root.CleanupDigest
		}
		h.mu.Unlock()
	}
	return nil
}

func (h *IndexedHandler) restoreGenerations(ctx context.Context) error {
	rootDir := path.Join(h.objectRoot, ".roots")
	err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if entry.IsDir() || path.Base(objectPath) != "current.yaml" {
			return nil
		}
		snapshot, ok, err := h.loadCurrentSnapshot(ctx, objectPath)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		h.setRootSnapshot(snapshot.RootID, snapshot)
		h.mu.Lock()
		if h.roots[snapshot.RootID] == nil {
			primary := make([]string, 0, len(snapshot.Anchors))
			for _, anchor := range snapshot.Anchors {
				if anchor.State == MetadataPresent {
					primary = append(primary, anchor.Path)
				}
			}
			now := time.Now().UTC()
			h.roots[snapshot.RootID] = &rootEntry{root: RepositoryRoot{
				ID: snapshot.RootID, Path: snapshot.RootPath, DisplayName: snapshot.RootPath,
				PrimaryMetadata: primary, Targets: append([]MetadataTarget(nil), snapshot.Targets...),
			}, closureRevision: 1, lastSeenAt: now, lastValidatedAt: snapshot.Published}
		}
		h.mu.Unlock()
		h.reportMetadataState()
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (h *IndexedHandler) cleanCurrentRefTemps(ctx context.Context) {
	rootDir := path.Join(h.objectRoot, ".roots")
	err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasPrefix(path.Base(objectPath), "current.yaml.tmp.") {
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

func (h *IndexedHandler) loadCurrentSnapshot(ctx context.Context, currentPath string) (*LiveSnapshot, bool, error) {
	reader, err := h.store.OpenObject(ctx, h.name, currentPath)
	if err != nil {
		return nil, false, fmt.Errorf("open committed metadata reference %s: %w", currentPath, err)
	}
	data, readErr := readBoundedState(reader, maxCurrentStateSize)
	_ = reader.Close()
	var ref currentReference
	if readErr != nil || strictYAML(data, &ref) != nil || ref.Version != 2 || ref.RootID == "" || ref.Generation == "" ||
		ref.SnapshotDigest == "" || ref.CleanupIndexDigest == "" {
		if err := h.store.DeleteObject(ctx, h.name, currentPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, false, fmt.Errorf("discard invalid metadata reference %s: %w", currentPath, err)
		}
		return nil, false, nil
	}
	reject := func(cause error) error {
		slog.Warn("committed metadata generation rejected", "instance", h.name, "root_id", ref.RootID, "generation", ref.Generation, "err", cause)
		if err := h.store.DeleteObject(ctx, h.name, currentPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("discard rejected metadata reference %s: %w", currentPath, err)
		}
		return nil
	}
	snapshot, snapshotDigest, ok := h.loadSnapshotWithDigest(ctx, h.snapshotPath(ref.RootID, ref.Generation))
	if !ok || snapshot.RootID != ref.RootID || snapshot.Generation != ref.Generation {
		return nil, false, reject(errors.New("referenced snapshot is missing or has mismatched identity"))
	}
	if snapshotDigest != ref.SnapshotDigest || snapshot.CleanupIndexDigest != ref.CleanupIndexDigest {
		return nil, false, reject(errors.New("current reference digest mismatch"))
	}
	if err := h.validateSnapshot(ctx, snapshot, true); err != nil {
		return nil, false, reject(err)
	}
	return snapshot, true, nil
}

func (h *IndexedHandler) durableCurrentGeneration(ctx context.Context, rootID string) (string, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, h.currentPath(rootID))
	if err != nil {
		return "", false
	}
	defer func() { _ = reader.Close() }()
	data, err := readBoundedState(reader, maxCurrentStateSize)
	if err != nil {
		return "", false
	}
	var ref currentReference
	if strictYAML(data, &ref) != nil || ref.Version != 2 || ref.RootID != rootID || ref.Generation == "" ||
		ref.SnapshotDigest == "" || ref.CleanupIndexDigest == "" {
		return "", false
	}
	return ref.Generation, true
}

func (h *IndexedHandler) loadSnapshot(ctx context.Context, objectPath string) (*LiveSnapshot, bool) {
	snapshot, _, ok := h.loadSnapshotWithDigest(ctx, objectPath)
	return snapshot, ok
}

func (h *IndexedHandler) loadSnapshotWithDigest(ctx context.Context, objectPath string) (*LiveSnapshot, string, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, "", false
	}
	data, readErr := readBoundedState(reader, maxSnapshotSize)
	_ = reader.Close()
	var snapshot LiveSnapshot
	if readErr != nil || strictYAML(data, &snapshot) != nil || snapshot.Version != snapshotSchemaVersion || snapshot.RootID == "" {
		return nil, "", false
	}
	for cleanPath, object := range snapshot.Metadata {
		if object.State == MetadataPresent {
			object.StorePath = h.generationMetadataPath(snapshot.RootID, snapshot.Generation, object.Path)
		}
		object.StatusCode = metadataStateStatus(object.State)
		snapshot.Metadata[cleanPath] = object
	}
	return &snapshot, digestBytes(data), true
}

func readBoundedState(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: limit %d bytes", errPersistedStateTooLarge, limit)
	}
	return data, nil
}
