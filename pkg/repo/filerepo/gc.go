package filerepo

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (h *GenerationManager) GC(ctx context.Context, limit int) (bool, error) {
	h.gcMu.Lock()
	defer h.gcMu.Unlock()
	if err := h.flushLastSeen(ctx); err != nil {
		return false, err
	}
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	if h.gcPhase == generationGCIdle {
		h.gcPhase = generationGCRetire
		h.gcRetained = make(map[string]bool)
	}
	switch h.gcPhase {
	case generationGCRetire:
		return h.retireInactiveRoots(ctx, limit, now)
	case generationGCMarkers:
		return h.scanCommittedGenerations(ctx, limit)
	case generationGCSnapshots:
		return h.deleteUnusedSnapshots(ctx, limit, now)
	case generationGCBlobs:
		return h.deleteUnretainedGenerationBlobs(ctx, limit, now)
	default:
		return false, fmt.Errorf("invalid generation GC phase %d", h.gcPhase)
	}
}

func (h *GenerationManager) retireInactiveRoots(ctx context.Context, limit int, now time.Time) (bool, error) {
	h.mu.RLock()
	rootSet := make(map[string]struct{}, len(h.current)+len(h.pending)+len(h.lastSeen))
	for rootID := range h.current {
		rootSet[rootID] = struct{}{}
	}
	for rootID := range h.pending {
		rootSet[rootID] = struct{}{}
	}
	for rootID := range h.lastSeen {
		rootSet[rootID] = struct{}{}
	}
	h.mu.RUnlock()
	rootIDs := make([]string, 0, len(rootSet))
	for rootID := range rootSet {
		if rootID > h.gcCursor {
			rootIDs = append(rootIDs, rootID)
		}
	}
	sort.Strings(rootIDs)
	inspected := 0
	for _, rootID := range rootIDs {
		if inspected >= limit {
			return true, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		inspected++
		h.mu.Lock()
		activeReader := h.hasActiveReadersLocked(rootID)
		inactive := !h.retiring[rootID] && !activeReader && now.Sub(h.lastSeen[rootID]) >= h.config.InactiveAfter
		if inactive {
			h.retiring[rootID] = true
		}
		h.mu.Unlock()
		if !inactive {
			h.gcCursor = rootID
			continue
		}

		h.commitMu.Lock()
		h.mu.RLock()
		activeReader = h.hasActiveReadersLocked(rootID)
		stillInactive := !activeReader && now.Sub(h.lastSeen[rootID]) >= h.config.InactiveAfter
		h.mu.RUnlock()
		if !stillInactive {
			h.mu.Lock()
			delete(h.retiring, rootID)
			h.mu.Unlock()
			h.commitMu.Unlock()
			h.gcCursor = rootID
			continue
		}
		for _, stateName := range []string{pendingName(rootID), lastSeenName(rootID), currentName(rootID)} {
			if err := os.Remove(statePath(h.config.StateDir, stateName)); err != nil && !errors.Is(err, os.ErrNotExist) {
				h.mu.Lock()
				delete(h.retiring, rootID)
				h.mu.Unlock()
				h.commitMu.Unlock()
				return false, err
			}
		}
		h.mu.Lock()
		delete(h.current, rootID)
		delete(h.retained, rootID)
		delete(h.pending, rootID)
		delete(h.lastSeen, rootID)
		delete(h.lastSeenPersisted, rootID)
		delete(h.retryWindows, rootID)
		delete(h.retiring, rootID)
		delete(h.pollQueued, rootID)
		delete(h.forceRebuildQueued, rootID)
		queued := h.pollQueue[:0]
		for _, queuedRootID := range h.pollQueue {
			if queuedRootID != rootID {
				queued = append(queued, queuedRootID)
			}
		}
		clear(h.pollQueue[len(queued):])
		h.pollQueue = queued
		h.mu.Unlock()
		h.commitMu.Unlock()
		if err := removeEmptyStateParents(statePath(h.config.StateDir, currentName(rootID)), statePath(h.config.StateDir, "repositories")); err != nil {
			return false, err
		}
		h.gcCursor = rootID
	}
	h.gcPhase, h.gcCursor = generationGCMarkers, ""
	return true, nil
}

func (h *GenerationManager) hasActiveReadersLocked(rootID string) bool {
	prefix := rootID + "\x00"
	for identity, count := range h.readers {
		if count > 0 && strings.HasPrefix(identity, prefix) {
			return true
		}
	}
	return false
}

func (h *GenerationManager) scanCommittedGenerations(ctx context.Context, limit int) (bool, error) {
	inspected := 0
	complete := true
	err := filepath.WalkDir(statePath(h.config.StateDir, "repositories"), func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "generations" {
				return filepath.SkipDir
			}
			return nil
		}
		if name <= h.gcCursor || entry.Name() != "current.yaml" {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if inspected >= limit {
			complete = false
			return fs.SkipAll
		}
		inspected++
		var marker currentMarker
		markerErr := readYAML("/", name, &marker)
		if markerErr == nil {
			markerErr = h.validateCurrentMarker(filepath.Base(filepath.Dir(name)), marker)
		}
		if markerErr != nil {
			slog.Warn("invalid current metadata marker ignored during GC", "path", name, "err", markerErr)
			h.gcCursor = name
			return nil
		}
		current := snapshotReference{Generation: marker.Generation, CandidateID: marker.CandidateID, SnapshotSHA256: marker.SnapshotSHA256}
		if _, err := h.loadSnapshot(ctx, marker.RootID, marker.Root, marker.Upstream, current); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("invalid current metadata snapshot ignored during GC", "path", name, "err", err)
			h.gcCursor = name
			return nil
		}
		h.gcRetained[candidatePrefix(marker.RootID, marker.Generation, marker.CandidateID)] = true
		seen := map[string]bool{marker.CandidateID: true}
		retained := 0
		for _, previous := range marker.Previous {
			if retained >= h.config.KeepPrevious {
				break
			}
			if seen[previous.CandidateID] {
				continue
			}
			if _, err := h.loadSnapshot(ctx, marker.RootID, marker.Root, marker.Upstream, previous); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Warn("invalid retained metadata snapshot ignored during GC", "path", name, "candidate", previous.CandidateID, "err", err)
				continue
			}
			seen[previous.CandidateID] = true
			retained++
			h.gcRetained[candidatePrefix(marker.RootID, previous.Generation, previous.CandidateID)] = true
		}
		h.gcCursor = name
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil || !complete {
		return !complete, err
	}
	h.gcPhase, h.gcCursor = generationGCSnapshots, ""
	return true, nil
}

func (h *GenerationManager) deleteUnusedSnapshots(ctx context.Context, limit int, now time.Time) (bool, error) {
	inspected := 0
	complete := true
	err := filepath.WalkDir(statePath(h.config.StateDir, "repositories"), func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || name <= h.gcCursor || entry.Name() != "snapshot.json" {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if inspected >= limit {
			complete = false
			return fs.SkipAll
		}
		inspected++
		prefix, err := candidateStatePrefix(h.config.StateDir, name)
		if err != nil {
			slog.Warn("invalid metadata candidate path ignored during GC", "path", name, "err", err)
			h.gcCursor = name
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if now.Sub(info.ModTime()) < h.config.GracePeriod {
			h.gcRetained[prefix] = true
		}
		h.commitMu.Lock()
		if !h.gcRetained[prefix] && !h.candidateInUse(prefix) {
			err = os.Remove(name)
			if err == nil || errors.Is(err, os.ErrNotExist) {
				err = removeEmptyStateParents(name, statePath(h.config.StateDir, "repositories"))
			}
		}
		h.commitMu.Unlock()
		if err != nil {
			return err
		}
		h.gcCursor = name
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil || !complete {
		return !complete, err
	}
	h.gcPhase, h.gcCursor = generationGCBlobs, ""
	return true, nil
}

// candidateInUse is checked under commitMu before deletion, so publication
// cannot race the final protection check. Reader accounting remains under mu.
func (h *GenerationManager) candidateInUse(prefix string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for rootID, current := range h.current {
		if candidatePrefix(rootID, current.snapshot.Generation, current.snapshot.CandidateID) == prefix {
			return true
		}
	}
	for rootID, previousSnapshots := range h.retained {
		for _, previous := range previousSnapshots {
			if candidatePrefix(rootID, previous.snapshot.Generation, previous.snapshot.CandidateID) == prefix {
				return true
			}
		}
	}
	for rootID, pending := range h.pending {
		if candidatePrefix(rootID, pending.Generation, pending.CandidateID) == prefix {
			return true
		}
	}
	parts := strings.Split(prefix, "/")
	for identity, count := range h.readers {
		rootID, candidateID, ok := strings.Cut(identity, "\x00")
		if count > 0 && ok && candidateID == parts[3] && path.Base(repositoryDirectory(rootID)) == parts[1] {
			return true
		}
	}
	return false
}

func (h *GenerationManager) deleteUnretainedGenerationBlobs(ctx context.Context, limit int, now time.Time) (bool, error) {
	inspected := 0
	complete := true
	err := fs.WalkDir(h.config.Store.TenantFS(h.config.Tenant), "generations", func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || objectPath <= h.gcCursor {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if inspected >= limit {
			complete = false
			return fs.SkipAll
		}
		inspected++
		parts := strings.Split(objectPath, "/")
		if len(parts) < 5 || !validSHA256(parts[1]) || !validSHA256(parts[2]) || !validCandidateID(parts[3]) {
			h.gcCursor = objectPath
			return nil
		}
		prefix := strings.Join(parts[:4], "/")
		if h.gcRetained[prefix] {
			h.gcCursor = objectPath
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if now.Sub(info.ModTime()) < h.config.GracePeriod {
			h.gcCursor = objectPath
			return nil
		}
		h.commitMu.Lock()
		if !h.candidateInUse(prefix) {
			err = h.config.Store.DeleteObject(ctx, h.config.Tenant, objectPath)
		}
		h.commitMu.Unlock()
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		h.gcCursor = objectPath
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	if err != nil || !complete {
		return !complete, err
	}
	h.gcPhase, h.gcCursor, h.gcRetained = generationGCIdle, "", nil
	return false, nil
}

func candidateStatePrefix(stateDir, snapshotPath string) (string, error) {
	repositories := statePath(stateDir, "repositories")
	relative, err := filepath.Rel(repositories, snapshotPath)
	if err != nil {
		return "", err
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	if len(parts) != 5 || !validSHA256(parts[0]) || parts[1] != "generations" || !validSHA256(parts[2]) || !validCandidateID(parts[3]) || parts[4] != "snapshot.json" {
		return "", errors.New("invalid candidate snapshot path")
	}
	return "generations/" + parts[0] + "/" + parts[2] + "/" + parts[3], nil
}

func removeEmptyStateParents(filename, stop string) error {
	stop = filepath.Clean(stop)
	for directory := filepath.Dir(filename); directory != stop && strings.HasPrefix(directory, stop+string(filepath.Separator)); directory = filepath.Dir(directory) {
		if err := os.Remove(directory); err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, fs.ErrExist) {
				return nil
			}
			return err
		}
	}
	return nil
}
