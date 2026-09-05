package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	maxCurrentMarkerSize    = 64 << 10
	maxRepositoryMarkerSize = 2 << 20
	maxSnapshotStateSize    = 256 << 20
)

func encodeJSONDigest(value any) ([]byte, string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	data = append(data, '\n')
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:]), nil
}

func prepareJSON(root, name string, value any, limit int64) (*preparedStateFile, error) {
	data, _, err := encodeJSONDigest(value)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("state document exceeds %d bytes", limit)
	}
	return prepareBytes(root, name, data)
}

func writeBytes(root, name string, data []byte) error {
	prepared, err := prepareBytes(root, name, data)
	if err != nil {
		return err
	}
	defer prepared.discard()
	return prepared.commit()
}

type preparedStateFile struct {
	temporary   string
	destination string
}

func prepareBytes(root, name string, data []byte) (*preparedStateFile, error) {
	cleaned, err := CleanPath(name)
	if err != nil {
		return nil, err
	}
	destination := filepath.Join(root, filepath.FromSlash(cleaned))
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return nil, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".cache-proxy-tmp-state-*")
	if err != nil {
		return nil, err
	}
	temporaryName := temporary.Name()
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
		return nil, err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
		return nil, err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryName)
		return nil, err
	}
	return &preparedStateFile{temporary: temporaryName, destination: destination}, nil
}

func (f *preparedStateFile) commit() error {
	if err := os.Rename(f.temporary, f.destination); err != nil {
		return err
	}
	f.temporary = ""
	directory, err := os.Open(filepath.Dir(f.destination))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (f *preparedStateFile) discard() {
	if f != nil && f.temporary != "" {
		_ = os.Remove(f.temporary)
	}
}

func readJSON(root, name string, limit int64, target any) error {
	data, err := readStateFile(statePath(root, name), limit)
	if err != nil {
		return err
	}
	return decodeJSON(data, target)
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("state contains multiple JSON values")
		}
		return err
	}
	return nil
}

func prepareYAML(root, name string, value any) (*preparedStateFile, error) {
	data, err := yaml.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > maxCurrentMarkerSize {
		return nil, fmt.Errorf("state document exceeds %d bytes", maxCurrentMarkerSize)
	}
	return prepareBytes(root, name, data)
}

func readYAML(root, name string, target any) error {
	data, err := readStateFile(statePath(root, name), maxCurrentMarkerSize)
	if err != nil {
		return err
	}
	return decodeYAML(data, target)
}

func readStateFile(name string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("state file limit must be positive")
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	readLimit := limit
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	data, err := io.ReadAll(io.LimitReader(file, readLimit))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("state file exceeds %d bytes", limit)
	}
	return data, nil
}

func decodeYAML(data []byte, target any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("state contains multiple YAML documents")
		}
		return err
	}
	return nil
}

func statePath(root, name string) string {
	return filepath.Join(root, filepath.FromSlash(name))
}

func repositoryDirectory(rootID string) string {
	sum := sha256.Sum256([]byte(rootID))
	return "repositories/" + hex.EncodeToString(sum[:])
}

func currentName(rootID string) string  { return repositoryDirectory(rootID) + "/current.yaml" }
func pendingName(rootID string) string  { return repositoryDirectory(rootID) + "/pending.json" }
func lastSeenName(rootID string) string { return repositoryDirectory(rootID) + "/last-seen.json" }
func snapshotName(rootID, generation, candidateID string) string {
	return repositoryDirectory(rootID) + "/generations/" + generation + "/" + candidateID + "/snapshot.json"
}

func prepareSnapshot(snapshot *Snapshot) error {
	cleanRootValue, rootErr := cleanRoot(snapshot.Root)
	cleanAnchor, anchorErr := CleanPath(snapshot.Anchor)
	upstream, upstreamErr := url.Parse(snapshot.Upstream)
	validRoot := rootErr == nil && cleanRootValue == snapshot.Root
	validAnchor := anchorErr == nil && cleanAnchor == snapshot.Anchor && containsPath(snapshot.Root, snapshot.Anchor)
	validUpstream := upstreamErr == nil && (upstream.Scheme == "http" || upstream.Scheme == "https") && upstream.Host != ""
	if snapshot.RootID == "" || !validRoot || !validAnchor || !validSHA256(snapshot.Generation) || !validUpstream || snapshot.PublishedAt.IsZero() {
		return errors.New("invalid metadata snapshot")
	}
	if !validCandidateID(snapshot.CandidateID) {
		return errors.New("invalid metadata candidate ID")
	}
	generationRoot := candidatePrefix(snapshot.RootID, snapshot.Generation, snapshot.CandidateID)
	snapshot.byPath = make(map[string]Object, len(snapshot.Objects))
	for _, object := range snapshot.Objects {
		cleaned, err := CleanPath(object.Path)
		if err != nil || cleaned != object.Path || !containsPath(snapshot.Root, object.Path) {
			return fmt.Errorf("invalid snapshot object path %q", object.Path)
		}
		if _, duplicate := snapshot.byPath[object.Path]; duplicate {
			return fmt.Errorf("duplicate snapshot object %q", object.Path)
		}
		storedObject := strings.HasPrefix(object.Key, generationRoot+"/objects/")
		storedAnchor := object.Path == snapshot.Anchor && object.Key == generationRoot+"/anchor" &&
			strings.EqualFold(object.SHA256, snapshot.Generation)
		validKey := storedObject || storedAnchor
		if object.Size < 0 || !validSHA256(object.SHA256) || !validKey || object.Retainable && object.Path == snapshot.Anchor {
			return fmt.Errorf("invalid snapshot object %q", object.Path)
		}
		snapshot.byPath[object.Path] = object
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (h *GenerationManager) restore() error {
	repositories := statePath(h.config.StateDir, "repositories")
	entries, err := os.ReadDir(repositories)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		directory := "repositories/" + entry.Name()
		var seen lastSeenMarker
		seenPresent := false
		if err := readJSON(h.config.StateDir, directory+"/last-seen.json", maxRepositoryMarkerSize, &seen); err == nil {
			if seen.RootID != "" && path.Base(repositoryDirectory(seen.RootID)) == entry.Name() && !seen.SeenAt.IsZero() {
				h.lastSeen[seen.RootID] = seen.SeenAt
				h.lastSeenPersisted[seen.RootID] = seen.SeenAt
				seenPresent = true
			} else {
				slog.Warn("invalid metadata last-seen state ignored", "mode", h.config.Mode, "repository", entry.Name())
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("metadata last-seen state ignored", "mode", h.config.Mode, "repository", entry.Name(), "err", err)
		}

		if err := h.restoreCurrentGeneration(entry.Name(), seen, seenPresent); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("current metadata state ignored", "mode", h.config.Mode, "repository", entry.Name(), "err", err)
		}

		if err := h.restorePendingAnchor(entry.Name(), seen, seenPresent); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("pending metadata state ignored", "mode", h.config.Mode, "repository", entry.Name(), "err", err)
		}
	}
	return nil
}

func (h *GenerationManager) restoreCurrentGeneration(repositoryName string, seen lastSeenMarker, seenPresent bool) error {
	directory := "repositories/" + repositoryName
	var marker currentMarker
	if err := readYAML(h.config.StateDir, directory+"/current.yaml", &marker); err != nil {
		return err
	}
	if marker.Upstream != h.config.Upstream {
		if err := os.Remove(statePath(h.config.StateDir, directory+"/current.yaml")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove metadata current marker for obsolete upstream: %w", err)
		}
		return nil
	}
	if err := h.validateCurrentMarker(repositoryName, marker); err != nil {
		return err
	}
	currentReference := snapshotReference{
		Generation:     marker.Generation,
		CandidateID:    marker.CandidateID,
		SnapshotSHA256: marker.SnapshotSHA256,
	}
	current, err := h.loadSnapshot(context.Background(), marker.RootID, marker.Root, marker.Upstream, currentReference)
	if err != nil {
		return fmt.Errorf("load current metadata snapshot: %w", err)
	}
	previousSnapshots := make([]*liveSnapshot, 0, min(len(marker.Previous), h.config.KeepPrevious))
	seenCandidateIDs := map[string]struct{}{marker.CandidateID: {}}
	for _, reference := range marker.Previous {
		if len(previousSnapshots) >= h.config.KeepPrevious {
			break
		}
		if _, duplicate := seenCandidateIDs[reference.CandidateID]; duplicate {
			continue
		}
		previousSnapshot, err := h.loadSnapshot(context.Background(), marker.RootID, marker.Root, marker.Upstream, reference)
		if err != nil {
			slog.Warn("retained metadata snapshot ignored", "mode", h.config.Mode, "repository", repositoryName, "candidate", reference.CandidateID, "err", err)
			continue
		}
		seenCandidateIDs[reference.CandidateID] = struct{}{}
		previousSnapshots = append(previousSnapshots, previousSnapshot)
	}
	h.current[current.snapshot.RootID] = current
	if len(previousSnapshots) == 0 {
		delete(h.retained, current.snapshot.RootID)
	} else {
		h.retained[current.snapshot.RootID] = previousSnapshots
	}
	if !seenPresent || seen.RootID != current.snapshot.RootID {
		h.lastSeen[current.snapshot.RootID] = current.snapshot.PublishedAt
		delete(h.lastSeenPersisted, current.snapshot.RootID)
	}
	return nil
}

func (h *GenerationManager) validateCurrentMarker(repositoryName string, marker currentMarker) error {
	cleanRootValue, rootErr := cleanRoot(marker.Root)
	validRepository := marker.RootID != "" && path.Base(repositoryDirectory(marker.RootID)) == repositoryName
	if !validRepository || marker.Upstream != h.config.Upstream || rootErr != nil || cleanRootValue != marker.Root ||
		!validSHA256(marker.Generation) || !validCandidateID(marker.CandidateID) || !validSHA256(marker.SnapshotSHA256) {
		return errors.New("invalid current metadata marker")
	}
	return nil
}

func (h *GenerationManager) loadSnapshot(ctx context.Context, rootID, root, upstream string, reference snapshotReference) (*liveSnapshot, error) {
	if !validSHA256(reference.Generation) || !validCandidateID(reference.CandidateID) || !validSHA256(reference.SnapshotSHA256) {
		return nil, errors.New("invalid metadata snapshot reference")
	}
	snapshotPath := snapshotName(rootID, reference.Generation, reference.CandidateID)
	data, err := readStateFile(statePath(h.config.StateDir, snapshotPath), maxSnapshotStateSize)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if reference.SnapshotSHA256 != hex.EncodeToString(digest[:]) {
		return nil, errors.New("metadata snapshot digest mismatch")
	}
	var snapshot Snapshot
	if err := decodeJSON(data, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.RootID != rootID || snapshot.Root != root || snapshot.Generation != reference.Generation ||
		snapshot.CandidateID != reference.CandidateID || snapshot.Upstream != upstream {
		return nil, errors.New("metadata snapshot reference does not match snapshot")
	}
	if err := prepareSnapshot(&snapshot); err != nil {
		return nil, err
	}
	validatedKeys := make(map[string]struct{})
	for _, object := range snapshot.Objects {
		if _, validated := validatedKeys[object.Key]; validated {
			continue
		}
		info, err := h.config.Store.StatObject(ctx, h.config.Tenant, object.Key)
		if err != nil {
			return nil, fmt.Errorf("open metadata object %s: %w", object.Path, err)
		}
		if info.Size != object.Size {
			return nil, fmt.Errorf("metadata object %s failed validation", object.Path)
		}
		validatedKeys[object.Key] = struct{}{}
	}
	return &liveSnapshot{snapshot: &snapshot, snapshotSHA256: reference.SnapshotSHA256}, nil
}

func (h *GenerationManager) restorePendingAnchor(repositoryName string, seen lastSeenMarker, seenPresent bool) error {
	directory := "repositories/" + repositoryName
	var pending pendingAnchor
	if err := readJSON(h.config.StateDir, directory+"/pending.json", maxRepositoryMarkerSize, &pending); err != nil {
		return err
	}
	if pending.Upstream != h.config.Upstream {
		if err := os.Remove(statePath(h.config.StateDir, directory+"/pending.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove metadata pending marker for obsolete upstream: %w", err)
		}
		return nil
	}
	cleanRootValue, rootErr := cleanRoot(pending.Root)
	cleanAnchor, anchorErr := CleanPath(pending.Path)
	validRepository := pending.RootID != "" && path.Base(repositoryDirectory(pending.RootID)) == repositoryName
	validRoot := rootErr == nil && cleanRootValue == pending.Root
	validAnchor := anchorErr == nil && cleanAnchor == pending.Path && containsPath(pending.Root, pending.Path)
	expectedKey := candidatePrefix(pending.RootID, pending.Generation, pending.CandidateID) + "/anchor"
	if !validRepository || !validCandidateID(pending.CandidateID) || !validRoot || !validAnchor ||
		!validSHA256(pending.Generation) || pending.Key != expectedKey {
		return errors.New("invalid pending metadata state")
	}
	if current := h.current[pending.RootID]; current != nil && current.snapshot.CandidateID == pending.CandidateID {
		_ = os.Remove(statePath(h.config.StateDir, directory+"/pending.json"))
		return nil
	}
	info, err := h.config.Store.StatObject(context.Background(), h.config.Tenant, pending.Key)
	if err != nil || info.Size < 0 || info.Size > h.config.AnchorMaxBytes {
		return errors.New("pending metadata anchor is unavailable")
	}
	h.pending[pending.RootID] = pending
	if !seenPresent || seen.RootID != pending.RootID {
		if h.lastSeen[pending.RootID].IsZero() {
			h.lastSeen[pending.RootID] = time.Now().UTC()
		}
		delete(h.lastSeenPersisted, pending.RootID)
	}
	return nil
}

func validCandidateID(value string) bool {
	if len(value) != 32 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

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
