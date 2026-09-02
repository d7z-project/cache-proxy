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
		temporary.Close()
		_ = os.Remove(temporaryName)
		return nil, err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
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

func readJSON(root, name string, limit int64, target any) ([]byte, error) {
	data, err := readStateFile(statePath(root, name), limit)
	if err != nil {
		return nil, err
	}
	return data, decodeJSON(data, target)
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
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

func readYAML(root, name string, target any) ([]byte, error) {
	data, err := readStateFile(statePath(root, name), maxCurrentMarkerSize)
	if err != nil {
		return nil, err
	}
	return data, decodeYAML(data, target)
}

func readStateFile(name string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("state file limit must be positive")
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
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
	if err := decoder.Decode(&trailing); err != io.EOF {
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
	if snapshot.Version != StateVersion || snapshot.RootID == "" || rootErr != nil || cleanRootValue != snapshot.Root || anchorErr != nil || cleanAnchor != snapshot.Anchor || !containsPath(snapshot.Root, snapshot.Anchor) || !validSHA256(snapshot.Generation) || upstreamErr != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" || snapshot.PublishedAt.IsZero() {
		return errors.New("invalid metadata snapshot")
	}
	generationRoot := candidatePrefix(snapshot.RootID, snapshot.Generation, snapshot.CandidateID)
	if !validCandidateID(snapshot.CandidateID) {
		return errors.New("invalid metadata candidate ID")
	}
	snapshot.byPath = make(map[string]Object, len(snapshot.Objects))
	for _, object := range snapshot.Objects {
		cleaned, err := CleanPath(object.Path)
		if err != nil || cleaned != object.Path || !containsPath(snapshot.Root, object.Path) {
			return fmt.Errorf("invalid snapshot object path %q", object.Path)
		}
		if _, duplicate := snapshot.byPath[object.Path]; duplicate {
			return fmt.Errorf("duplicate snapshot object %q", object.Path)
		}
		switch object.State {
		case ObjectPresent:
			validKey := strings.HasPrefix(object.Key, generationRoot+"/objects/") || object.Path == snapshot.Anchor && object.Key == generationRoot+"/anchor" && strings.EqualFold(object.SHA256, snapshot.Generation)
			if object.Size < 0 || !validSHA256(object.SHA256) || !validKey {
				return fmt.Errorf("invalid present snapshot object %q", object.Path)
			}
		case ObjectNotFound, ObjectForbidden:
			if object.Key != "" {
				return fmt.Errorf("invalid absent snapshot object %q", object.Path)
			}
		default:
			return fmt.Errorf("invalid snapshot object state %q", object.State)
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
		if _, err := readJSON(h.config.StateDir, directory+"/last-seen.json", maxRepositoryMarkerSize, &seen); err == nil {
			if seen.Version == StateVersion && seen.RootID != "" && path.Base(repositoryDirectory(seen.RootID)) == entry.Name() && !seen.SeenAt.IsZero() {
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
	if _, err := readYAML(h.config.StateDir, directory+"/current.yaml", &marker); err != nil {
		return err
	}
	cleanRootValue, rootErr := cleanRoot(marker.Root)
	upstream, upstreamErr := url.Parse(marker.Upstream)
	if marker.Version != StateVersion || marker.RootID == "" || path.Base(repositoryDirectory(marker.RootID)) != repositoryName || rootErr != nil || cleanRootValue != marker.Root || !validSHA256(marker.Generation) || !validCandidateID(marker.CandidateID) || !validSHA256(marker.SnapshotSHA256) || upstreamErr != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" {
		return errors.New("invalid current metadata marker")
	}
	snapshotPath := snapshotName(marker.RootID, marker.Generation, marker.CandidateID)
	data, err := readStateFile(statePath(h.config.StateDir, snapshotPath), maxSnapshotStateSize)
	if err != nil {
		return fmt.Errorf("load current metadata snapshot: %w", err)
	}
	digest := sha256.Sum256(data)
	if !strings.EqualFold(marker.SnapshotSHA256, hex.EncodeToString(digest[:])) {
		return errors.New("current metadata snapshot digest mismatch")
	}
	var snapshot Snapshot
	if err := decodeJSON(data, &snapshot); err != nil {
		return err
	}
	if snapshot.RootID != marker.RootID || snapshot.Root != marker.Root || snapshot.Generation != marker.Generation || snapshot.CandidateID != marker.CandidateID || snapshot.Upstream != marker.Upstream {
		return errors.New("current metadata marker does not match snapshot")
	}
	if err := prepareSnapshot(&snapshot); err != nil {
		return err
	}
	validatedKeys := make(map[string]struct{})
	for _, object := range snapshot.Objects {
		if object.State != ObjectPresent {
			continue
		}
		if _, validated := validatedKeys[object.Key]; validated {
			continue
		}
		info, err := h.config.Store.StatObject(context.Background(), h.config.Tenant, object.Key)
		if err != nil {
			return fmt.Errorf("open current metadata object %s: %w", object.Path, err)
		}
		if info.Size != object.Size {
			return fmt.Errorf("current metadata object %s failed validation", object.Path)
		}
		validatedKeys[object.Key] = struct{}{}
	}
	h.current[snapshot.RootID] = &liveSnapshot{snapshot: &snapshot}
	if !seenPresent || seen.RootID != snapshot.RootID {
		h.lastSeen[snapshot.RootID] = snapshot.PublishedAt
		delete(h.lastSeenPersisted, snapshot.RootID)
	}
	return nil
}

func (h *GenerationManager) restorePendingAnchor(repositoryName string, seen lastSeenMarker, seenPresent bool) error {
	directory := "repositories/" + repositoryName
	var pending pendingAnchor
	if _, err := readJSON(h.config.StateDir, directory+"/pending.json", maxRepositoryMarkerSize, &pending); err != nil {
		return err
	}
	cleanRootValue, rootErr := cleanRoot(pending.Root)
	cleanAnchor, anchorErr := CleanPath(pending.Path)
	upstream, upstreamErr := url.Parse(pending.Upstream)
	if pending.Version != StateVersion || pending.RootID == "" || !validCandidateID(pending.CandidateID) || path.Base(repositoryDirectory(pending.RootID)) != repositoryName || rootErr != nil || cleanRootValue != pending.Root || anchorErr != nil || cleanAnchor != pending.Path || !containsPath(pending.Root, pending.Path) || !validSHA256(pending.Generation) || pending.Key != candidatePrefix(pending.RootID, pending.Generation, pending.CandidateID)+"/anchor" || upstreamErr != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" {
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
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	if h.gcPhase == generationGCIdle {
		h.gcPhase = generationGCRetire
		h.gcRetained = make(map[string]bool)
		h.gcProtected = make(map[string]bool)
	}
	switch h.gcPhase {
	case generationGCRetire:
		return h.retireInactiveRoots(ctx, limit, now)
	case generationGCScan:
		return h.scanGenerationCandidates(ctx, limit, now)
	case generationGCDelete:
		return h.deleteGenerationCandidates(ctx, limit)
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
		current := h.current[rootID]
		activeReader := current != nil && h.readers[rootID+"\x00"+current.snapshot.CandidateID] != 0
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
		current = h.current[rootID]
		activeReader = current != nil && h.readers[rootID+"\x00"+current.snapshot.CandidateID] != 0
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
		delete(h.pending, rootID)
		delete(h.lastSeen, rootID)
		delete(h.lastSeenPersisted, rootID)
		delete(h.retryWindows, rootID)
		delete(h.retiring, rootID)
		h.mu.Unlock()
		h.commitMu.Unlock()
		if err := removeEmptyStateParents(statePath(h.config.StateDir, currentName(rootID)), statePath(h.config.StateDir, "repositories")); err != nil {
			return false, err
		}
		h.gcCursor = rootID
	}
	h.gcPhase, h.gcCursor = generationGCScan, ""
	return true, nil
}

func (h *GenerationManager) scanGenerationCandidates(ctx context.Context, limit int, now time.Time) (bool, error) {
	inspected := 0
	complete := true
	err := filepath.WalkDir(statePath(h.config.StateDir, "repositories"), func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || name <= h.gcCursor || entry.Name() != "snapshot.json" && entry.Name() != "current.yaml" {
			return nil
		}
		if inspected >= limit {
			complete = false
			return fs.SkipAll
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		inspected++
		if entry.Name() == "current.yaml" {
			var marker currentMarker
			if _, err := readYAML("/", name, &marker); err != nil || marker.Version != StateVersion || !validSHA256(marker.Generation) || !validCandidateID(marker.CandidateID) || !validSHA256(marker.SnapshotSHA256) || path.Base(repositoryDirectory(marker.RootID)) != filepath.Base(filepath.Dir(name)) {
				slog.Warn("invalid current metadata marker ignored during GC", "path", name, "err", err)
				h.gcCursor = name
				return nil
			}
			identity := marker.RootID + "\x00" + marker.CandidateID
			h.gcProtected[identity] = true
			h.gcRetained[candidatePrefix(marker.RootID, marker.Generation, marker.CandidateID)] = true
			h.gcCursor = name
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		prefix, pathErr := candidateStatePrefix(h.config.StateDir, name)
		var snapshot Snapshot
		_, snapshotErr := readJSON("/", name, maxSnapshotStateSize, &snapshot)
		if snapshotErr == nil {
			snapshotErr = prepareSnapshot(&snapshot)
		}
		if snapshotErr == nil {
			snapshotErr = pathErr
		}
		if snapshotErr == nil && (filepath.Clean(name) != statePath(h.config.StateDir, snapshotName(snapshot.RootID, snapshot.Generation, snapshot.CandidateID)) || prefix != candidatePrefix(snapshot.RootID, snapshot.Generation, snapshot.CandidateID)) {
			snapshotErr = errors.New("snapshot path does not match its identity")
		}
		if snapshotErr != nil {
			slog.Warn("invalid metadata snapshot scheduled for GC", "path", name, "err", snapshotErr)
			if pathErr == nil && now.Sub(info.ModTime()) >= h.config.GracePeriod && !h.gcRetained[prefix] {
				h.gcCandidates = append(h.gcCandidates, gcCandidate{name: name, modified: info.ModTime(), prefix: prefix})
			}
			h.gcCursor = name
			return nil
		}
		keys := make(map[string]struct{})
		for _, object := range snapshot.Objects {
			if object.Key != "" {
				keys[object.Key] = struct{}{}
			}
		}
		candidate := gcCandidate{name: name, modified: info.ModTime(), prefix: prefix, snapshot: snapshot, keys: make([]string, 0, len(keys))}
		for key := range keys {
			candidate.keys = append(candidate.keys, key)
		}
		sort.Strings(candidate.keys)
		h.gcCandidates = append(h.gcCandidates, candidate)
		h.gcCursor = name
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil || !complete {
		return !complete, err
	}

	h.mu.RLock()
	protected := make(map[string]bool, len(h.gcProtected)+len(h.current)+len(h.pending)+len(h.readers))
	for identity := range h.gcProtected {
		protected[identity] = true
	}
	activeRoots := make(map[string]bool, len(h.current)+len(h.pending))
	for rootID, current := range h.current {
		protected[rootID+"\x00"+current.snapshot.CandidateID] = true
		h.gcRetained[candidatePrefix(rootID, current.snapshot.Generation, current.snapshot.CandidateID)] = true
		activeRoots[rootID] = true
	}
	for rootID, pending := range h.pending {
		protected[rootID+"\x00"+pending.CandidateID] = true
		h.gcRetained[candidatePrefix(rootID, pending.Generation, pending.CandidateID)] = true
		activeRoots[rootID] = true
	}
	for readerKey := range h.readers {
		protected[readerKey] = true
	}
	h.mu.RUnlock()
	sort.Slice(h.gcCandidates, func(i, j int) bool { return h.gcCandidates[i].modified.After(h.gcCandidates[j].modified) })
	kept := make(map[string]int)
	deletable := h.gcCandidates[:0]
	for _, candidate := range h.gcCandidates {
		if candidate.snapshot.RootID == "" {
			if !h.gcRetained[candidate.prefix] && now.Sub(candidate.modified) >= h.config.GracePeriod {
				deletable = append(deletable, candidate)
			}
			continue
		}
		identity := candidate.snapshot.RootID + "\x00" + candidate.snapshot.CandidateID
		prefix := candidate.prefix
		if protected[identity] || now.Sub(candidate.modified) < h.config.GracePeriod || activeRoots[candidate.snapshot.RootID] && kept[candidate.snapshot.RootID] < h.config.KeepPrevious {
			if !protected[identity] && activeRoots[candidate.snapshot.RootID] {
				kept[candidate.snapshot.RootID]++
			}
			h.gcRetained[prefix] = true
			continue
		}
		deletable = append(deletable, candidate)
	}
	h.gcCandidates = deletable
	h.gcPhase, h.gcCursor = generationGCDelete, ""
	return true, nil
}

func (h *GenerationManager) deleteGenerationCandidates(ctx context.Context, limit int) (bool, error) {
	inspected := 0
	for h.gcCandidateIndex < len(h.gcCandidates) {
		candidate := &h.gcCandidates[h.gcCandidateIndex]
		h.mu.RLock()
		current := h.current[candidate.snapshot.RootID]
		pending := h.pending[candidate.snapshot.RootID]
		active := h.gcRetained[candidate.prefix] || candidate.snapshot.RootID != "" && (current != nil && current.snapshot.CandidateID == candidate.snapshot.CandidateID || pending.CandidateID == candidate.snapshot.CandidateID || h.readers[candidate.snapshot.RootID+"\x00"+candidate.snapshot.CandidateID] != 0)
		h.mu.RUnlock()
		if active {
			h.gcRetained[candidate.prefix] = true
			h.gcCandidateIndex++
			h.gcObjectIndex = 0
			continue
		}
		for h.gcObjectIndex < len(candidate.keys) {
			if inspected >= limit {
				return true, nil
			}
			if err := ctx.Err(); err != nil {
				return false, err
			}
			key := candidate.keys[h.gcObjectIndex]
			if err := h.config.Store.DeleteObject(ctx, h.config.Tenant, key); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return false, err
			}
			h.gcObjectIndex++
			inspected++
		}
		if inspected >= limit {
			return true, nil
		}
		if err := os.Remove(candidate.name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if err := removeEmptyStateParents(candidate.name, statePath(h.config.StateDir, "repositories")); err != nil {
			return false, err
		}
		inspected++
		h.gcCandidateIndex++
		h.gcObjectIndex = 0
	}
	h.gcPhase, h.gcCursor = generationGCBlobs, ""
	return true, nil
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
		if inspected >= limit {
			complete = false
			return fs.SkipAll
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		inspected++
		parts := strings.Split(objectPath, "/")
		if len(parts) < 5 {
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
		h.mu.RLock()
		active := false
		for rootID, current := range h.current {
			if candidatePrefix(rootID, current.snapshot.Generation, current.snapshot.CandidateID) == prefix {
				active = true
				break
			}
		}
		if !active {
			for rootID, pending := range h.pending {
				if candidatePrefix(rootID, pending.Generation, pending.CandidateID) == prefix {
					active = true
					break
				}
			}
		}
		h.mu.RUnlock()
		if !active {
			err = h.config.Store.DeleteObject(ctx, h.config.Tenant, objectPath)
		}
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
	h.gcPhase, h.gcCursor, h.gcCandidates, h.gcRetained, h.gcProtected = generationGCIdle, "", nil, nil, nil
	h.gcCandidateIndex, h.gcObjectIndex = 0, 0
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
