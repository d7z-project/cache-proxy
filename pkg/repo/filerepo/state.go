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
	"log/slog"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
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
	current.validatedAt, current.header = marker.ValidatedAt, marker.Header
	if current.validatedAt.After(time.Now()) {
		current.validatedAt = time.Time{}
	}
	current.nextCheck = h.nextCheckAt(marker.RootID, current.validatedAt, current.header)
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
