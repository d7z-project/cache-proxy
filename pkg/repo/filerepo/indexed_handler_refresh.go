package filerepo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func (h *IndexedHandler) RefreshRoot(ctx context.Context, rootID string) error {
	_, err := h.RefreshRootTask(ctx, rootID)
	return err
}

func (h *IndexedHandler) RefreshRootTask(ctx context.Context, rootID string) (*scheduler.TaskOutcome, error) {
	h.mu.RLock()
	entry, ok := h.roots[rootID]
	var root RepositoryRoot
	if ok && entry != nil {
		root = entry.root
		root.Targets = append([]MetadataTarget(nil), entry.root.Targets...)
		root.PrimaryMetadata = append([]string(nil), entry.root.PrimaryMetadata...)
		root.Attributes = append([]RepositoryAttribute(nil), entry.root.Attributes...)
		root.Components = append([]string(nil), entry.root.Components...)
		root.Architectures = append([]string(nil), entry.root.Architectures...)
	}
	h.mu.RUnlock()
	if !ok || len(root.Targets) == 0 {
		return nil, fmt.Errorf("root %s not found or has no targets", rootID)
	}
	if h.rootRetired(rootID) || h.rootNeedsRetirement(rootID, time.Now()) {
		if err := h.beginRootRetirement(ctx, rootID); err != nil {
			return nil, err
		}
		h.stats.RecordRepositoryMaintenance(h.name, h.mode, "root_retirement", "started", nil)
		return repositoryRefreshOutcome("retired", "root_inactive", "", ""), scheduler.ErrTaskSkipped
	}
	h.mu.Lock()
	if h.refreshing[rootID] {
		h.mu.Unlock()
		return nil, scheduler.ErrTaskSkipped
	}
	h.refreshing[rootID] = true
	delete(h.refreshErrors, rootID)
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.refreshing[rootID] = false
		h.mu.Unlock()
		h.reportMetadataState()
	}()
	h.reportMetadataState()
	upstreams := h.upstreams
	if len(upstreams) == 0 {
		return nil, errors.New("no upstreams available")
	}
	staging, hasStaging, err := h.loadRefreshStaging(ctx, rootID)
	if err != nil {
		return nil, err
	}
	if hasStaging {
		found := false
		ordered := []string{staging.Upstream}
		for _, upstream := range upstreams {
			if upstream == staging.Upstream {
				found = true
				continue
			}
			ordered = append(ordered, upstream)
		}
		if found {
			upstreams = ordered
		} else {
			if err := h.discardRefreshStaging(ctx, rootID, staging); err != nil {
				return nil, err
			}
			hasStaging = false
		}
	}
	generation := strconv.FormatInt(time.Now().UnixNano(), 36)
	if hasStaging {
		generation = staging.Generation
	}
	var firstErr error
	for _, upstream := range upstreams {
		if !hasStaging || staging.Upstream != upstream {
			if hasStaging {
				if err := h.discardRefreshStaging(ctx, rootID, staging); err != nil {
					return nil, err
				}
			}
			generation = strconv.FormatInt(time.Now().UnixNano(), 36)
			staging = refreshStagingState{RootID: rootID, Generation: generation, Upstream: upstream, CreatedAt: time.Now().UTC()}
			hasStaging = h.mode == config.ModeDEB
			if hasStaging {
				if err := h.saveRefreshStaging(ctx, staging); err != nil {
					return nil, err
				}
			}
		}
		attemptCtx := ctx
		cancelAttempt := func() {}
		if h.mode == config.ModeDEB {
			attemptCtx, cancelAttempt = context.WithTimeout(ctx, refreshSliceDuration)
		}
		snapshot, cleanupIndex, anchors, anchorDigest, transfers, err := h.buildSnapshot(
			attemptCtx, root, generation, upstream, staging.Anchors,
		)
		attemptDeadline := attemptCtx.Err()
		cancelAttempt()
		if err != nil {
			if errors.Is(err, errMetadataAnchorChanged) {
				if cleanupIndex != nil {
					_ = cleanupIndex.Close()
				}
				if discardErr := h.discardRefreshStaging(ctx, rootID, staging); discardErr != nil {
					return nil, errors.Join(err, discardErr)
				}
				h.stats.RecordRepositoryMaintenance(h.name, h.mode, "metadata_refresh", "anchor_changed", nil)
				return &scheduler.TaskOutcome{
					Result: "restarted", ReasonCode: "staging_anchor_changed",
					Detail: fmt.Sprintf("old=%s new=%s", staging.AnchorSetDigest, anchorDigest),
				}, scheduler.RetryAt(time.Now().Add(maintenanceContinuationDelay))
			}
			if h.mode == config.ModeDEB && ctx.Err() == nil &&
				(errors.Is(err, errMetadataRefreshContinuation) || errors.Is(attemptDeadline, context.DeadlineExceeded)) {
				if cleanupIndex != nil {
					fragment, fragmentErr := h.persistCleanupFragment(ctx, staging, cleanupIndex)
					_ = cleanupIndex.Close()
					if fragmentErr != nil {
						return nil, fragmentErr
					}
					staging.CleanupFragments = append(staging.CleanupFragments, fragment)
				}
				staging.Anchors = anchors
				staging.AnchorSetDigest = anchorDigest
				staging.EntryCursor += transfers
				staging.ParseCursor = len(staging.CleanupFragments)
				staging.Phase = "metadata_fetch"
				if saveErr := h.saveRefreshStaging(ctx, staging); saveErr != nil {
					return nil, saveErr
				}
				h.stats.RecordRepositoryMaintenance(h.name, h.mode, "metadata_refresh", "continuation", map[string]int{"transfers": transfers})
				return &scheduler.TaskOutcome{
					Result: "partial", ReasonCode: "staging_continuation",
					Detail: fmt.Sprintf("generation=%s upstream=%s transfers=%d", generation, upstream, transfers),
				}, scheduler.RetryAt(time.Now().Add(maintenanceContinuationDelay))
			}
			var limited *httpcache.UpstreamRateLimitError
			if errors.As(err, &limited) {
				if cleanupIndex != nil {
					_ = cleanupIndex.Close()
				}
				if hasStaging {
					staging.Anchors = anchors
					staging.AnchorSetDigest = anchorDigest
					if saveErr := h.saveRefreshStaging(ctx, staging); saveErr != nil {
						return nil, errors.Join(err, saveErr)
					}
				}
				if !limited.RetryAfter.IsZero() {
					return nil, scheduler.RetryAt(limited.RetryAfter)
				}
				return nil, err
			}
			if cleanupIndex != nil {
				_ = cleanupIndex.Close()
			}
			if cleanupErr := errors.Join(
				h.removeCandidateGeneration(rootID, generation),
				h.deleteRefreshStaging(ctx, rootID),
			); cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
			hasStaging = false
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !errors.Is(err, errMetadataMirrorRetry) {
				return nil, err
			}
			if firstErr == nil {
				firstErr = err
			}
			slog.Debug(
				"repository refresh failed on upstream",
				"instance", h.name,
				"root_id", rootID,
				"upstream", upstream,
				"err", err,
			)
			continue
		}
		if hasStaging {
			mergedIndex, mergeErr := h.mergeCleanupFragments(ctx, staging, cleanupIndex)
			_ = cleanupIndex.Close()
			if mergeErr != nil {
				return nil, errors.Join(mergeErr, h.discardRefreshStaging(ctx, rootID, staging))
			}
			cleanupIndex = mergedIndex
			staging.Anchors = anchors
			staging.AnchorSetDigest = anchorDigest
			staging.EntryCursor += transfers
			staging.ParseCursor = len(staging.CleanupFragments)
			staging.Phase = "commit"
			if err := h.saveRefreshStaging(ctx, staging); err != nil {
				_ = cleanupIndex.Close()
				return nil, err
			}
		}
		current := h.rootSnapshot(rootID)
		cleanupIndexExists := false
		if current != nil && current.RootID != "" && current.Generation != "" {
			_, err := h.store.StatObject(ctx, h.name, h.cleanupIndexPath(current.RootID, current.Generation))
			cleanupIndexExists = err == nil
		}
		if cleanupIndexExists && snapshotsMetadataEqual(current, snapshot) {
			if err := cleanupIndex.Close(); err != nil {
				return nil, fmt.Errorf("close unchanged cleanup index: %w", err)
			}
			if err := errors.Join(
				h.removeCandidateGeneration(rootID, generation),
				h.deleteRefreshStaging(ctx, rootID),
			); err != nil {
				return nil, err
			}
			outcome := h.completeSuccessfulRefresh(
				rootID, current, "unchanged", "same_as_current", upstream,
			)
			slog.Debug(
				"repository refresh skipped identical metadata",
				"instance", h.name,
				"mode", h.mode,
				"root_id", rootID,
				"upstream", upstream,
			)
			return outcome, nil
		}
		if err := h.publishSnapshot(ctx, snapshot, cleanupIndex); err != nil {
			_ = cleanupIndex.Close()
			if cleanupErr := errors.Join(
				h.removeCandidateGeneration(rootID, generation),
				h.deleteRefreshStaging(ctx, rootID),
			); cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		_ = cleanupIndex.Close()
		h.setRootSnapshot(rootID, snapshot)
		if err := h.deleteRefreshStaging(ctx, rootID); err != nil {
			return nil, err
		}
		outcome := h.completeSuccessfulRefresh(
			rootID, snapshot, "updated", "published", upstream,
		)
		h.stats.RecordRepositoryMaintenance(h.name, h.mode, "metadata_refresh", "published", map[string]int{"transfers": transfers})
		slog.Debug(
			"repository refresh succeeded",
			"instance", h.name,
			"mode", h.mode,
			"root_id", rootID,
			"upstream", upstream,
		)
		return outcome, nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	h.mu.Lock()
	h.refreshErrors[rootID] = firstErr.Error()
	h.mu.Unlock()
	h.saveState(context.Background())
	return nil, firstErr
}

func (h *IndexedHandler) completeSuccessfulRefresh(
	rootID string,
	snapshot *LiveSnapshot,
	result, reasonCode, upstream string,
) *scheduler.TaskOutcome {
	h.mu.Lock()
	delete(h.refreshErrors, rootID)
	h.mu.Unlock()
	h.saveState(context.Background())
	h.reportMetadataState()
	return repositoryRefreshOutcome(result, reasonCode, snapshot.Generation, upstream)
}

func (h *IndexedHandler) removeCandidateGeneration(rootID, generation string) error {
	if rootID == "" || generation == "" {
		return nil
	}
	rootDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID))
	var cleanupErr error
	for _, item := range []string{
		path.Join(rootDir, "generations", generation),
		path.Join(rootDir, "snapshots", generation+".yaml"),
		path.Join(rootDir, "current.yaml.tmp."+generation),
	} {
		if err := h.store.RemoveAll(path.Join(h.name, item)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove candidate metadata path %s: %w", item, err))
		}
	}
	return cleanupErr
}

func (h *IndexedHandler) buildSnapshot(
	ctx context.Context,
	root RepositoryRoot,
	generation, upstream string,
	expectedAnchors []MetadataAnchor,
) (*LiveSnapshot, *PathIndexBuilder, []MetadataAnchor, string, int, error) {
	expectedByPath := make(map[string]MetadataAnchor, len(expectedAnchors))
	for _, anchor := range expectedAnchors {
		expectedByPath[anchor.Path] = anchor
	}
	session := &RefreshSession{
		handler:         h,
		rootID:          root.ID,
		upstream:        upstream,
		generation:      generation,
		blobs:           map[string]*MetadataBlob{},
		targets:         append([]MetadataTarget(nil), root.Targets...),
		anchors:         map[string]MetadataAnchor{},
		expectedAnchors: expectedByPath,
	}
	if h.mode == config.ModeDEB {
		session.maxTransfers = refreshSliceTransfers
	}
	defer session.Close()
	indexBuilder := &PathIndexBuilder{}
	keepIndex := false
	defer func() {
		if !keepIndex {
			_ = indexBuilder.Close()
		}
	}()
	snapshot, err := h.build(ctx, session, indexBuilder)
	anchors, anchorDigest, _ := session.anchorSet()
	if err != nil {
		keepIndex = true
		return nil, indexBuilder, anchors, anchorDigest, session.transfers, err
	}
	if snapshot == nil {
		return nil, nil, anchors, anchorDigest, session.transfers, errors.New("metadata refresh produced no snapshot")
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]MetadataObject{}
	}
	snapshot.RootID = root.ID
	snapshot.RootPath = root.Path
	snapshot.Version = snapshotSchemaVersion
	snapshot.Generation = generation
	snapshot.Upstream = upstream
	snapshot.Published = time.Now().UTC()
	snapshot.Targets = append([]MetadataTarget(nil), root.Targets...)
	digests := map[string]string{}
	for pathKey, obj := range snapshot.Metadata {
		if obj.Path == "" {
			obj.Path = pathKey
		}
		if obj.State == "" {
			switch obj.StatusCode {
			case http.StatusForbidden:
				obj.State = MetadataForbidden
			case http.StatusNotFound:
				obj.State = MetadataNotFound
			default:
				obj.State = MetadataPresent
			}
		}
		if obj.State != MetadataPresent {
			if obj.State != MetadataForbidden && obj.State != MetadataNotFound {
				return nil, nil, anchors, anchorDigest, session.transfers, fmt.Errorf("%s: unsupported persisted metadata status %d", pathKey, obj.StatusCode)
			}
			obj.StatusCode = metadataStateStatus(obj.State)
			obj.StorePath = ""
			obj.Digest = ""
			obj.Size = 0
			obj.ChecksumType = ""
			obj.Checksum = ""
			snapshot.Metadata[pathKey] = obj
			continue
		}
		obj.StorePath = h.generationMetadataPath(root.ID, generation, obj.Path)
		info, err := h.store.StatObject(ctx, h.name, obj.StorePath)
		if err != nil {
			if obj.Required {
				return nil, nil, anchors, anchorDigest, session.transfers, fmt.Errorf("%s: required metadata missing", obj.Path)
			}
			snapshot.Metadata[pathKey] = obj
			continue
		}
		obj.Size = info.Size
		digest, ok := digests[obj.StorePath]
		if !ok {
			digest, err = h.metadataObjectDigest(ctx, obj.StorePath)
			if err != nil {
				return nil, nil, anchors, anchorDigest, session.transfers, fmt.Errorf("hash metadata %s: %w", obj.Path, err)
			}
			digests[obj.StorePath] = digest
		}
		obj.Digest = digest
		snapshot.Metadata[pathKey] = obj
	}
	anchors, anchorDigest, err = session.ConfirmAnchors(ctx)
	if err != nil {
		return nil, nil, anchors, anchorDigest, session.transfers, err
	}
	snapshot.Anchors = anchors
	snapshot.AnchorSetDigest = anchorDigest
	if err := h.validateSnapshot(ctx, snapshot, false); err != nil {
		return nil, nil, anchors, anchorDigest, session.transfers, err
	}
	if err := h.pruneUnreferencedGenerationMetadata(ctx, snapshot); err != nil {
		return nil, nil, anchors, anchorDigest, session.transfers, err
	}
	keepIndex = true
	return snapshot, indexBuilder, anchors, anchorDigest, session.transfers, nil
}

func (h *IndexedHandler) pruneUnreferencedGenerationMetadata(ctx context.Context, snapshot *LiveSnapshot) error {
	keep := map[string]struct{}{}
	for _, object := range snapshot.Metadata {
		if object.State == MetadataPresent {
			keep[h.generationMetadataPath(snapshot.RootID, snapshot.Generation, object.Path)] = struct{}{}
		}
	}
	dir := h.generationMetadataPath(snapshot.RootID, snapshot.Generation, "")
	err := fs.WalkDir(h.store.TenantFS(h.name), dir, func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		if _, ok := keep[objectPath]; ok {
			return nil
		}
		return h.store.DeleteObject(ctx, h.name, objectPath)
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (h *IndexedHandler) validateSnapshot(ctx context.Context, snapshot *LiveSnapshot, requireCleanupIndex bool) error {
	if snapshot == nil || snapshot.Version != snapshotSchemaVersion {
		return errors.New("unsupported metadata snapshot version")
	}
	if snapshot.RootID == "" || snapshot.Generation == "" || len(snapshot.Metadata) == 0 ||
		snapshot.Upstream == "" || len(snapshot.Anchors) == 0 || snapshot.AnchorSetDigest == "" {
		return errors.New("metadata snapshot identity or manifest is empty")
	}
	anchorDigest, err := metadataAnchorsDigest(snapshot.Anchors)
	if err != nil || anchorDigest != snapshot.AnchorSetDigest {
		return errors.New("metadata snapshot anchor set digest mismatch")
	}
	if requireCleanupIndex {
		if snapshot.CleanupIndexDigest == "" {
			return errors.New("metadata cleanup index digest is empty")
		}
		digest, err := h.metadataObjectDigest(ctx, h.cleanupIndexPath(snapshot.RootID, snapshot.Generation))
		if err != nil {
			return fmt.Errorf("metadata cleanup index missing: %w", err)
		}
		if digest != snapshot.CleanupIndexDigest {
			return errors.New("metadata cleanup index digest mismatch")
		}
	}

	required := 0
	digests := make(map[string]string, len(snapshot.Metadata))
	for cleanPath, obj := range snapshot.Metadata {
		if !httpcache.SafePath(cleanPath) || !httpcache.SafePath(obj.Path) {
			return fmt.Errorf("invalid metadata manifest path %q", cleanPath)
		}
		if obj.Required {
			required++
		}
		if obj.State != MetadataPresent {
			if obj.Required || obj.Path != cleanPath || obj.Digest != "" || obj.Size != 0 ||
				(obj.State != MetadataForbidden && obj.State != MetadataNotFound) {
				return fmt.Errorf("metadata %s has invalid negative manifest entry", cleanPath)
			}
			continue
		}
		if obj.Path != cleanPath {
			actual, ok := snapshot.Metadata[obj.Path]
			if !ok {
				return fmt.Errorf("metadata alias %s targets missing object %s", cleanPath, obj.Path)
			}
			if actual.Path != obj.Path {
				return fmt.Errorf("metadata alias %s targets non-canonical object %s", cleanPath, obj.Path)
			}
		}
		expectedPath := h.generationMetadataPath(snapshot.RootID, snapshot.Generation, obj.Path)
		if obj.Digest == "" {
			return fmt.Errorf("metadata %s has no persisted digest", cleanPath)
		}
		info, err := h.store.StatObject(ctx, h.name, expectedPath)
		if err != nil {
			return fmt.Errorf("validate metadata %s: %w", cleanPath, err)
		}
		if info.Size != obj.Size {
			return fmt.Errorf("metadata %s persisted size mismatch", cleanPath)
		}
		digest, ok := digests[expectedPath]
		if !ok {
			var err error
			digest, err = h.metadataObjectDigest(ctx, expectedPath)
			if err != nil {
				return fmt.Errorf("validate metadata %s: %w", cleanPath, err)
			}
			digests[expectedPath] = digest
		}
		if digest != obj.Digest {
			return fmt.Errorf("metadata %s persisted digest mismatch", cleanPath)
		}
	}
	if required == 0 {
		return errors.New("metadata snapshot has no required objects")
	}
	if h.validator == nil {
		return nil
	}
	opener := func(cleanPath string) (io.ReadCloser, error) {
		obj, ok := snapshot.Metadata[cleanPath]
		if !ok {
			return nil, fmt.Errorf("metadata %s is absent from manifest", cleanPath)
		}
		if obj.Path != cleanPath {
			obj, ok = snapshot.Metadata[obj.Path]
			if !ok {
				return nil, fmt.Errorf("metadata alias %s target is absent", cleanPath)
			}
		}
		return h.store.OpenObject(ctx, h.name, h.generationMetadataPath(snapshot.RootID, snapshot.Generation, obj.Path))
	}
	if err := h.validator.ValidateSnapshot(ctx, snapshot, opener); err != nil {
		return fmt.Errorf("validate protocol metadata manifest: %w", err)
	}
	return nil
}

func (h *IndexedHandler) metadataObjectDigest(ctx context.Context, objectPath string) (string, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

func snapshotsMetadataEqual(current, next *LiveSnapshot) bool {
	if current == nil || next == nil || len(current.Metadata) == 0 || len(current.Metadata) != len(next.Metadata) {
		return false
	}
	for key, nextObject := range next.Metadata {
		currentObject, ok := current.Metadata[key]
		if !ok ||
			(currentObject.State == MetadataPresent && currentObject.Digest == "") ||
			(nextObject.State == MetadataPresent && nextObject.Digest == "") {
			return false
		}
		if currentObject.Path != nextObject.Path ||
			currentObject.Required != nextObject.Required ||
			currentObject.State != nextObject.State ||
			currentObject.Digest != nextObject.Digest ||
			currentObject.Size != nextObject.Size ||
			currentObject.ChecksumType != nextObject.ChecksumType ||
			currentObject.Checksum != nextObject.Checksum {
			return false
		}
	}
	return true
}

func metadataStateStatus(state MetadataObjectState) int {
	switch state {
	case MetadataNotFound:
		return http.StatusNotFound
	case MetadataForbidden:
		return http.StatusForbidden
	default:
		return 0
	}
}

func repositoryRefreshOutcome(result, reasonCode, generation, upstream string) *scheduler.TaskOutcome {
	return &scheduler.TaskOutcome{
		Result:     result,
		ReasonCode: reasonCode,
		Detail:     fmt.Sprintf("generation=%s upstream=%s", generation, upstream),
	}
}
