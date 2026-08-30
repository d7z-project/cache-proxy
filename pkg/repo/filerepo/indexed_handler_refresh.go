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
	"gopkg.d7z.net/cache-proxy/pkg/health"
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
	targets := append([]MetadataTarget(nil), root.Targets...)
	var (
		refreshGen uint64
		release    func()
	)
	if h.serviceHealth != nil {
		rh, done, err := h.serviceHealth.TryStartRefresh(rootID, time.Now())
		if err != nil {
			switch {
			case errors.Is(err, health.ErrRefreshAlreadyRunning):
				return nil, scheduler.ErrTaskSkipped
			case errors.Is(err, health.ErrRefreshBlocked):
				return nil, scheduler.ErrTaskSkipped
			case errors.Is(err, health.ErrRefreshResourceRemoved):
				h.removeRoot(rootID)
				h.saveState(context.Background())
				return nil, scheduler.ErrTaskSkipped
			}
			return nil, fmt.Errorf("start refresh %s: %w", rootID, err)
		}
		refreshGen = rh.Generation
		release = done
		defer func() {
			release()
			h.reportMetadataState()
		}()
	} else {
		defer h.reportMetadataState()
	}
	h.reportMetadataState()
	upstreams := h.orderedUpstreams()
	if len(upstreams) == 0 {
		return nil, errors.New("no upstreams available")
	}
	staging, hasStaging := h.loadRefreshStaging(ctx, rootID)
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
			h.discardRefreshStaging(ctx, staging)
			hasStaging = false
		}
	}
	generation := strconv.FormatInt(time.Now().UnixNano(), 36)
	if hasStaging {
		generation = staging.Generation
	}
	var firstErr error
	for _, upstream := range upstreams {
		if current := h.rootSnapshot(rootID); current != nil && current.Upstream == upstream {
			unchanged, err := h.canSkipRefresh(ctx, current, upstream, targets)
			if err != nil {
				var limited *httpcache.UpstreamRateLimitError
				if errors.As(err, &limited) {
					if !limited.RetryAfter.IsZero() {
						return nil, scheduler.RetryAt(limited.RetryAfter)
					}
					return nil, err
				}
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
					"repository refresh head check failed",
					"instance", h.name,
					"root_id", rootID,
					"upstream", upstream,
					"err", err,
				)
				continue
			}
			if unchanged {
				if hasStaging {
					h.discardRefreshStaging(ctx, staging)
				}
				outcome := h.completeSuccessfulRefresh(
					rootID, refreshGen, current, "unchanged", "same_as_current", upstream,
				)
				slog.Debug(
					"repository refresh skipped unchanged metadata",
					"instance", h.name,
					"mode", h.mode,
					"root_id", rootID,
					"upstream", upstream,
				)
				return outcome, nil
			}
		}
		if !hasStaging || staging.Upstream != upstream {
			if hasStaging {
				h.discardRefreshStaging(ctx, staging)
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
		snapshot, cleanupPaths, anchorPath, anchorDigest, transfers, err := h.buildSnapshot(
			attemptCtx, root, generation, upstream, staging.AnchorPath, staging.AnchorDigest,
		)
		attemptDeadline := attemptCtx.Err()
		cancelAttempt()
		if err != nil {
			if errors.Is(err, errMetadataAnchorChanged) {
				h.discardRefreshStaging(ctx, staging)
				h.stats.RecordRepositoryMaintenance(h.name, h.mode, "metadata_refresh", "anchor_changed", nil)
				return &scheduler.TaskOutcome{
					Result: "restarted", ReasonCode: "staging_anchor_changed",
					Detail: fmt.Sprintf("old=%s new=%s", staging.AnchorDigest, anchorDigest),
				}, scheduler.RetryAt(time.Now().Add(maintenanceContinuationDelay))
			}
			if h.mode == config.ModeDEB && ctx.Err() == nil &&
				(errors.Is(err, errMetadataRefreshContinuation) || errors.Is(attemptDeadline, context.DeadlineExceeded)) {
				staging.AnchorPath = anchorPath
				staging.AnchorDigest = anchorDigest
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
				if hasStaging {
					staging.AnchorPath = anchorPath
					staging.AnchorDigest = anchorDigest
					_ = h.saveRefreshStaging(ctx, staging)
				}
				if !limited.RetryAfter.IsZero() {
					return nil, scheduler.RetryAt(limited.RetryAfter)
				}
				return nil, err
			}
			h.cleanupFailedGeneration(rootID, generation)
			_ = h.deleteRefreshStaging(ctx, rootID)
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
		current := h.rootSnapshot(rootID)
		cleanupIndexExists := false
		if current != nil && current.RootID != "" && current.Generation != "" {
			_, err := h.store.StatObject(ctx, h.name, h.cleanupIndexPath(current.RootID, current.Generation))
			cleanupIndexExists = err == nil
		}
		if cleanupIndexExists && snapshotsMetadataEqual(current, snapshot) {
			h.cleanupFailedGeneration(rootID, generation)
			_ = h.deleteRefreshStaging(ctx, rootID)
			outcome := h.completeSuccessfulRefresh(
				rootID, refreshGen, current, "unchanged", "same_as_current", upstream,
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
		if err := h.publishSnapshot(ctx, snapshot, cleanupPaths); err != nil {
			h.cleanupFailedGeneration(rootID, generation)
			_ = h.deleteRefreshStaging(ctx, rootID)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		h.setRootSnapshot(rootID, snapshot)
		_ = h.deleteRefreshStaging(ctx, rootID)
		outcome := h.completeSuccessfulRefresh(
			rootID, refreshGen, snapshot, "updated", "published", upstream,
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
	if h.serviceHealth != nil {
		h.serviceHealth.FinishRefresh(rootID, refreshGen, refreshHealthError(firstErr), nil)
		if _, ok := h.serviceHealth.ResourceHealth(rootID); !ok {
			h.removeRoot(rootID)
		}
	}
	h.saveState(context.Background())
	return nil, firstErr
}

func (h *IndexedHandler) completeSuccessfulRefresh(
	rootID string,
	refreshGeneration uint64,
	snapshot *LiveSnapshot,
	result, reasonCode, upstream string,
) *scheduler.TaskOutcome {
	if h.serviceHealth != nil {
		h.serviceHealth.FinishRefresh(
			rootID,
			refreshGeneration,
			nil,
			targetsToResourceTargets(snapshot.Targets),
		)
	}
	h.saveState(context.Background())
	h.reportMetadataState()
	return repositoryRefreshOutcome(result, reasonCode, snapshot.Generation, upstream)
}

func (h *IndexedHandler) cleanupFailedGeneration(rootID, generation string) {
	if rootID == "" || generation == "" {
		return
	}
	rootDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID))
	for _, item := range []string{
		path.Join(rootDir, "generations", generation),
		path.Join(rootDir, "snapshots", generation+".yaml"),
		path.Join(rootDir, "current.yaml.tmp."+generation),
	} {
		if err := h.store.RemoveAll(path.Join(h.name, item)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			slog.Debug(
				"cleanup failed metadata generation failed",
				"instance", h.name,
				"root_id", rootID,
				"path", item,
				"err", err,
			)
		}
	}
}

func (h *IndexedHandler) buildSnapshot(
	ctx context.Context,
	root RepositoryRoot,
	generation, upstream, expectedAnchorPath, expectedAnchorDigest string,
) (*LiveSnapshot, []string, string, string, int, error) {
	session := &RefreshSession{
		handler:    h,
		rootID:     root.ID,
		upstream:   upstream,
		generation: generation,
		blobs:      map[string]*MetadataBlob{},
		targets:    append([]MetadataTarget(nil), root.Targets...),
	}
	if h.mode == config.ModeDEB {
		session.maxTransfers = refreshSliceTransfers
		session.expectedAnchorPath = expectedAnchorPath
		session.expectedAnchorDigest = expectedAnchorDigest
	}
	defer session.Close()
	indexBuilder := &PathIndexBuilder{}
	snapshot, err := h.build(ctx, session, indexBuilder)
	anchorPath, anchorDigest := session.anchorPath, session.anchorDigest
	if err != nil {
		return nil, nil, anchorPath, anchorDigest, session.transfers, err
	}
	if snapshot == nil {
		return nil, nil, anchorPath, anchorDigest, session.transfers, errors.New("metadata refresh produced no snapshot")
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
	cleanupPaths := indexBuilder.Finalize()
	digests := map[string]string{}
	for pathKey, obj := range snapshot.Metadata {
		if obj.Path == "" {
			obj.Path = pathKey
		}
		if obj.StatusCode != 0 {
			if obj.StatusCode != http.StatusForbidden && obj.StatusCode != http.StatusNotFound {
				return nil, nil, anchorPath, anchorDigest, session.transfers, fmt.Errorf("%s: unsupported persisted metadata status %d", pathKey, obj.StatusCode)
			}
			snapshot.Metadata[pathKey] = obj
			continue
		}
		obj.StorePath = h.generationMetadataPath(root.ID, generation, obj.Path)
		info, err := h.store.StatObject(ctx, h.name, obj.StorePath)
		if err != nil {
			if obj.Required {
				return nil, nil, anchorPath, anchorDigest, session.transfers, fmt.Errorf("%s: required metadata missing", obj.Path)
			}
			snapshot.Metadata[pathKey] = obj
			continue
		}
		obj.Size = info.Size
		digest, ok := digests[obj.StorePath]
		if !ok {
			digest, err = h.metadataObjectDigest(ctx, obj.StorePath)
			if err != nil {
				return nil, nil, anchorPath, anchorDigest, session.transfers, fmt.Errorf("hash metadata %s: %w", obj.Path, err)
			}
			digests[obj.StorePath] = digest
		}
		obj.Digest = digest
		snapshot.Metadata[pathKey] = obj
	}
	if err := h.validateSnapshot(ctx, snapshot, false); err != nil {
		return nil, nil, anchorPath, anchorDigest, session.transfers, err
	}
	if err := h.pruneUnreferencedGenerationMetadata(ctx, snapshot); err != nil {
		return nil, nil, anchorPath, anchorDigest, session.transfers, err
	}
	return snapshot, cleanupPaths, anchorPath, anchorDigest, session.transfers, nil
}

func (h *IndexedHandler) pruneUnreferencedGenerationMetadata(ctx context.Context, snapshot *LiveSnapshot) error {
	keep := map[string]struct{}{}
	for _, object := range snapshot.Metadata {
		if object.StatusCode == 0 && object.StorePath != "" {
			keep[object.StorePath] = struct{}{}
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
	if snapshot.RootID == "" || snapshot.Generation == "" || len(snapshot.Metadata) == 0 {
		return errors.New("metadata snapshot identity or manifest is empty")
	}
	if requireCleanupIndex {
		if _, err := h.store.StatObject(ctx, h.name, h.cleanupIndexPath(snapshot.RootID, snapshot.Generation)); err != nil {
			return fmt.Errorf("metadata cleanup index missing: %w", err)
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
		if obj.StatusCode != 0 {
			if obj.Required || obj.Path != cleanPath || obj.StorePath != "" || obj.Digest != "" || obj.Size != 0 ||
				(obj.StatusCode != http.StatusForbidden && obj.StatusCode != http.StatusNotFound) {
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
		if obj.StorePath != expectedPath {
			return fmt.Errorf("metadata %s has invalid generation store path", cleanPath)
		}
		if obj.Digest == "" {
			return fmt.Errorf("metadata %s has no persisted digest", cleanPath)
		}
		info, err := h.store.StatObject(ctx, h.name, obj.StorePath)
		if err != nil {
			return fmt.Errorf("validate metadata %s: %w", cleanPath, err)
		}
		if obj.Size > 0 && info.Size != obj.Size {
			return fmt.Errorf("metadata %s persisted size mismatch", cleanPath)
		}
		digest, ok := digests[obj.StorePath]
		if !ok {
			var err error
			digest, err = h.metadataObjectDigest(ctx, obj.StorePath)
			if err != nil {
				return fmt.Errorf("validate metadata %s: %w", cleanPath, err)
			}
			digests[obj.StorePath] = digest
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
		return h.store.OpenObject(ctx, h.name, obj.StorePath)
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
		if !ok || currentObject.Digest == "" || nextObject.Digest == "" {
			return false
		}
		if currentObject.Path != nextObject.Path ||
			currentObject.Required != nextObject.Required ||
			currentObject.StatusCode != nextObject.StatusCode ||
			currentObject.Digest != nextObject.Digest ||
			currentObject.Size != nextObject.Size ||
			currentObject.ChecksumType != nextObject.ChecksumType ||
			currentObject.Checksum != nextObject.Checksum {
			return false
		}
	}
	return true
}

func repositoryRefreshOutcome(result, reasonCode, generation, upstream string) *scheduler.TaskOutcome {
	return &scheduler.TaskOutcome{
		Result:     result,
		ReasonCode: reasonCode,
		Detail:     fmt.Sprintf("generation=%s upstream=%s", generation, upstream),
	}
}
