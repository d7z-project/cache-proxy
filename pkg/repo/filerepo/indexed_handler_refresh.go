package filerepo

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func (h *IndexedHandler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	deleted := 0
	return fs.WalkDir(h.store.TenantFS(h.name), h.objectRoot, func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil || entry.IsDir() || strings.Contains(objectPath, "/.roots/") {
			return nil
		}
		if opts.DryRun {
			deleted++
			slog.Info("indexed cleanup dry-run delete", "instance", h.name, "path", objectPath)
			return nil
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("indexed cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		} else {
			deleted++
		}
		return nil
	})
}

func (h *IndexedHandler) RefreshRoot(ctx context.Context, rootID string) error {
	_, err := h.RefreshRootTask(ctx, rootID)
	return err
}

func (h *IndexedHandler) RefreshRootTask(ctx context.Context, rootID string) (*scheduler.TaskOutcome, error) {
	h.mu.RLock()
	entry, ok := h.roots[rootID]
	h.mu.RUnlock()
	if !ok || entry == nil || len(entry.root.Targets) == 0 {
		return nil, fmt.Errorf("root %s not found or has no targets", rootID)
	}
	targets := append([]MetadataTarget(nil), entry.root.Targets...)
	var (
		refreshGen uint64
		release    func()
	)
	if h.sh != nil {
		rh, done, err := h.sh.TryStartRefresh(rootID, time.Now())
		if err != nil {
			switch {
			case errors.Is(err, health.ErrRefreshAlreadyRunning):
				return nil, scheduler.ErrTaskSkipped
			case errors.Is(err, health.ErrRefreshBlockedUntil):
				if blockedUntil, ok := h.sh.RefreshBlockedUntil(rootID); ok && !blockedUntil.IsZero() {
					return nil, scheduler.RetryAt(blockedUntil)
				}
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
	upstreams := h.weightedUpstreams()
	if len(upstreams) == 0 {
		return nil, errors.New("no upstreams available")
	}
	generation := strconv.FormatInt(time.Now().UnixNano(), 36)
	var firstErr error
	for _, upstream := range upstreams {
		if current := h.rootSnapshot(rootID); current != nil && current.Upstream == upstream {
			unchanged, err := h.canSkipRefresh(ctx, current, upstream, targets)
			if err != nil {
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
				if h.sh != nil {
					h.sh.FinishRefresh(rootID, refreshGen, nil, targetsToProbe(current.Targets))
				}
				h.saveState(context.Background())
				h.reportMetadataState()
				slog.Debug(
					"repository refresh skipped unchanged metadata",
					"instance", h.name,
					"mode", h.mode,
					"root_id", rootID,
					"upstream", upstream,
				)
				return repositoryRefreshOutcome("unchanged", "same_as_current", current.Generation, upstream), nil
			}
		}
		snapshot, cleanupPaths, err := h.buildSnapshot(ctx, entry.root, generation, upstream)
		if err != nil {
			h.cleanupFailedGeneration(rootID, generation)
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
		if current := h.rootSnapshot(rootID); h.canReuseCurrentSnapshot(ctx, current) &&
			snapshotsMetadataEqual(current, snapshot) {
			h.cleanupFailedGeneration(rootID, generation)
			if h.sh != nil {
				h.sh.FinishRefresh(rootID, refreshGen, nil, targetsToProbe(current.Targets))
			}
			h.saveState(context.Background())
			h.reportMetadataState()
			slog.Debug(
				"repository refresh skipped identical metadata",
				"instance", h.name,
				"mode", h.mode,
				"root_id", rootID,
				"upstream", upstream,
			)
			return repositoryRefreshOutcome("unchanged", "same_as_current", current.Generation, upstream), nil
		}
		if err := h.publishSnapshot(ctx, snapshot, cleanupPaths); err != nil {
			h.cleanupFailedGeneration(rootID, generation)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if h.sh != nil {
			h.sh.FinishRefresh(rootID, refreshGen, nil, targetsToProbe(snapshot.Targets))
		}
		h.setRootSnapshot(rootID, snapshot)
		h.saveState(context.Background())
		h.reportMetadataState()
		slog.Debug(
			"repository refresh succeeded",
			"instance", h.name,
			"mode", h.mode,
			"root_id", rootID,
			"upstream", upstream,
		)
		return repositoryRefreshOutcome("updated", "published", snapshot.Generation, upstream), nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	if h.sh != nil {
		h.sh.FinishRefresh(rootID, refreshGen, refreshHealthError(firstErr), nil)
		if _, ok := h.sh.ResourceHealth(rootID); !ok {
			h.removeRoot(rootID)
		}
	}
	h.saveState(context.Background())
	return nil, firstErr
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

func (h *IndexedHandler) CleanupRoot(ctx context.Context, rootID string, opts config.CleanupConfig) error {
	rootDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "generations")
	currentGen := h.currentGeneration(rootID)
	if currentGen == "" {
		return nil
	}
	keep, err := h.loadCleanupPathSet(ctx, rootID, currentGen)
	if err != nil {
		return err
	}
	deleted := 0
	var toDelete []string
	if err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted+len(toDelete) >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil || entry.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(objectPath, rootDir+"/")
		parts := strings.SplitN(rel, "/", 3)
		if len(parts) == 0 {
			return nil
		}
		if parts[0] != currentGen {
			toDelete = append(toDelete, objectPath)
			return nil
		}
		if len(parts) != 3 || (parts[1] != "artifacts" && parts[1] != "auxiliary") {
			return nil
		}
		if _, ok := keep[parts[2]]; !ok {
			toDelete = append(toDelete, objectPath)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, item := range toDelete {
		if opts.DryRun {
			deleted++
			slog.Info("metadata gc dry-run delete", "instance", h.name, "root_id", rootID, "path", item)
			continue
		}
		if err := h.store.DeleteObject(ctx, h.name, item); err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("metadata gc failed", "path", item, "err", err)
		}
		deleted++
	}
	return nil
}

func (h *IndexedHandler) buildSnapshot(
	ctx context.Context,
	root RepositoryRoot,
	generation, upstream string,
) (*LiveSnapshot, []string, error) {
	session := &RefreshSession{
		handler:    h,
		rootID:     root.ID,
		upstream:   upstream,
		generation: generation,
		blobs:      map[string]*MetadataBlob{},
		targets:    append([]MetadataTarget(nil), root.Targets...),
	}
	defer session.Close()
	indexBuilder := &PathIndexBuilder{}
	snapshot, err := h.build(ctx, session, indexBuilder)
	if err != nil {
		return nil, nil, err
	}
	if snapshot == nil {
		return nil, nil, errors.New("metadata refresh produced no snapshot")
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]MetadataObject{}
	}
	snapshot.RootID = root.ID
	snapshot.RootPath = root.Path
	snapshot.Generation = generation
	snapshot.Upstream = upstream
	snapshot.Published = time.Now().UTC()
	snapshot.Targets = append([]MetadataTarget(nil), root.Targets...)
	cleanupPaths := indexBuilder.Finalize()
	for pathKey, obj := range snapshot.Metadata {
		if obj.Path == "" {
			obj.Path = pathKey
		}
		obj.StorePath = h.generationMetadataPath(root.ID, generation, obj.Path)
		if _, err := h.store.StatObject(ctx, h.name, obj.StorePath); err != nil {
			if obj.Required {
				return nil, nil, fmt.Errorf("%s: required metadata missing", obj.Path)
			}
			snapshot.Metadata[pathKey] = obj
			continue
		}
		digest, err := h.metadataObjectDigest(ctx, obj.StorePath)
		if err != nil {
			return nil, nil, fmt.Errorf("hash metadata %s: %w", obj.Path, err)
		}
		obj.Digest = digest
		snapshot.Metadata[pathKey] = obj
	}
	return snapshot, cleanupPaths, nil
}

func (h *IndexedHandler) metadataObjectDigest(ctx context.Context, objectPath string) (string, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

func (h *IndexedHandler) canReuseCurrentSnapshot(ctx context.Context, current *LiveSnapshot) bool {
	if current == nil || current.RootID == "" || current.Generation == "" {
		return false
	}
	_, err := h.store.StatObject(ctx, h.name, h.cleanupIndexPath(current.RootID, current.Generation))
	return err == nil
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
			currentObject.Digest != nextObject.Digest {
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

func (h *IndexedHandler) loadCleanupPathSet(
	ctx context.Context,
	rootID, generation string,
) (map[string]struct{}, error) {
	reader, err := h.store.OpenObject(ctx, h.name, h.cleanupIndexPath(rootID, generation))
	if err != nil {
		return nil, fmt.Errorf("load cleanup index for root %s generation %s: %w", rootID, generation, err)
	}
	defer reader.Close()

	paths := map[string]struct{}{}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		cleanPath := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(scanner.Text())), "/")
		if cleanPath == "." || cleanPath == "" || !httpcache.SafePath(cleanPath) {
			continue
		}
		paths[cleanPath] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}
