package filerepo

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const metadataGenerationRetention = 15 * time.Minute

const maintenanceContinuationDelay = time.Second

type cleanupResult struct {
	Scanned   int
	Protected int
	Deleted   int
	Truncated bool
}

func (h *IndexedHandler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	_, err := h.cleanupContent(ctx, opts)
	return err
}

func (h *IndexedHandler) CleanupTask(ctx context.Context, opts config.CleanupConfig) (*scheduler.TaskOutcome, error) {
	result, err := h.cleanupContent(ctx, opts)
	if err != nil {
		result := "error"
		if strings.Contains(err.Error(), "cleanup index") {
			result = "index_error"
		}
		h.stats.RecordRepositoryMaintenance(h.name, h.mode, "content_cleanup", result, nil)
		return nil, err
	}
	h.recordCleanupMaintenance("content_cleanup", result)
	outcome := maintenanceOutcome("content_cleanup", result)
	if result.Truncated {
		return outcome, scheduler.RetryAt(time.Now().Add(maintenanceContinuationDelay))
	}
	return outcome, nil
}

func (h *IndexedHandler) cleanupContent(ctx context.Context, opts config.CleanupConfig) (cleanupResult, error) {
	var result cleanupResult
	keep, err := h.currentCleanupPaths(ctx)
	if err != nil {
		return result, err
	}
	err = fs.WalkDir(h.store.TenantFS(h.name), h.objectRoot, func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || entry.IsDir() || objectPath == h.statePath() || strings.Contains(objectPath, "/.roots/") {
			return nil
		}
		result.Scanned++
		if cleanPath, class, ok := h.contentObject(objectPath); ok {
			if _, current := keep[cleanPath]; current || h.base.Busy(objectPath) {
				result.Protected++
				return nil
			}
			expireAfter := h.policy.AuxiliaryExpireAfter
			if class == ResourceArtifact {
				expireAfter = h.policy.ArtifactExpireAfter
			}
			if expireAfter.IsNever() || expireAfter.IsUnset() {
				return nil
			}
			info, statErr := h.store.StatObject(ctx, h.name, objectPath)
			if statErr != nil {
				return nil
			}
			fetchedAt, parseErr := utils.ParseFetchedAt(info.Options["fetched-at"])
			if parseErr == nil && time.Since(fetchedAt) <= expireAfter.Duration() {
				return nil
			}
		}
		if opts.BatchSize > 0 && result.Deleted >= opts.BatchSize {
			result.Truncated = true
			return fs.SkipAll
		}
		if opts.DryRun {
			result.Deleted++
			slog.Info("indexed cleanup dry-run delete", "instance", h.name, "path", objectPath)
			return nil
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("indexed cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		} else {
			result.Deleted++
		}
		return nil
	})
	return result, err
}

func (h *IndexedHandler) currentCleanupPaths(ctx context.Context) (map[string]struct{}, error) {
	h.mu.RLock()
	type cleanupReference struct {
		rootID     string
		generation string
	}
	references := make(map[string]cleanupReference, len(h.rootSnapshots))
	for _, snapshot := range h.rootSnapshots {
		if snapshot != nil {
			key := snapshot.RootID + "\x00" + snapshot.Generation
			references[key] = cleanupReference{rootID: snapshot.RootID, generation: snapshot.Generation}
		}
	}
	for rootID, entry := range h.roots {
		if entry != nil && entry.retirementCleanupGeneration != "" {
			key := rootID + "\x00" + entry.retirementCleanupGeneration
			references[key] = cleanupReference{rootID: rootID, generation: entry.retirementCleanupGeneration}
		}
	}
	h.mu.RUnlock()
	keep := map[string]struct{}{}
	for _, reference := range references {
		paths, err := h.loadCleanupPathSet(ctx, reference.rootID, reference.generation)
		if err != nil {
			return nil, err
		}
		for cleanPath := range paths {
			keep[cleanPath] = struct{}{}
		}
	}
	return keep, nil
}

func (h *IndexedHandler) contentObject(objectPath string) (string, ResourceClass, bool) {
	prefix := path.Join(h.objectRoot, ".content") + "/"
	rel := strings.TrimPrefix(objectPath, prefix)
	if rel == objectPath {
		return "", ResourceUnknown, false
	}
	parts := strings.SplitN(rel, "/", 2)
	if len(parts) != 2 || !httpcache.SafePath(parts[1]) {
		return "", ResourceUnknown, false
	}
	switch parts[0] {
	case "artifacts":
		return parts[1], ResourceArtifact, true
	case "sidecars":
		return parts[1], ResourceSidecar, true
	default:
		return "", ResourceUnknown, false
	}
}

func (h *IndexedHandler) CleanupRoot(ctx context.Context, rootID string, opts config.CleanupConfig) error {
	_, err := h.cleanupRoot(ctx, rootID, opts)
	return err
}

func (h *IndexedHandler) CleanupRootTask(ctx context.Context, rootID string, opts config.CleanupConfig) (*scheduler.TaskOutcome, error) {
	if h.rootNeedsRetirement(rootID, time.Now()) {
		if err := h.beginRootRetirement(ctx, rootID); err != nil {
			return nil, err
		}
	}
	result, err := h.cleanupRoot(ctx, rootID, opts)
	if err != nil {
		h.stats.RecordRepositoryMaintenance(h.name, h.mode, "metadata_gc", "error", nil)
		return nil, err
	}
	h.recordCleanupMaintenance("metadata_gc", result)
	outcome := maintenanceOutcome("metadata_gc", result)
	if result.Truncated {
		return outcome, scheduler.RetryAt(time.Now().Add(maintenanceContinuationDelay))
	}
	if !opts.DryRun && h.rootRetired(rootID) {
		removed, err := h.finishRootRetirement(ctx, rootID)
		if err != nil {
			return nil, err
		}
		if removed {
			outcome.Result = "retired"
			outcome.ReasonCode = "root_inactive"
			h.stats.RecordRepositoryMaintenance(h.name, h.mode, "root_retirement", "complete", nil)
		}
	}
	return outcome, nil
}

func (h *IndexedHandler) finishRootRetirement(ctx context.Context, rootID string) (bool, error) {
	if h.rootHasReaders(rootID) {
		return false, nil
	}
	rootBase := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID))
	hasFiles, err := h.generationHasFiles(ctx, rootBase)
	if err != nil || hasFiles {
		return false, err
	}
	if err := h.store.RemoveAll(path.Join(h.name, rootBase)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	h.removeRoot(rootID)
	if h.serviceHealth != nil {
		h.serviceHealth.RemoveResource(rootID)
	} else if h.bus != nil {
		h.bus.Publish(bus.Event{Type: bus.EventMetadataRemoved, Payload: bus.MetadataRemovedPayload{Instance: h.name, RootID: rootID}})
	}
	h.saveState(context.Background())
	return true, nil
}

func (h *IndexedHandler) cleanupRoot(ctx context.Context, rootID string, opts config.CleanupConfig) (cleanupResult, error) {
	var result cleanupResult
	rootBase := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID))
	rootDir := path.Join(rootBase, "generations")
	retained, err := h.retainedMetadataGenerations(ctx, rootID)
	if err != nil {
		return result, err
	}
	var toDelete []string
	if err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || entry.IsDir() {
			return nil
		}
		result.Scanned++
		rel := strings.TrimPrefix(objectPath, rootDir+"/")
		parts := strings.SplitN(rel, "/", 3)
		if len(parts) == 0 {
			return nil
		}
		if _, keep := retained[parts[0]]; !keep && !h.metadataGenerationInUse(rootID, parts[0]) {
			if opts.BatchSize > 0 && result.Deleted+len(toDelete) >= opts.BatchSize {
				result.Truncated = true
				return fs.SkipAll
			}
			toDelete = append(toDelete, objectPath)
			return nil
		}
		result.Protected++
		return nil
	}); err != nil {
		return result, err
	}
	for _, item := range toDelete {
		if opts.DryRun {
			result.Deleted++
			slog.Info("metadata gc dry-run delete", "instance", h.name, "root_id", rootID, "path", item)
			continue
		}
		if err := h.store.DeleteObject(ctx, h.name, item); err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("metadata gc failed", "path", item, "err", err)
			continue
		}
		result.Deleted++
	}
	if result.Truncated {
		return result, nil
	}
	snapshotDir := path.Join(rootBase, "snapshots")
	err = fs.WalkDir(h.store.TenantFS(h.name), snapshotDir, func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			return nil
		}
		result.Scanned++
		generation := strings.TrimSuffix(entry.Name(), ".yaml")
		if _, keep := retained[generation]; generation == "" || keep || h.metadataGenerationInUse(rootID, generation) {
			result.Protected++
			return nil
		}
		hasFiles, err := h.generationHasFiles(ctx, path.Join(rootDir, generation))
		if err != nil || hasFiles {
			return err
		}
		if opts.BatchSize > 0 && result.Deleted >= opts.BatchSize {
			result.Truncated = true
			return fs.SkipAll
		}
		if opts.DryRun {
			result.Deleted++
			slog.Info("metadata gc dry-run delete snapshot and cleanup index", "instance", h.name, "root_id", rootID, "generation", generation)
			return nil
		}
		cleanupIndex := h.cleanupIndexPath(rootID, generation)
		if err := h.store.DeleteObject(ctx, h.name, cleanupIndex); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("delete cleanup index for root %s generation %s: %w", rootID, generation, err)
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("metadata snapshot gc failed", "path", objectPath, "err", err)
			return nil
		}
		result.Deleted++
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return result, nil
	}
	return result, err
}

func maintenanceOutcome(kind string, result cleanupResult) *scheduler.TaskOutcome {
	state := "complete"
	if result.Truncated {
		state = "partial"
	}
	return &scheduler.TaskOutcome{
		Result: state, ReasonCode: kind,
		Detail: fmt.Sprintf("scanned=%d protected=%d deleted=%d truncated=%t", result.Scanned, result.Protected, result.Deleted, result.Truncated),
	}
}

func (h *IndexedHandler) recordCleanupMaintenance(operation string, result cleanupResult) {
	state := "complete"
	if result.Truncated {
		state = "truncated"
	}
	h.stats.RecordRepositoryMaintenance(h.name, h.mode, operation, state, map[string]int{
		"scanned": result.Scanned, "protected": result.Protected, "deleted": result.Deleted,
	})
}

func (h *IndexedHandler) retainedMetadataGenerations(ctx context.Context, rootID string) (map[string]struct{}, error) {
	retained := map[string]struct{}{}
	if staging, ok := h.loadRefreshStaging(ctx, rootID); ok {
		retained[staging.Generation] = struct{}{}
	}
	if generation := h.currentGeneration(rootID); generation != "" {
		retained[generation] = struct{}{}
	}
	if generation, ok := h.durableCurrentGeneration(ctx, rootID); ok {
		retained[generation] = struct{}{}
	}
	hasCurrent := len(retained) > 0

	snapshotDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "snapshots")
	cutoff := time.Now().Add(-metadataGenerationRetention)
	var newestPrevious *LiveSnapshot
	err := fs.WalkDir(h.store.TenantFS(h.name), snapshotDir, func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			return nil
		}
		snapshot, ok := h.loadSnapshot(ctx, objectPath)
		if !ok || snapshot.RootID != rootID || snapshot.Generation == "" {
			return nil
		}
		if _, current := retained[snapshot.Generation]; current {
			return nil
		}
		if hasCurrent && snapshot.Published.After(cutoff) {
			retained[snapshot.Generation] = struct{}{}
		}
		if newestPrevious == nil || snapshot.Published.After(newestPrevious.Published) ||
			snapshot.Published.Equal(newestPrevious.Published) && snapshot.Generation > newestPrevious.Generation {
			newestPrevious = snapshot
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("scan metadata snapshots for root %s: %w", rootID, err)
	}
	if hasCurrent && newestPrevious != nil {
		retained[newestPrevious.Generation] = struct{}{}
	}
	return retained, nil
}

func (h *IndexedHandler) generationHasFiles(ctx context.Context, generationDir string) (bool, error) {
	hasFiles := false
	err := fs.WalkDir(h.store.TenantFS(h.name), generationDir, func(_ string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return walkErr
		}
		if !entry.IsDir() {
			hasFiles = true
			return fs.SkipAll
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return hasFiles, err
}

func (h *IndexedHandler) loadCleanupPathSet(
	ctx context.Context,
	rootID, generation string,
) (map[string]struct{}, error) {
	reader, err := h.store.OpenObject(ctx, h.name, h.cleanupIndexPath(rootID, generation))
	if err != nil {
		return nil, fmt.Errorf("load cleanup index for root %s generation %s: %w", rootID, generation, err)
	}
	defer func() { _ = reader.Close() }()

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
