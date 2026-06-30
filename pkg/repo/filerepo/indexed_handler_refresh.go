package filerepo

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func (h *IndexedHandler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	if !h.hasAnyRootSnapshot() {
		return nil
	}
	currentPaths, err := h.currentCleanupPaths(ctx)
	if err != nil {
		return err
	}
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
		objectInfo, statErr := h.store.StatObject(ctx, h.name, objectPath)
		if statErr != nil || objectInfo.Options["indexed"] != "true" {
			return nil
		}
		cleanPath := strings.TrimPrefix(objectPath, h.objectRoot+"/")
		switch h.classify(cleanPath) {
		case ResourceArtifact, ResourceAuxiliary:
			if containsSortedPath(currentPaths, cleanPath) {
				return nil
			}
		default:
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

func (h *IndexedHandler) RefreshSubPath(ctx context.Context, subPath string) error {
	h.mu.RLock()
	entry, ok := h.roots[subPath]
	h.mu.RUnlock()
	if !ok || entry == nil || len(entry.targets) == 0 {
		return fmt.Errorf("root %s not found or has no targets", subPath)
	}
	var (
		refreshGen uint64
		release    func()
	)
	if h.sh != nil {
		rh, done, err := h.sh.TryStartRefresh(subPath, time.Now())
		if err != nil {
			reason := "rejected"
			switch {
			case errors.Is(err, health.ErrRefreshAlreadyRunning):
				reason = "already_refreshing"
			case errors.Is(err, health.ErrRefreshBlockedUntil):
				reason = "blocked"
			case errors.Is(err, health.ErrRefreshResourceRemoved):
				reason = "removed"
			}
			slog.Debug("subpath refresh skipped", "instance", h.name, "mode", h.mode, "subPath", subPath, "reason", reason)
			return scheduler.ErrTaskSkipped
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
		return errors.New("no upstreams available")
	}
	generation := strconv.FormatInt(time.Now().UnixNano(), 36)
	var firstErr error
	for _, upstream := range upstreams {
		snapshot, err := h.buildSnapshot(ctx, subPath, generation, upstream, entry.targets)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			slog.Debug("subpath refresh failed on upstream", "instance", h.name, "subPath", subPath, "upstream", upstream, "err", err)
			continue
		}
		if err := h.publishSnapshot(ctx, snapshot); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if h.sh != nil {
			h.sh.FinishRefresh(subPath, refreshGen, nil, targetsToProbe(snapshot.Targets))
		}
		h.mu.Lock()
		h.rootSnapshots[subPath] = snapshot
		h.rebuildAggregateLocked()
		h.mu.Unlock()
		h.reportMetadataState()
		slog.Debug("subpath refresh succeeded", "instance", h.name, "mode", h.mode, "subPath", subPath, "upstream", upstream)
		return nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	if h.sh != nil {
		h.sh.FinishRefresh(subPath, refreshGen, refreshHealthError(firstErr), nil)
	}
	return firstErr
}

func (h *IndexedHandler) CleanupSubPath(ctx context.Context, subPath string) error {
	rootDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(subPath), "generations")
	currentGen := h.currentGeneration(subPath)
	if currentGen == "" {
		return nil
	}
	var toDelete []string
	if err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		parts := strings.Split(strings.TrimPrefix(objectPath, rootDir+"/"), "/")
		if len(parts) > 0 && parts[0] != currentGen {
			toDelete = append(toDelete, objectPath)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, item := range toDelete {
		if err := h.store.DeleteObject(ctx, h.name, item); err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("metadata gc failed", "path", item, "err", err)
		}
	}
	return nil
}

func (h *IndexedHandler) buildSnapshot(ctx context.Context, rootKey, generation, upstream string, targets []MetadataTarget) (*LiveSnapshot, error) {
	session := &RefreshSession{
		handler:    h,
		rootKey:    rootKey,
		upstream:   upstream,
		generation: generation,
		blobs:      map[string]*MetadataBlob{},
		targets:    append([]MetadataTarget(nil), targets...),
	}
	defer session.Close()
	indexBuilder := &PathIndexBuilder{}
	snapshot, err := h.build(ctx, session, indexBuilder)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, errors.New("metadata refresh produced no snapshot")
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]MetadataObject{}
	}
	snapshot.RootKey = rootKey
	snapshot.Generation = generation
	snapshot.Upstream = upstream
	snapshot.Published = time.Now().UTC()
	snapshot.Targets = targets
	for pathKey, obj := range snapshot.Metadata {
		if obj.Path == "" {
			obj.Path = pathKey
		}
		obj.StorePath = h.generationMetadataPath(rootKey, generation, obj.Path)
		snapshot.Metadata[pathKey] = obj
		if obj.Required {
			if _, err := h.store.StatObject(ctx, h.name, obj.StorePath); err != nil {
				return nil, fmt.Errorf("%s: required metadata missing", obj.Path)
			}
		}
	}
	return snapshot, nil
}

func (h *IndexedHandler) currentCleanupPaths(ctx context.Context) ([]string, error) {
	h.mu.RLock()
	snapshots := make([]*LiveSnapshot, 0, len(h.rootSnapshots))
	for _, snapshot := range h.rootSnapshots {
		snapshots = append(snapshots, snapshot)
	}
	h.mu.RUnlock()

	var all []string
	for _, snapshot := range snapshots {
		paths, err := h.cleanupPathsForSnapshot(ctx, snapshot)
		if err != nil {
			return nil, err
		}
		all = append(all, paths...)
	}
	if len(all) == 0 {
		return nil, nil
	}
	sort.Strings(all)
	n := 1
	for i := 1; i < len(all); i++ {
		if all[i] == all[n-1] {
			continue
		}
		all[n] = all[i]
		n++
	}
	return all[:n], nil
}

func (h *IndexedHandler) cleanupPathsForSnapshot(ctx context.Context, snapshot *LiveSnapshot) ([]string, error) {
	if snapshot == nil {
		return nil, nil
	}
	if h.rebuild == nil {
		return nil, nil
	}
	builder := &PathIndexBuilder{}
	if err := h.rebuild(ctx, &LocalSession{handler: h, snapshot: snapshot, ctx: ctx}, builder); err != nil {
		return nil, err
	}
	return builder.Finalize(), nil
}

func containsSortedPath(paths []string, cleanPath string) bool {
	if len(paths) == 0 {
		return false
	}
	idx := sort.SearchStrings(paths, cleanPath)
	return idx < len(paths) && paths[idx] == cleanPath
}
