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
		cleanPath := strings.TrimPrefix(objectPath, h.objectRoot+"/")
		switch h.inspect(cleanPath).Class {
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

func (h *IndexedHandler) RefreshRoot(ctx context.Context, rootID string) error {
	h.mu.RLock()
	entry, ok := h.roots[rootID]
	h.mu.RUnlock()
	if !ok || entry == nil || len(entry.root.Targets) == 0 {
		return fmt.Errorf("root %s not found or has no targets", rootID)
	}
	targets := append([]MetadataTarget(nil), entry.root.Targets...)
	var (
		refreshGen uint64
		release    func()
	)
	if h.sh != nil {
		rh, done, err := h.sh.TryStartRefresh(rootID, time.Now())
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
			slog.Debug("repository refresh skipped", "instance", h.name, "mode", h.mode, "root_id", rootID, "reason", reason)
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
		if current := h.rootSnapshot(rootID); current != nil && current.Upstream == upstream {
			unchanged, err := h.canSkipRefresh(ctx, current, upstream, targets)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				slog.Debug("repository refresh head check failed", "instance", h.name, "root_id", rootID, "upstream", upstream, "err", err)
				continue
			}
			if unchanged {
				if h.sh != nil {
					h.sh.FinishRefresh(rootID, refreshGen, nil, targetsToProbe(current.Targets))
				}
				h.saveState(context.Background())
				h.reportMetadataState()
				slog.Debug("repository refresh skipped unchanged metadata", "instance", h.name, "mode", h.mode, "root_id", rootID, "upstream", upstream)
				return nil
			}
		}
		snapshot, err := h.buildSnapshot(ctx, entry.root, generation, upstream)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			slog.Debug("repository refresh failed on upstream", "instance", h.name, "root_id", rootID, "upstream", upstream, "err", err)
			continue
		}
		if err := h.publishSnapshot(ctx, snapshot); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if h.sh != nil {
			h.sh.FinishRefresh(rootID, refreshGen, nil, targetsToProbe(snapshot.Targets))
		}
		h.mu.Lock()
		h.rootSnapshots[rootID] = snapshot
		h.rebuildAggregateLocked()
		h.mu.Unlock()
		h.saveState(context.Background())
		h.reportMetadataState()
		slog.Debug("repository refresh succeeded", "instance", h.name, "mode", h.mode, "root_id", rootID, "upstream", upstream)
		return nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	if h.sh != nil {
		h.sh.FinishRefresh(rootID, refreshGen, refreshHealthError(firstErr), nil)
	}
	h.saveState(context.Background())
	return firstErr
}

func (h *IndexedHandler) CleanupRoot(ctx context.Context, rootID string) error {
	rootDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "generations")
	currentGen := h.currentGeneration(rootID)
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

func (h *IndexedHandler) buildSnapshot(ctx context.Context, root RepositoryRoot, generation, upstream string) (*LiveSnapshot, error) {
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
		return nil, err
	}
	if snapshot == nil {
		return nil, errors.New("metadata refresh produced no snapshot")
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
	for pathKey, obj := range snapshot.Metadata {
		if obj.Path == "" {
			obj.Path = pathKey
		}
		obj.StorePath = h.generationMetadataPath(root.ID, generation, obj.Path)
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
