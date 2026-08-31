package httpcache

import (
	"context"
	"io/fs"
	"log/slog"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *Handler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	if h.config.ExpireAfter.IsNever() || h.config.ExpireAfter.IsUnset() {
		return nil
	}
	h.cleanupMu.Lock()
	defer h.cleanupMu.Unlock()
	next, err := cleanupStoreTenant(ctx, h.store, h.name, h.config.ExpireAfter.Duration(), opts, h.cleanupAfter, h.flights.active)
	if err == nil {
		h.cleanupAfter = next
	}
	return err
}

func cleanupStoreTenant(ctx context.Context, store *blobfs.Store, tenant string, expireAfter time.Duration, opts config.CleanupConfig, after string, busy func(string) bool) (string, error) {
	deleted, inspected := 0, 0
	inspectionLimit := 0
	if opts.BatchSize > 0 {
		inspectionLimit = opts.BatchSize * 10
	}
	lastPath := ""
	completed := true
	err := fs.WalkDir(store.TenantFS(tenant), ".", func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			completed = false
			return fs.SkipAll
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if after != "" && objectPath <= after {
			return nil
		}
		if inspectionLimit > 0 && inspected >= inspectionLimit {
			completed = false
			return fs.SkipAll
		}
		inspected++
		lastPath = objectPath
		if busy(objectPath) {
			return nil
		}
		info, statErr := store.StatObject(ctx, tenant, objectPath)
		if statErr != nil {
			return statErr
		}
		if info.State != "ACTIVE" {
			return nil
		}
		fetchedAt, parseErr := utils.ParseFetchedAt(info.Options["fetched-at"])
		if parseErr == nil && time.Since(fetchedAt) <= expireAfter {
			return nil
		}
		if parseErr != nil {
			slog.Debug("cleanup parse fetched-at failed", "instance", tenant, "path", objectPath, "err", parseErr)
		}
		if opts.DryRun {
			deleted++
			slog.Info("cleanup dry-run delete", "instance", tenant, "path", objectPath)
			return nil
		}
		if err := store.DeleteObject(ctx, tenant, objectPath); err != nil {
			return err
		}
		deleted++
		return nil
	})
	if err != nil {
		return after, err
	}
	if completed {
		return "", nil
	}
	return lastPath, nil
}
