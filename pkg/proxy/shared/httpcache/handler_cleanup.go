package httpcache

import (
	"context"
	"errors"
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
	return CleanupStoreTenant(ctx, h.store, h.name, h.config.ExpireAfter.Duration(), opts)
}

func CleanupStoreTenant(ctx context.Context, store *blobfs.Store, tenant string, expireAfter time.Duration, opts config.CleanupConfig) error {
	deleted := 0
	return fs.WalkDir(store.TenantFS(tenant), ".", func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil || entry.IsDir() {
			return nil
		}
		info, statErr := store.StatObject(ctx, tenant, objectPath)
		if statErr != nil || info.State != "ACTIVE" {
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
		if err := store.DeleteObject(ctx, tenant, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("cleanup delete failed", "instance", tenant, "path", objectPath, "err", err)
		} else {
			deleted++
		}
		return nil
	})
}
