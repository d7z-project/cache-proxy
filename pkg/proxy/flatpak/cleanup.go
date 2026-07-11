package flatpak

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *Handler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	deleted := 0
	return fs.WalkDir(h.store.TenantFS(h.name), ".", func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil || entry.IsDir() || strings.HasPrefix(objectPath, metadataRoot+"/") {
			return nil
		}
		expireAfter := h.expireAfter
		if strings.HasPrefix(objectPath, "flatpak/deltas/") {
			expireAfter = h.deltaExpireAfter
		}
		if expireAfter.IsNever() || expireAfter.IsUnset() {
			return nil
		}
		info, statErr := h.store.StatObject(ctx, h.name, objectPath)
		if statErr != nil || info.State != "ACTIVE" {
			return nil
		}
		fetchedAt, parseErr := utils.ParseFetchedAt(info.Options["fetched-at"])
		if parseErr == nil && time.Since(fetchedAt) <= expireAfter.Duration() {
			return nil
		}
		if parseErr != nil {
			slog.Debug("flatpak cleanup parse fetched-at failed", "instance", h.name, "path", objectPath, "err", parseErr)
		}
		if opts.DryRun {
			deleted++
			slog.Info("flatpak cleanup dry-run delete", "instance", h.name, "path", objectPath)
			return nil
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("flatpak cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		} else {
			deleted++
		}
		return nil
	})
}
