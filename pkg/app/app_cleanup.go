package app

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	goruntime "runtime"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (a *App) gcLoop() {
	defer close(a.gcDone)
	ticker := time.NewTicker(a.config.Storage.GC.Blob.Duration())
	defer ticker.Stop()
	for {
		select {
		case <-a.lifecycleCtx.Done():
			return
		case <-ticker.C:
			if _, err := a.store.RunGC(a.lifecycleCtx, blobfs.GCOptions{Compact: true}); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("blob gc failed", "err", err)
			}
		}
	}
}

func (a *App) cleanupLoop() {
	defer close(a.cleanupDone)
	ticker := time.NewTicker(a.config.Storage.Cleanup.Interval.Duration())
	defer ticker.Stop()
	for {
		select {
		case <-a.lifecycleCtx.Done():
			return
		case <-ticker.C:
			if a.config.Storage.Cleanup.Enabled {
				a.runCleanup(a.lifecycleCtx)
			}
		}
	}
}

func (a *App) runCleanup(ctx context.Context) {
	cfg := a.config.Storage.Cleanup
	if !cfg.Enabled || ctx.Err() != nil {
		return
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = goruntime.NumCPU()
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	var totalScanned, totalDeleted int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	for _, name := range enabledEntryNames(a.entries) {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		select {
		case <-ctx.Done():
			wg.Done()
		case sem <- struct{}{}:
			go func(name string) {
				defer wg.Done()
				defer func() { <-sem }()
				scanned, deleted, _ := a.cleanupInstance(ctx, name, batchSize, cfg.DryRun)
				mu.Lock()
				totalScanned += scanned
				totalDeleted += deleted
				mu.Unlock()
			}(name)
		}
	}
	wg.Wait()
	slog.Info("cache cleanup finished", "scanned", totalScanned, "deleted", totalDeleted, "dry_run", cfg.DryRun)
}

func (a *App) cleanupInstance(ctx context.Context, name string, batchSize int, dryRun bool) (int64, int64, error) {
	entry := a.entries[name]
	if entry == nil || entry.ExpireAfter.IsNever() || entry.ExpireAfter.IsUnset() {
		return 0, 0, nil
	}

	var scanned, deleted int64
	var batch []string
	err := fs.WalkDir(a.store.TenantFS(name), ".", func(path string, dirEntry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || dirEntry.IsDir() {
			return nil
		}
		info, statErr := a.store.StatObject(ctx, name, path)
		if statErr != nil || info.State != "ACTIVE" {
			return nil
		}
		expired, age, parseErr := isExpiredByOptions(info.Options, entry.ExpireAfter.Duration())
		if parseErr != nil {
			slog.Debug("cleanup parse fetched-at failed", "instance", name, "path", path, "err", parseErr)
		}
		if !expired {
			return nil
		}
		scanned++
		if dryRun {
			slog.Info("cleanup dry-run delete", "instance", name, "path", path, "expired_for", age.Round(time.Second))
			return nil
		}
		batch = append(batch, path)
		if len(batch) < batchSize {
			return nil
		}
		deleted += deleteBatch(ctx, a.store, name, batch)
		batch = batch[:0]
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	})
	if len(batch) > 0 {
		deleted += deleteBatch(ctx, a.store, name, batch)
	}
	if errors.Is(err, context.Canceled) {
		return scanned, deleted, nil
	}
	return scanned, deleted, err
}

func isExpiredByOptions(options map[string]string, expireAfter time.Duration) (bool, time.Duration, error) {
	fetchedAtStr := options["fetched-at"]
	if fetchedAtStr == "" {
		return true, 0, nil
	}
	fetchedAt, err := utils.ParseFetchedAt(fetchedAtStr)
	if err != nil {
		return true, 0, err
	}
	if expireAfter <= 0 {
		return false, 0, nil
	}
	expiredAt := fetchedAt.Add(expireAfter)
	now := time.Now()
	if now.After(expiredAt) {
		return true, now.Sub(expiredAt), nil
	}
	return false, 0, nil
}

func deleteBatch(ctx context.Context, store *blobfs.Store, instance string, batch []string) int64 {
	var deleted int64
	for _, item := range batch {
		if ctx.Err() != nil {
			return deleted
		}
		if err := store.DeleteObject(ctx, instance, item); err == nil {
			deleted++
		} else if !errors.Is(err, context.Canceled) {
			slog.Warn("cleanup delete failed", "instance", instance, "path", item, "err", err)
		}
	}
	return deleted
}
