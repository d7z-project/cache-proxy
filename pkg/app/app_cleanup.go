package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
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
				slog.Info("blob gc failed", "err", err)
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
	if !a.config.Storage.Cleanup.Enabled || ctx.Err() != nil {
		return
	}
	workers := a.config.Storage.Cleanup.Workers
	if workers <= 0 {
		workers = len(a.handlers)
	}
	if workers <= 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	for _, handler := range a.handlers {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		select {
		case <-ctx.Done():
			wg.Done()
		case sem <- struct{}{}:
			go func(handler proxyruntime.Instance) {
				defer wg.Done()
				defer func() { <-sem }()
				defer func() {
					if r := recover(); r != nil {
						slog.Error("cleanup panic", "panic", r)
					}
				}()
				if err := handler.Cleanup(ctx); err != nil && !errors.Is(err, context.Canceled) {
					slog.Info("instance cleanup failed", "err", err)
				}
			}(handler)
		}
	}
	wg.Wait()
}
