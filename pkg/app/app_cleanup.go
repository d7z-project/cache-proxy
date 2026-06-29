package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func (a *App) runCleanup(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	a.routesMu.RLock()
	cleanup := a.config.Storage.Cleanup
	workers := cleanup.Workers
	handlers := make([]proxyruntime.Instance, len(a.handlers))
	copy(handlers, a.handlers)
	a.routesMu.RUnlock()

	if !cleanup.Enabled {
		return
	}

	if workers <= 0 {
		workers = len(handlers)
	}
	if workers <= 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	for _, handler := range handlers {
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
				if err := handler.Cleanup(ctx, cleanup); err != nil && !errors.Is(err, context.Canceled) {
					slog.Info("instance cleanup failed", "err", err)
				}
			}(handler)
		}
	}
	wg.Wait()
}
