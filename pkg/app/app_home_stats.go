package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
)

func formatHitRate(cache map[string]uint64) string {
	rate, ok := cacheHitRate(cache)
	if !ok {
		return "\u2014"
	}
	return fmt.Sprintf("%.1f%%", rate*100)
}

func cacheHitRate(cache map[string]uint64) (float64, bool) {
	if len(cache) == 0 {
		return 0, false
	}
	var total uint64
	var hits uint64
	for cacheResult, count := range cache {
		total += count
		switch strings.ToUpper(cacheResult) {
		case "HIT", "FRESH", "STALE", "REVALIDATED":
			hits += count
		}
	}
	if total == 0 {
		return 0, false
	}
	return float64(hits) / float64(total), true
}

func formatCompact(n uint64) string {
	switch {
	case n < 1000:
		return strconv.FormatUint(n, 10)
	case n < 1000000:
		v := float64(n) / 1000
		if v >= 100 {
			return fmt.Sprintf("%.0fk", v)
		}
		return fmt.Sprintf("%.1fk", v)
	default:
		v := float64(n) / 1000000
		if v >= 100 {
			return fmt.Sprintf("%.0fM", v)
		}
		return fmt.Sprintf("%.1fM", v)
	}
}

func formatBytes(n int64) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%dB", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1fK", float64(n)/1024)
	case n < 1024*1024*1024:
		return fmt.Sprintf("%.1fM", float64(n)/(1024*1024))
	default:
		return fmt.Sprintf("%.1fG", float64(n)/(1024*1024*1024))
	}
}

func collectInstanceUsage(ctx context.Context, instances []string, stores map[string]*blobfs.Store) map[string]int64 {
	usage := make(map[string]int64)
	for _, instance := range instances {
		if ctx.Err() != nil {
			break
		}
		store := stores[instance]
		if store == nil {
			continue
		}
		stats, err := store.Stats(ctx)
		if err == nil && stats != nil {
			usage[instance] = stats.Bytes.LogicalObjectBytes
		}
	}
	return usage
}

func (a *App) instanceUsage(ctx context.Context, instances []string) map[string]int64 {
	a.instanceUsageMu.Lock()
	previous := a.instanceUsageCachedAt
	result := make(map[string]int64, len(a.instanceUsageCache))
	for instance, size := range a.instanceUsageCache {
		result[instance] = size
	}
	a.instanceUsageMu.Unlock()
	if time.Since(previous) >= 5*time.Minute {
		a.refreshInstanceUsage(ctx, instances)
	}
	return result
}

func (a *App) refreshInstanceUsage(parent context.Context, instances []string) {
	if len(a.stores) == 0 || !a.instanceUsageRefreshing.CompareAndSwap(false, true) {
		return
	}
	a.instanceUsageMu.Lock()
	if a.instanceUsageClosing {
		a.instanceUsageMu.Unlock()
		a.instanceUsageRefreshing.Store(false)
		return
	}
	a.instanceUsageWG.Add(1)
	a.instanceUsageMu.Unlock()
	names := append([]string(nil), instances...)
	go func() {
		defer a.instanceUsageWG.Done()
		defer a.instanceUsageRefreshing.Store(false)
		baseCtx := a.lifecycleCtx
		if baseCtx == nil {
			baseCtx = parent
		}
		if baseCtx == nil {
			baseCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
		defer cancel()
		usage := collectInstanceUsage(ctx, names, a.stores)
		a.instanceUsageMu.Lock()
		a.instanceUsageCache = usage
		a.instanceUsageCachedAt = time.Now()
		a.instanceUsageMu.Unlock()
	}()
}
