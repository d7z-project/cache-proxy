package app

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

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

func i18nStr(i18n map[string]string, key string, args ...any) string {
	msg, ok := i18n[key]
	if !ok {
		msg = key
	}
	if len(args) == 0 {
		return msg
	}
	return fmt.Sprintf(msg, args...)
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

func collectTenantUsage(ctx context.Context, tenants []string, store *blobfs.Store) map[string]int64 {
	usage := make(map[string]int64)
	if store == nil {
		return usage
	}
	for _, tenant := range tenants {
		var total int64
		_ = fs.WalkDir(store.TenantFS(tenant), ".", func(path string, d fs.DirEntry, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err != nil || d.IsDir() {
				return nil
			}
			info, statErr := d.Info()
			if statErr != nil {
				return nil
			}
			total += info.Size()
			return nil
		})
		usage[tenant] = total
	}
	return usage
}
