package app

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"gopkg.d7z.net/blobfs"

	httpcache "gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func formatHitRate(cache map[string]uint64) string {
	served := cache["HIT"] + cache["FRESH"] + cache["REFRESH"] + cache["STALE"]
	total := served + cache["MISS"] + cache["BYPASS"]
	if total == 0 {
		return "\u2014"
	}
	return fmt.Sprintf("%.1f%%", float64(served)/float64(total)*100)
}

func instanceStatus(s httpcache.InstanceStats, i18n map[string]string) (color, label, extra string) {
	if s.MetadataState != "" {
		switch s.MetadataState {
		case "ready":
			color = "green"
		case "refreshing":
			color = "blue"
		case "degraded":
			color = "yellow"
		default:
			color = "gray"
		}
		key := s.MetadataState
		if key == "booting" {
			key = "loading"
		}
		label = i18nStr(i18n, key)
		if !s.LastRefreshAt.IsZero() {
			extra = relativeTime(time.Since(s.LastRefreshAt), i18n)
		}
		return
	}
	if s.UpstreamRequests == 0 {
		return "", "\u2014", ""
	}
	errRate := float64(s.UpstreamErrors) / float64(s.UpstreamRequests) * 100
	if errRate >= 5 {
		return "yellow", i18nStr(i18n, "n_err", int(s.UpstreamErrors)), ""
	}
	return "green", i18nStr(i18n, "upstream_ok"), ""
}

func relativeTime(d time.Duration, i18n map[string]string) string {
	switch {
	case d < time.Second:
		return i18nStr(i18n, "just_now")
	case d < time.Minute:
		return i18nStr(i18n, "s_ago", int(d.Seconds()))
	case d < time.Hour:
		return i18nStr(i18n, "m_ago", int(d.Minutes()))
	default:
		return i18nStr(i18n, "h_ago", int(d.Hours()))
	}
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

func formatRootStateColor(state string) string {
	switch state {
	case "active":
		return "green"
	case "suspect":
		return "yellow"
	case "blocked":
		return "red"
	default:
		return "gray"
	}
}

func collectTenantUsage(ctx context.Context, tenants []string, store *blobfs.Store) map[string]int64 {
	usage := make(map[string]int64)
	for _, tenant := range tenants {
		var total int64
		_ = fs.WalkDir(store.TenantFS(tenant), ".", func(path string, d fs.DirEntry, err error) error {
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
