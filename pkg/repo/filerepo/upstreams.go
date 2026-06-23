package filerepo

import "strings"

func CollectUpstreams(explicit []string, fallback []string) []string {
	seen := map[string]struct{}{}
	upstreams := make([]string, 0, len(explicit)+len(fallback))
	for _, group := range [][]string{explicit, fallback} {
		for _, raw := range group {
			value := strings.TrimRight(strings.TrimSpace(raw), "/")
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			upstreams = append(upstreams, value)
		}
	}
	return upstreams
}
