package flatpak

import (
	"net/http"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func rewriteDescriptor(req *http.Request, data []byte) []byte {
	base := proxyBaseURL(req)
	lines := strings.SplitAfter(string(data), "\n")
	for i, line := range lines {
		key, _, ok := strings.Cut(strings.TrimRight(line, "\r\n"), "=")
		if !ok || key != "Url" {
			continue
		}
		lines[i] = replaceINIValue(line, key, base)
	}
	return []byte(strings.Join(lines, ""))
}

func proxyBaseURL(req *http.Request) string {
	prefix := strings.TrimSpace(req.Header.Get("X-Cache-Proxy-Prefix"))
	if prefix == "" || prefix == "/" {
		return httpcache.BaseURL(req)
	}
	return strings.TrimRight(httpcache.BaseURL(req), "/") + "/" + strings.Trim(strings.TrimPrefix(prefix, "/"), "/")
}

func replaceINIValue(line, key, value string) string {
	switch {
	case strings.HasSuffix(line, "\r\n"):
		return key + "=" + value + "\r\n"
	case strings.HasSuffix(line, "\n"):
		return key + "=" + value + "\n"
	default:
		return key + "=" + value
	}
}
