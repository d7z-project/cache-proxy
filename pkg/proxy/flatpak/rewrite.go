package flatpak

import (
	"net/http"
	"strings"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func rewriteDescriptor(req *http.Request, data []byte) []byte {
	base := proxyruntime.ExternalBaseURL(req)
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
