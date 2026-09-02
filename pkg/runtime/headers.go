package runtime

import (
	"net/http"
	"strings"
)

// CopyEndToEndHeaders copies HTTP headers while removing fixed and
// Connection-declared hop-by-hop fields.
func CopyEndToEndHeaders(destination, source http.Header) {
	connectionHeaders := make(map[string]struct{})
	for _, value := range source.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			if name := http.CanonicalHeaderKey(strings.TrimSpace(token)); name != "" {
				connectionHeaders[name] = struct{}{}
			}
		}
	}
	for name, values := range source {
		canonical := http.CanonicalHeaderKey(name)
		if _, declared := connectionHeaders[canonical]; declared || isHopByHopHeader(canonical) {
			continue
		}
		destination[canonical] = append([]string(nil), values...)
	}
}

func isHopByHopHeader(name string) bool {
	switch name {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}
