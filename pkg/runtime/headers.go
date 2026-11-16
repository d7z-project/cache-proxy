package runtime

import (
	"net/http"
	"strings"
)

var privateResponseHeaders = map[string]struct{}{
	"Alt-Svc":                      {},
	"Cache-Status":                 {},
	"Cdn-Loop":                     {},
	"Cdn-Cache-Control":            {},
	"Cloudflare-Cdn-Cache-Control": {},
	"Forwarded":                    {},
	"Nel":                          {},
	"Proxy-Authentication-Info":    {},
	"Proxy-Status":                 {},
	"Report-To":                    {},
	"Reporting-Endpoints":          {},
	"Server":                       {},
	"Server-Timing":                {},
	"Surrogate-Control":            {},
	"Surrogate-Key":                {},
	"Traceparent":                  {},
	"Tracestate":                   {},
	"Via":                          {},
	"X-Amz-Cf-Id":                  {},
	"X-Amz-Cf-Pop":                 {},
	"X-Amz-Id-2":                   {},
	"X-Amz-Request-Id":             {},
	"X-Backend":                    {},
	"X-Backend-Server":             {},
	"X-Cache":                      {},
	"X-Cache-Error":                {},
	"X-Cache-Hits":                 {},
	"X-Cdn":                        {},
	"X-Cdn-Cache":                  {},
	"X-Correlation-Id":             {},
	"X-Edge-Location":              {},
	"X-Lighttpd-Send-File":         {},
	"X-Powered-By":                 {},
	"X-Proxy-Cache":                {},
	"X-Request-Id":                 {},
	"X-Request-Time":               {},
	"X-Response-Time":              {},
	"X-Runtime":                    {},
	"X-Sendfile":                   {},
	"X-Served-By":                  {},
	"X-Timer":                      {},
	"X-Upstream":                   {},
	"X-Upstream-Address":           {},
	"X-Upstream-Host":              {},
	"X-Upstream-Name":              {},
	"X-Upstream-Response-Time":     {},
	"X-Upstream-Service-Time":      {},
	"X-Varnish":                    {},
}

var privateResponsePrefixes = []string{
	"Akamai-",
	"Cf-",
	"Fastly-",
	"X-Accel-",
	"X-Azure-",
	"X-B3-",
	"X-Cache-",
	"X-Cdn-",
	"X-Cloud-Trace-",
	"X-Datadog-",
	"X-Envoy-",
	"X-Fastly-",
	"X-Forwarded-",
	"X-Original-",
	"X-Ot-",
	"X-Proxy-",
	"X-Request-",
	"X-Rewrite-",
	"X-Sentry-",
	"X-Source-",
	"X-Trace-",
}

// CopyEndToEndHeaders copies HTTP headers while removing fixed and
// Connection-declared hop-by-hop fields.
func CopyEndToEndHeaders(destination, source http.Header) {
	connectionHeaders := make(map[string]struct{})
	for name, values := range source {
		if strings.EqualFold(name, "Connection") {
			for _, value := range values {
				for token := range strings.SplitSeq(value, ",") {
					if name := http.CanonicalHeaderKey(strings.TrimSpace(token)); name != "" {
						connectionHeaders[name] = struct{}{}
					}
				}
			}
		}
	}
	for name, values := range source {
		canonical := http.CanonicalHeaderKey(name)
		if _, declared := connectionHeaders[canonical]; declared {
			continue
		}
		switch canonical {
		case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
			continue
		}
		destination[canonical] = append([]string(nil), values...)
	}
}

// CopyDownstreamResponseHeaders copies representation and protocol response
// fields while removing upstream proxy, CDN, routing, and internal metadata.
// Cache-proxy-owned diagnostics are added by the caller after this boundary.
func CopyDownstreamResponseHeaders(destination, source http.Header) {
	CopyEndToEndHeaders(destination, source)
	for name := range destination {
		canonical := http.CanonicalHeaderKey(name)
		if _, private := privateResponseHeaders[canonical]; private {
			destination.Del(name)
			continue
		}
		for _, prefix := range privateResponsePrefixes {
			if strings.HasPrefix(canonical, prefix) {
				destination.Del(name)
				break
			}
		}
	}
}
