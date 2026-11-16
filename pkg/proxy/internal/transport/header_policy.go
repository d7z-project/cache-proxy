package transport

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/net/http/httpguts"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

var upstreamReadHeaders = []string{
	"Accept",
	"Accept-Charset",
	"Accept-Encoding",
	"Accept-Language",
	"Authorization",
	"Cache-Control",
	"Cookie",
	"If-Match",
	"If-Modified-Since",
	"If-None-Match",
	"If-Range",
	"If-Unmodified-Since",
	"Pragma",
	"Range",
	"Want-Content-Digest",
	"Want-Digest",
	"Want-Repr-Digest",
}

var upstreamEntityHeaders = []string{
	"Content-Digest",
	"Content-Encoding",
	"Content-Language",
	"Content-MD5",
	"Content-Type",
	"Digest",
	"Repr-Digest",
}

var crossOriginRedirectHeaders = map[string]struct{}{
	"Accept":              {},
	"Accept-Charset":      {},
	"Accept-Encoding":     {},
	"Accept-Language":     {},
	"Cache-Control":       {},
	"If-Match":            {},
	"If-Modified-Since":   {},
	"If-None-Match":       {},
	"If-Range":            {},
	"If-Unmodified-Since": {},
	"Pragma":              {},
	"Range":               {},
	"Want-Content-Digest": {},
	"Want-Digest":         {},
	"Want-Repr-Digest":    {},
}

var protectedRequestHeaders = map[string]struct{}{
	"B3":                       {},
	"Baggage":                  {},
	"Cdn-Loop":                 {},
	"Client-Ip":                {},
	"Content-Length":           {},
	"Early-Data":               {},
	"Expect":                   {},
	"Forwarded":                {},
	"Grpc-Trace-Bin":           {},
	"Host":                     {},
	"Origin":                   {},
	"Referer":                  {},
	"Remote-User":              {},
	"Sentry-Trace":             {},
	"Traceparent":              {},
	"Tracestate":               {},
	"True-Client-Ip":           {},
	"Uber-Trace-Id":            {},
	"User-Agent":               {},
	"Via":                      {},
	"X-Arr-Clientcert":         {},
	"X-Authenticated-User":     {},
	"X-Backend":                {},
	"X-Backend-Server":         {},
	"X-Client-Cert":            {},
	"X-Client-Ip":              {},
	"X-Cluster-Client-Ip":      {},
	"X-Cloud-Trace-Context":    {},
	"X-Correlation-Id":         {},
	"X-Forwarded-Client-Cert":  {},
	"X-Ot-Span-Context":        {},
	"X-Originating-Ip":         {},
	"X-Proxyuser-Ip":           {},
	"X-Real-Ip":                {},
	"X-Remote-Ip":              {},
	"X-Remote-User":            {},
	"X-Request-Id":             {},
	"X-Request-Start":          {},
	"X-Ssl-Client-Cert":        {},
	"X-Upstream":               {},
	"X-Upstream-Address":       {},
	"X-Upstream-Host":          {},
	"X-Upstream-Name":          {},
	"X-Upstream-Response-Time": {},
	"X-Upstream-Service-Time":  {},
}

var protectedRequestPrefixes = []string{
	"Cf-",
	"Fastly-",
	"Fly-",
	"X-Accel-",
	"X-Amzn-",
	"X-Appengine-",
	"X-Auth-Request-",
	"X-Azure-",
	"X-B3-",
	"X-Datadog-",
	"X-Envoy-",
	"X-Fastly-",
	"X-Forwarded-",
	"X-Internal-",
	"X-Original-",
	"X-Ot-",
	"X-Proxy-",
	"X-Request-",
	"X-Rewrite-",
	"X-Sentry-",
	"X-Trace-",
	"X-Vercel-",
	"Sec-",
}

// CopyUpstreamReadHeaders copies the finite set of representation and
// authentication fields supported by bodyless upstream requests. Extra fields
// are protocol-owned extensions and still pass through the final boundary.
func CopyUpstreamReadHeaders(destination, source http.Header, extra ...string) {
	copySelectedHeaders(destination, source, upstreamReadHeaders)
	copySelectedHeaders(destination, source, extra)
	SanitizeReadRequestHeaders(destination)
}

// CopyUpstreamPostHeaders copies fields needed by the supported read-only POST
// operations. Publishing methods are rejected before reaching this boundary.
func CopyUpstreamPostHeaders(destination, source http.Header, extra ...string) {
	copySelectedHeaders(destination, source, upstreamReadHeaders)
	copySelectedHeaders(destination, source, upstreamEntityHeaders)
	copySelectedHeaders(destination, source, extra)
	SanitizeMethodOverrideHeaders(destination)
}

func copySelectedHeaders(destination, source http.Header, names []string) {
	for _, name := range names {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" {
			continue
		}
		destination.Del(canonical)
		for _, value := range source.Values(canonical) {
			destination.Add(canonical, value)
		}
	}
}

// ValidatePassHeader rejects configurable fields that could cross the fixed
// request privacy or HTTP connection boundary.
func ValidatePassHeader(name string) error {
	name = strings.TrimSpace(name)
	if !httpguts.ValidHeaderFieldName(name) {
		return errors.New("must be a valid HTTP header name")
	}
	canonical := http.CanonicalHeaderKey(name)
	if protectedUpstreamRequestHeader(canonical) {
		return fmt.Errorf("header %q is reserved by the upstream privacy boundary", canonical)
	}
	for _, managed := range upstreamReadHeaders {
		if canonical == managed {
			return fmt.Errorf("header %q is already managed for upstream reads", canonical)
		}
	}
	test := make(http.Header)
	test.Set(canonical, "value")
	clean := make(http.Header)
	proxyruntime.CopyEndToEndHeaders(clean, test)
	if clean.Get(canonical) == "" {
		return fmt.Errorf("header %q is connection-specific", canonical)
	}
	SanitizeReadRequestHeaders(clean)
	if clean.Get(canonical) == "" {
		return fmt.Errorf("header %q is not valid on bodyless upstream reads", canonical)
	}
	return nil
}

// SanitizeUpstreamRequest is the final boundary applied to every actual
// transport hop, including redirects and OCI authentication requests.
func SanitizeUpstreamRequest(request *http.Request, userAgent string) {
	if request == nil {
		return
	}
	header := make(http.Header, len(request.Header))
	proxyruntime.CopyEndToEndHeaders(header, request.Header)
	for name := range header {
		if protectedUpstreamRequestHeader(name) {
			header.Del(name)
		}
	}
	SanitizeMethodOverrideHeaders(header)
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	header.Set("User-Agent", userAgent)
	header.Set("Via", "1.1 gateway")
	request.Host = ""
	request.Header = header
}

func protectedUpstreamRequestHeader(name string) bool {
	canonical := http.CanonicalHeaderKey(name)
	if _, protected := protectedRequestHeaders[canonical]; protected {
		return true
	}
	for _, prefix := range protectedRequestPrefixes {
		if strings.HasPrefix(canonical, prefix) {
			return true
		}
	}
	return false
}

func retainCrossOriginRedirectHeaders(header http.Header) {
	for name := range header {
		if _, keep := crossOriginRedirectHeaders[http.CanonicalHeaderKey(name)]; !keep {
			delete(header, name)
		}
	}
}

// SanitizeUpstreamResponse removes fields that describe or control upstream
// proxy/CDN infrastructure before protocol code can persist or forward them.
func SanitizeUpstreamResponse(response *http.Response) {
	if response == nil {
		return
	}
	header := make(http.Header, len(response.Header))
	proxyruntime.CopyDownstreamResponseHeaders(header, response.Header)
	response.Header = header
}

// CopyDownstreamResponseHeaders also protects responses read from historical
// cache entries that predate the transport boundary.
func CopyDownstreamResponseHeaders(destination, source http.Header) {
	proxyruntime.CopyDownstreamResponseHeaders(destination, source)
}
