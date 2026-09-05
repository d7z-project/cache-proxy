package transport

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

// EscapePathSegments preserves path separators while escaping each protocol
// path segment for use in an upstream URL.
func EscapePathSegments(value string) string {
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

// SourceRevalidationHeader preserves downstream validators for rewritten bodies.
func SourceRevalidationHeader(source http.Header) http.Header {
	header := source.Clone()
	for _, name := range []string{"ETag", "Last-Modified"} {
		if value := header.Get(name); value != "" {
			header.Set("X-Source-"+name, value)
		}
		header.Del(name)
	}
	return header
}

// ResponseCacheable applies the response policy shared by package protocols.
// allowPrivate is true only when the cache key is scoped to the request's
// credentials.
func ResponseCacheable(response *http.Response, allowPrivate bool) bool {
	if response.Request != nil && proxyruntime.ParseCachePolicy(response.Request.Header, time.Now(), 0).NoStore {
		return false
	}
	policy := proxyruntime.ParseCachePolicy(response.Header, time.Now(), 0)
	if policy.NoStore || strings.Contains(response.Header.Get("Vary"), "*") {
		return false
	}
	private := policy.Private || response.Header.Get("Set-Cookie") != ""
	return !private || allowPrivate
}

// WriteResponse forwards an upstream response and closes its body.
func WriteResponse(w http.ResponseWriter, request *http.Request, response *http.Response, cacheResult string) int {
	defer func() { _ = response.Body.Close() }()
	CopyEndToEndHeaders(w.Header(), response.Header)
	w.Header().Set("X-Cache", cacheResult)
	w.WriteHeader(response.StatusCode)
	if request.Method != http.MethodHead {
		_, _ = io.Copy(w, response.Body)
	}
	return response.StatusCode
}
