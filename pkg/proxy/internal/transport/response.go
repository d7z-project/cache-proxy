package transport

import (
	"io"
	"net/http"
	"net/url"
	"strings"
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

// RequestForcesRevalidation reports whether the request explicitly asks the
// proxy to validate a cached response with its origin.
func RequestForcesRevalidation(request *http.Request) bool {
	control := strings.ToLower(request.Header.Get("Cache-Control"))
	return strings.Contains(control, "no-cache") || strings.Contains(control, "max-age=0")
}

// ResponseCacheable applies the response policy shared by package protocols.
// allowPrivate is true only when the cache key is scoped to the request's
// credentials.
func ResponseCacheable(response *http.Response, allowPrivate bool) bool {
	control := strings.ToLower(response.Header.Get("Cache-Control"))
	if strings.Contains(control, "no-store") || strings.Contains(response.Header.Get("Vary"), "*") {
		return false
	}
	private := strings.Contains(control, "private") || response.Header.Get("Set-Cookie") != ""
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
