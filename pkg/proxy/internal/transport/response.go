package transport

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
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

// ServeStoredResponse serves a validated cached representation and releases its
// reader lease. The filename controls MIME inference when Content-Type is absent.
func ServeStoredResponse(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, filename, cacheResult string) int {
	defer object.Reader.Close()
	CopyEndToEndHeaders(w.Header(), object.ResponseHeader())
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("X-Cache", cacheResult)
	recorder := &contentStatusWriter{ResponseWriter: w, status: http.StatusOK}
	http.ServeContent(recorder, request, filename, object.ValidatedAt, object.Reader)
	return recorder.status
}

// contentStatusWriter records ServeContent's conditional and range statuses.
type contentStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *contentStatusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *contentStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
