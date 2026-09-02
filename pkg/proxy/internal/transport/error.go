package transport

import (
	"net/http"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

// WriteError emits the only external form used for local proxy failures.
// Upstream and validation details belong in logs, never in the response body.
func WriteError(w http.ResponseWriter, status int) {
	proxyruntime.WriteError(w, status)
}
