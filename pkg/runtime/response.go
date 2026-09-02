package runtime

import (
	"io"
	"net/http"
)

// WriteError emits the canonical external form for local proxy failures.
func WriteError(w http.ResponseWriter, status int) {
	body := http.StatusText(status)
	if status >= http.StatusInternalServerError {
		body = "internal error"
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if w.Header().Get("X-Cache") == "" {
		w.Header().Set("X-Cache", "ERROR")
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body+"\n")
}

// RequireReadMethod rejects methods that cannot be served by a read-only
// package proxy. Protocols with a narrowly defined read-only POST operation
// must classify that operation before calling this helper.
func RequireReadMethod(w http.ResponseWriter, method string) bool {
	if method == http.MethodGet || method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	w.Header().Set("X-Cache", "REJECTED")
	WriteError(w, http.StatusMethodNotAllowed)
	return false
}
