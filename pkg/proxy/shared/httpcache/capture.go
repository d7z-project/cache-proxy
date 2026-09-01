package httpcache

import (
	"bytes"
	"errors"
	"net/http"
)

// Capture serves a bounded protocol metadata request through handler. It is
// intended for small anchor files that a protocol wrapper must parse before
// routing the client's dependent request.
func Capture(handler http.Handler, req *http.Request, limit int64) (int, []byte, error) {
	w := &captureWriter{header: make(http.Header), limit: limit}
	handler.ServeHTTP(w, req)
	if w.overflow {
		return w.statusCode(), nil, errors.New("captured response exceeds size limit")
	}
	return w.statusCode(), w.body.Bytes(), nil
}

type captureWriter struct {
	header   http.Header
	body     bytes.Buffer
	status   int
	limit    int64
	written  int64
	overflow bool
}

func (w *captureWriter) Header() http.Header { return w.header }

func (w *captureWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *captureWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.written += int64(len(p))
	if w.written > w.limit {
		w.overflow = true
		return len(p), nil
	}
	return w.body.Write(p)
}

func (w *captureWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
