package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

func (s *fixtureServer) serveControl(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/__e2e/ready":
		_, _ = io.WriteString(w, "ready\n")
	case "/__e2e/count":
		key := r.URL.Query().Get("method") + " " + r.URL.Query().Get("path")
		s.countsMu.RLock()
		count := s.counts[key]
		s.countsMu.RUnlock()
		_, _ = fmt.Fprintf(w, "%d\n", count)
	case "/__e2e/prefix":
		method := r.URL.Query().Get("method")
		prefix := r.URL.Query().Get("prefix")
		s.countsMu.RLock()
		count := 0
		for key, value := range s.counts {
			requestMethod, requestPath, _ := strings.Cut(key, " ")
			if requestMethod == method && strings.HasPrefix(requestPath, prefix) {
				count += value
			}
		}
		s.countsMu.RUnlock()
		_, _ = fmt.Fprintf(w, "%d\n", count)
	case "/__e2e/counts":
		method := r.URL.Query().Get("method")
		prefix := r.URL.Query().Get("prefix")
		s.countsMu.RLock()
		lines := make([]string, 0, len(s.counts))
		for key, value := range s.counts {
			requestMethod, requestPath, _ := strings.Cut(key, " ")
			if requestMethod == method && strings.HasPrefix(requestPath, prefix) {
				lines = append(lines, fmt.Sprintf("%s\t%d", requestPath, value))
			}
		}
		s.countsMu.RUnlock()
		sort.Strings(lines)
		_, _ = fmt.Fprintln(w, strings.Join(lines, "\n"))
	case "/__e2e/header":
		key := r.URL.Query().Get("method") + " " + r.URL.Query().Get("path")
		s.countsMu.RLock()
		value := s.headers[key].Get(r.URL.Query().Get("name"))
		s.countsMu.RUnlock()
		_, _ = io.WriteString(w, value)
	case "/__e2e/reset":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.countsMu.Lock()
		clear(s.counts)
		clear(s.headers)
		clear(s.faults)
		s.countsMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case "/__e2e/fault":
		requestPath := r.URL.Query().Get("path")
		if requestPath == "" || !strings.HasPrefix(requestPath, "/") || strings.HasPrefix(requestPath, "/__e2e/") {
			http.Error(w, "fault path must be an absolute fixture path", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPost:
			status := 0
			switch r.URL.Query().Get("status") {
			case "403":
				status = http.StatusForbidden
			case "404":
				status = http.StatusNotFound
			default:
				http.Error(w, "fault status must be 403 or 404", http.StatusBadRequest)
				return
			}
			s.countsMu.Lock()
			if s.faults == nil {
				s.faults = make(map[string]int)
			}
			s.faults[requestPath] = status
			s.countsMu.Unlock()
		case http.MethodDelete:
			s.countsMu.Lock()
			delete(s.faults, requestPath)
			s.countsMu.Unlock()
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "/__e2e/state":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Query().Get("value") {
		case "initial":
			s.updated.Store(false)
		case "updated":
			s.updated.Store(true)
		default:
			http.Error(w, "state must be initial or updated", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}
