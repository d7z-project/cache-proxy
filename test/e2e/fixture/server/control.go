package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func (s *fixtureServer) serveControl(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/__e2e/ready":
		_, _ = io.WriteString(w, "ready\n")
	case "/__e2e/count":
		key := r.URL.Query().Get("method") + " " + r.URL.Query().Get("path")
		s.mu.RLock()
		count := s.counts[key]
		s.mu.RUnlock()
		_, _ = fmt.Fprintf(w, "%d\n", count)
	case "/__e2e/prefix":
		method := r.URL.Query().Get("method")
		prefix := r.URL.Query().Get("prefix")
		s.mu.RLock()
		count := 0
		for key, value := range s.counts {
			requestMethod, requestPath, _ := strings.Cut(key, " ")
			if requestMethod == method && strings.HasPrefix(requestPath, prefix) {
				count += value
			}
		}
		s.mu.RUnlock()
		_, _ = fmt.Fprintf(w, "%d\n", count)
	case "/__e2e/counts":
		method := r.URL.Query().Get("method")
		prefix := r.URL.Query().Get("prefix")
		s.mu.RLock()
		lines := make([]string, 0, len(s.counts))
		for key, value := range s.counts {
			requestMethod, requestPath, _ := strings.Cut(key, " ")
			if requestMethod == method && strings.HasPrefix(requestPath, prefix) {
				lines = append(lines, fmt.Sprintf("%s\t%d", requestPath, value))
			}
		}
		s.mu.RUnlock()
		sort.Strings(lines)
		_, _ = fmt.Fprintln(w, strings.Join(lines, "\n"))
	case "/__e2e/header":
		key := r.URL.Query().Get("method") + " " + r.URL.Query().Get("path")
		s.mu.RLock()
		value := s.headers[key].Get(r.URL.Query().Get("name"))
		s.mu.RUnlock()
		_, _ = io.WriteString(w, value)
	case "/__e2e/reset":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		clear(s.counts)
		clear(s.headers)
		clear(s.faults)
		clear(s.cacheAges)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case "/__e2e/cache-age":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		requestPath := r.URL.Query().Get("path")
		if requestPath == "" || !strings.HasPrefix(requestPath, "/") || strings.HasPrefix(requestPath, "/__e2e/") {
			http.Error(w, "cache age path must be an absolute fixture path", http.StatusBadRequest)
			return
		}
		seconds, err := strconv.ParseInt(r.URL.Query().Get("seconds"), 10, 64)
		if err != nil || seconds < 0 || seconds > 86400 {
			http.Error(w, "cache age must be between 0 and 86400 seconds", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		if s.cacheAges == nil {
			s.cacheAges = make(map[string]int64)
		}
		s.cacheAges[requestPath] = seconds
		s.mu.Unlock()
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
			s.mu.Lock()
			if s.faults == nil {
				s.faults = make(map[string]int)
			}
			s.faults[requestPath] = status
			s.mu.Unlock()
		case http.MethodDelete:
			s.mu.Lock()
			delete(s.faults, requestPath)
			s.mu.Unlock()
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
