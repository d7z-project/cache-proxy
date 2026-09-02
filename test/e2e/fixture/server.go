package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var fixtureTime = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)

type fixtureServer struct {
	root      string
	publicURL string
	countsMu  sync.RWMutex
	counts    map[string]int
	static    http.Handler
}

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "listen address")
	root := flag.String("root", "/srv/fixture", "fixture root")
	publicURL := flag.String("public-url", "http://127.0.0.1:19090", "client-visible fixture URL")
	flag.Parse()

	server := &fixtureServer{
		root:      *root,
		publicURL: strings.TrimRight(*publicURL, "/"),
		counts:    make(map[string]int),
		static:    http.FileServer(http.Dir(*root)),
	}
	log.Printf("fixture listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, server))
}

func (s *fixtureServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/__e2e/") {
		s.serveControl(w, r)
		return
	}
	s.countsMu.Lock()
	s.counts[r.Method+" "+r.URL.Path]++
	if r.Method == http.MethodGet && r.Header.Get("If-None-Match") == "" && r.Header.Get("If-Modified-Since") == "" {
		s.counts["TRANSFER "+r.URL.Path]++
	}
	s.countsMu.Unlock()
	log.Printf("%s %s", r.Method, r.URL.RequestURI())

	switch {
	case strings.HasPrefix(r.URL.Path, "/git/repo.git/"):
		s.serveGit(w, r)
	case r.URL.Path == "/npm/e2e-pkg":
		s.serveNPM(w, r)
	case r.URL.Path == "/npm/-/npm/v1/security/advisories/bulk" || r.URL.Path == "/npm/-/npm/v1/security/audits/quick":
		s.serveNPMAudit(w, r)
	case r.URL.Path == "/cargo/config.json":
		s.serveCargoConfig(w, r)
	case r.URL.Path == "/cargo/e2/e-/e2e-crate":
		s.serveCargoIndex(w, r)
	case r.URL.Path == "/pypi/simple/e2e-pkg/":
		s.servePyPIProject(w, r)
	case r.URL.Path == "/oci/v2" || r.URL.Path == "/oci/v2/":
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
	default:
		s.serveStatic(w, r)
	}
}

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
	case "/__e2e/mutations":
		s.countsMu.RLock()
		count := 0
		for key, value := range s.counts {
			method, _, _ := strings.Cut(key, " ")
			if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete {
				count += value
			}
		}
		s.countsMu.RUnlock()
		_, _ = fmt.Fprintf(w, "%d\n", count)
	case "/__e2e/reset":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.countsMu.Lock()
		clear(s.counts)
		s.countsMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (s *fixtureServer) serveStatic(w http.ResponseWriter, r *http.Request) {
	name := filepath.Join(s.root, filepath.FromSlash(strings.TrimPrefix(r.URL.Path, "/")))
	info, err := os.Stat(name)
	if err == nil && !info.IsDir() {
		file, openErr := os.Open(name)
		if openErr != nil {
			http.Error(w, "fixture unavailable", http.StatusInternalServerError)
			return
		}
		defer file.Close()
		digest := sha256.New()
		if _, err := io.Copy(digest, file); err != nil {
			http.Error(w, "fixture unavailable", http.StatusInternalServerError)
			return
		}
		_, _ = file.Seek(0, io.SeekStart)
		w.Header().Set("ETag", `"`+hex.EncodeToString(digest.Sum(nil))+`"`)
		w.Header().Set("Cache-Control", "public, max-age=60")
		if strings.HasPrefix(r.URL.Path, "/oci/v2/") {
			w.Header().Set("Docker-Content-Digest", "sha256:"+hex.EncodeToString(digest.Sum(nil)))
			switch {
			case strings.Contains(r.URL.Path, "/manifests/"):
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			case strings.Contains(r.URL.Path, "/blobs/"):
				w.Header().Set("Content-Type", "application/octet-stream")
			}
		}
		if contentType := mime.TypeByExtension(filepath.Ext(name)); contentType != "" && w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", contentType)
		}
		http.ServeContent(w, r, filepath.Base(name), fixtureTime, file)
		return
	}
	s.static.ServeHTTP(w, r)
}

func (s *fixtureServer) serveGit(w http.ResponseWriter, r *http.Request) {
	command := exec.Command("git", "http-backend")
	command.Env = append(os.Environ(),
		"GIT_PROJECT_ROOT="+filepath.Join(s.root, "git"),
		"GIT_HTTP_EXPORT_ALL=1",
		"PATH_INFO="+strings.TrimPrefix(r.URL.Path, "/git"),
		"QUERY_STRING="+r.URL.RawQuery,
		"REQUEST_METHOD="+r.Method,
		"CONTENT_TYPE="+r.Header.Get("Content-Type"),
		"CONTENT_LENGTH="+strconv.FormatInt(r.ContentLength, 10),
		"REMOTE_USER=",
		"REMOTE_ADDR=127.0.0.1",
	)
	command.Stdin = r.Body
	output, err := command.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("git http-backend failed: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		http.Error(w, "git backend failed", http.StatusInternalServerError)
		return
	}
	headerBlock, body, ok := strings.Cut(string(output), "\r\n\r\n")
	if !ok {
		http.Error(w, "invalid git backend response", http.StatusInternalServerError)
		return
	}
	status := http.StatusOK
	for _, line := range strings.Split(headerBlock, "\r\n") {
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		if strings.EqualFold(name, "Status") {
			fields := strings.Fields(value)
			if len(fields) != 0 {
				status, _ = strconv.Atoi(fields[0])
			}
			continue
		}
		w.Header().Add(name, value)
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, body)
	}
}

func (s *fixtureServer) serveNPM(w http.ResponseWriter, r *http.Request) {
	archive, err := os.ReadFile(filepath.Join(s.root, "npm", "e2e-pkg-1.0.0.tgz"))
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	sha1Digest := sha1.Sum(archive)
	sha512Digest := sha512.Sum512(archive)
	document := map[string]any{
		"name":      "e2e-pkg",
		"dist-tags": map[string]string{"latest": "1.0.0"},
		"versions": map[string]any{"1.0.0": map[string]any{
			"name": "e2e-pkg", "version": "1.0.0",
			"dist": map[string]string{
				"tarball":   s.publicURL + "/npm/e2e-pkg/-/e2e-pkg-1.0.0.tgz",
				"shasum":    hex.EncodeToString(sha1Digest[:]),
				"integrity": "sha512-" + base64.StdEncoding.EncodeToString(sha512Digest[:]),
			},
		}},
	}
	s.writeJSON(w, r, document)
}

func (s *fixtureServer) serveNPMAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/advisories/bulk") {
		s.writeJSON(w, r, map[string]any{})
		return
	}
	document := map[string]any{
		"auditReportVersion": 2,
		"vulnerabilities":    map[string]any{},
		"metadata": map[string]any{
			"vulnerabilities": map[string]int{"info": 0, "low": 0, "moderate": 0, "high": 0, "critical": 0, "total": 0},
			"dependencies":    map[string]int{"prod": 1, "dev": 0, "optional": 0, "peer": 0, "peerOptional": 0, "total": 1},
		},
	}
	s.writeJSON(w, r, document)
}

func (s *fixtureServer) serveCargoConfig(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, map[string]any{"dl": s.publicURL + "/cargo/api/v1/crates", "api": s.publicURL + "/cargo"})
}

func (s *fixtureServer) serveCargoIndex(w http.ResponseWriter, r *http.Request) {
	archive, err := os.ReadFile(filepath.Join(s.root, "cargo", "api", "v1", "crates", "e2e-crate", "1.0.0", "download"))
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	digest := sha256.Sum256(archive)
	record := map[string]any{
		"name": "e2e-crate", "vers": "1.0.0", "deps": []any{},
		"cksum": hex.EncodeToString(digest[:]), "features": map[string]any{}, "yanked": false,
	}
	body, _ := json.Marshal(record)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("ETag", `"cargo-index"`)
	if r.Method != http.MethodHead {
		_, _ = w.Write(append(body, '\n'))
	}
}

func (s *fixtureServer) servePyPIProject(w http.ResponseWriter, r *http.Request) {
	wheel := "e2e_pkg-1.0.0-py3-none-any.whl"
	body, err := os.ReadFile(filepath.Join(s.root, "pypi", "files", wheel))
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	digest := sha256.Sum256(body)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	if r.Method != http.MethodHead {
		_, _ = fmt.Fprintf(w, `<!doctype html><a href="%s/pypi/files/%s#sha256=%x">%s</a>`, s.publicURL, wheel, digest, wheel)
	}
}

func (s *fixtureServer) writeJSON(w http.ResponseWriter, r *http.Request, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	digest := sha256.Sum256(body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("ETag", `"`+hex.EncodeToString(digest[:])+`"`)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
