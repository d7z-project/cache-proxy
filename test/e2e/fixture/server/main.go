package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var fixtureTime = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)

type fixtureServer struct {
	root      string
	publicURL string
	countsMu  sync.RWMutex
	counts    map[string]int
	headers   map[string]http.Header
	faults    map[string]int
	updated   atomic.Bool
}

type fixtureRevision struct {
	directory    string
	packageMajor int
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
		headers:   make(map[string]http.Header),
		faults:    make(map[string]int),
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
	s.headers[r.Method+" "+r.URL.Path] = r.Header.Clone()
	if r.Method == http.MethodGet && r.Header.Get("If-None-Match") == "" && r.Header.Get("If-Modified-Since") == "" {
		s.counts["TRANSFER "+r.URL.Path]++
	}
	s.countsMu.Unlock()
	log.Printf("%s %s", r.Method, r.URL.RequestURI())
	s.countsMu.RLock()
	faultStatus := s.faults[r.URL.Path]
	s.countsMu.RUnlock()
	if faultStatus != 0 {
		http.Error(w, http.StatusText(faultStatus), faultStatus)
		return
	}
	revision := fixtureRevision{directory: "initial", packageMajor: 1}
	if s.updated.Load() {
		revision = fixtureRevision{directory: "updated", packageMajor: 2}
	}
	if strings.Contains(r.URL.Path, "/__e2e_path__/") ||
		r.URL.Path == "/file/" || r.URL.Path == "/npm/" || r.URL.Path == "/maven/" ||
		r.URL.Path == "/cargo/" || r.URL.Path == "/pypi/" || r.URL.Path == "/deb/" ||
		r.URL.Path == "/apk/" || r.URL.Path == "/rpm/" || r.URL.Path == "/pacman/" ||
		r.URL.Path == "/git/repo.git/" || r.URL.Path == "/flatpak/repo/" {
		_, _ = io.WriteString(w, r.URL.RequestURI())
		return
	}

	switch {
	case strings.HasPrefix(r.URL.Path, "/git/repo.git/"):
		s.serveGit(w, r, revision)
	case r.URL.Path == "/npm/e2e-pkg":
		s.serveNPM(w, r, revision)
	case r.URL.Path == "/npm/-/npm/v1/security/advisories/bulk" || r.URL.Path == "/npm/-/npm/v1/security/audits/quick":
		s.serveNPMAudit(w, r, revision)
	case r.URL.Path == "/cargo/config.json":
		s.serveCargoConfig(w, r, revision)
	case r.URL.Path == "/cargo/e2/e-/e2e-crate":
		s.serveCargoIndex(w, r, revision)
	case r.URL.Path == "/pypi/simple/e2e-pkg/":
		s.servePyPIProject(w, r, revision)
	case r.URL.Path == "/oci/v2" || r.URL.Path == "/oci/v2/":
		s.serveOCIBase(w)
	default:
		s.serveStatic(w, r, revision)
	}
}
