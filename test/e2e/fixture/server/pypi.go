package main

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

func (s *fixtureServer) servePyPIProject(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	version := fmt.Sprintf("%d.0.0", revision.packageMajor)
	wheel := "e2e_pkg-" + version + "-py3-none-any.whl"
	body, err := os.ReadFile(filepath.Join(s.root, revision.directory, "pypi", "files", wheel))
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	digest := sha256.Sum256(body)
	document := fmt.Sprintf(`<!doctype html><a href="%s/pypi/files/%s#sha256=%x">%s</a>`, s.publicURL, wheel, digest, wheel)
	s.writeBody(w, r, "text/html; charset=utf-8", []byte(document), revision)
}
