package main

import (
	"crypto/sha1"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (s *fixtureServer) serveNPM(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	version := fmt.Sprintf("%d.0.0", revision.packageMajor)
	archive, err := os.ReadFile(filepath.Join(s.root, revision.directory, "npm", "e2e-pkg", "-", "e2e-pkg-"+version+".tgz"))
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	sha1Digest := sha1.Sum(archive)
	sha512Digest := sha512.Sum512(archive)
	document := map[string]any{
		"name":      "e2e-pkg",
		"dist-tags": map[string]string{"latest": version},
		"versions": map[string]any{version: map[string]any{
			"name": "e2e-pkg", "version": version,
			"dist": map[string]string{
				"tarball":   s.publicURL + "/npm/e2e-pkg/-/e2e-pkg-" + version + ".tgz",
				"shasum":    hex.EncodeToString(sha1Digest[:]),
				"integrity": "sha512-" + base64.StdEncoding.EncodeToString(sha512Digest[:]),
			},
		}},
	}
	s.writeJSON(w, r, document, revision)
}

func (s *fixtureServer) serveNPMAudit(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/advisories/bulk") {
		s.writeJSON(w, r, map[string]any{}, revision)
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
	s.writeJSON(w, r, document, revision)
}
