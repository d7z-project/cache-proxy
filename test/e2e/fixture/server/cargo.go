package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

func (s *fixtureServer) serveCargoConfig(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	s.writeJSON(w, r, map[string]any{"dl": s.publicURL + "/cargo/api/v1/crates", "api": s.publicURL + "/cargo"}, revision)
}

func (s *fixtureServer) serveCargoIndex(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	var body bytes.Buffer
	for major := 1; major <= revision.packageMajor; major++ {
		version := fmt.Sprintf("%d.0.0", major)
		archive, err := os.ReadFile(filepath.Join(s.root, revision.directory, "cargo", "api", "v1", "crates", "e2e-crate", version, "download"))
		if err != nil {
			http.Error(w, "fixture unavailable", http.StatusInternalServerError)
			return
		}
		digest := sha256.Sum256(archive)
		record := map[string]any{
			"name": "e2e-crate", "vers": version, "deps": []any{},
			"cksum": hex.EncodeToString(digest[:]), "features": map[string]any{}, "yanked": false,
		}
		encoded, _ := json.Marshal(record)
		body.Write(encoded)
		body.WriteByte('\n')
	}
	s.writeBody(w, r, "text/plain", body.Bytes(), revision)
}
