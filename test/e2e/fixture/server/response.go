package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"
)

func (s *fixtureServer) writeJSON(w http.ResponseWriter, r *http.Request, value any, revision fixtureRevision) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	s.writeBody(w, r, "application/json", body, revision)
}

func (*fixtureServer) writeBody(w http.ResponseWriter, r *http.Request, contentType string, body []byte, revision fixtureRevision) {
	digest := sha256.Sum256(body)
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("ETag", `"`+hex.EncodeToString(digest[:])+`"`)
	http.ServeContent(w, r, "", fixtureTime.Add(time.Duration(revision.packageMajor)*time.Hour), bytes.NewReader(body))
}
