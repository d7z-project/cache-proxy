package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *fixtureServer) serveStatic(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	name := filepath.Join(s.root, revision.directory, filepath.FromSlash(strings.TrimPrefix(r.URL.Path, "/")))
	info, err := os.Stat(name)
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	file, err := os.Open(name)
	if err != nil {
		http.Error(w, "fixture unavailable", http.StatusInternalServerError)
		return
	}
	defer func() { _ = file.Close() }()
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
	http.ServeContent(w, r, filepath.Base(name), fixtureTime.Add(time.Duration(revision.packageMajor)*time.Hour), file)
}
