package services

import (
	"net/http"
	"path/filepath"
	"sync"
)

type Worker struct {
	baseDir string
	blobs   *Blobs
	meta    *FileMeta
	locker  sync.RWMutex
	route   map[string]any
}

func NewWorker(baseDir string) (*Worker, error) {
	blobs, err := NewBlobs(filepath.Join(baseDir, "blobs"))
	if err != nil {
		return nil, err
	}
	meta, err := NewFileMeta(filepath.Join(baseDir, "meta"))
	if err != nil {
		return nil, err
	}
	return &Worker{
		baseDir: baseDir,
		blobs:   blobs,
		meta:    meta,
	}, nil
}

func (w *Worker) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" && req.Method != "HEAD" {
		resp.Header().Add("Allow", "GET, HEAD")
		resp.Header().Add("Content-Type", "text/plain")
		resp.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = resp.Write([]byte(http.StatusText(http.StatusMethodNotAllowed)))
		return
	}

}
