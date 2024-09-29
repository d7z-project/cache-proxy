package services

import (
	"github.com/pkg/errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Worker struct {
	baseDir       string
	locker        sync.RWMutex
	targets       map[string]*Target
	sortedTargets []string
	ticker        *time.Ticker
	closed        atomic.Bool
}

func NewWorker(baseDir string, gc time.Duration) (*Worker, error) {
	if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
		return nil, err
	}
	w := &Worker{
		baseDir: baseDir,
		ticker:  time.NewTicker(gc),
		locker:  sync.RWMutex{},
		targets: make(map[string]*Target),
	}
	go func(t *Worker) {
		for {
			<-t.ticker.C
			w.Refresh()
		}
	}(w)
	return w, nil
}

func (w *Worker) Bind(name string, target *Target) error {
	name = strings.Trim(name, "/")
	key := "/" + name + "/"
	w.locker.Lock()
	defer w.locker.Unlock()
	if w.targets[key] != nil {
		return errors.New("target already exists")
	}
	if target.meta != nil || target.blobs != nil {
		return errors.New("cannot bind to a non-empty meta")
	}
	var err error
	target.blobs, err = NewBlobs(filepath.Join(w.baseDir, name, "blobs"))
	if err != nil {
		return err
	}
	target.meta, err = NewFileMeta(filepath.Join(w.baseDir, name, "meta"))
	if err != nil {
		return err
	}
	w.targets[key] = target
	w.sortedTargets = append(w.sortedTargets, key)
	slices.Sort(w.sortedTargets)
	slices.Reverse(w.sortedTargets)
	return nil
}

func (w *Worker) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if w.closed.Load() {
		resp.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if req.Method != "GET" && req.Method != "HEAD" {
		resp.Header().Add("Allow", "GET, HEAD")
		resp.Header().Add("Content-Type", "text/plain")
		resp.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = resp.Write([]byte(http.StatusText(http.StatusMethodNotAllowed)))
		return
	}
	w.locker.RLock()
	defer w.locker.RUnlock()
	for _, target := range w.sortedTargets {
		if strings.HasPrefix(req.RequestURI, target) {
			w.targets[target].forward(resp, req, target)
			return
		}
	}
	resp.WriteHeader(http.StatusNotFound)
}

func (w *Worker) Close() error {
	w.locker.RLock()
	defer w.locker.RUnlock()
	w.ticker.Stop()
	for name, target := range w.targets {
		log.Printf("销毁代理目标 %s", name)
		if err := target.Close(); err != nil {
			log.Printf("销毁 %s 失败: %v", name, err)
		}
	}
	return nil
}

func (w *Worker) Refresh() {

}
