package services

import (
	_ "embed"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

//go:embed default.tmpl
var data []byte

type Worker struct {
	baseDir       string
	locker        sync.RWMutex
	blobs         *Blobs
	targets       map[string]*Target
	sortedTargets []string
	metaTracker   *time.Ticker
	blobTracker   *time.Ticker
	closed        atomic.Bool

	html *template.Template
}

func NewWorker(baseDir string, metaGc time.Duration, blobGc time.Duration) (*Worker, error) {
	if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
		return nil, err
	}

	tmpl, _ := template.New("memory").Parse(string(data))
	var err error
	w := &Worker{
		baseDir:     baseDir,
		metaTracker: time.NewTicker(metaGc),
		blobTracker: time.NewTicker(blobGc),
		locker:      sync.RWMutex{},
		targets:     make(map[string]*Target),
		html:        tmpl,
	}
	w.blobs, err = NewBlobs(filepath.Join(baseDir, "blobs"))
	if err != nil {
		w.metaTracker.Stop()
		w.blobTracker.Stop()
		return nil, err
	}
	go func(t *Worker) {
		for {
			<-t.metaTracker.C
			w.MetaRefresh()
		}
	}(w)
	go func(t *Worker) {
		for {
			<-t.blobTracker.C
			w.BlobRefresh()
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
	target.blobs = w.blobs
	target.meta, err = NewFileMeta(filepath.Join(w.baseDir, "meta", name))
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
	if strings.HasSuffix(req.URL.Path, "robots.txt") {
		resp.Header().Set("Content-Type", "text/plain")
		resp.WriteHeader(http.StatusOK)
		_, _ = resp.Write([]byte("User-agent: *\nDisallow: /\n"))
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
			forward, err := w.targets[target].forward(req.RequestURI[len(target):])
			if err != nil {
				resp.Header().Add("Content-Type", "text/html; charset=utf-8")
				resp.WriteHeader(http.StatusNotFound)
				_ = w.html.Execute(resp, map[string]any{
					"code":    http.StatusNotFound,
					"error":   err,
					"path":    req.RequestURI,
					"request": req,
				})
				zap.L().Debug("target error", zap.String("target", target), zap.Error(err))
				return
			}
			_ = forward.FlushClose(req, resp)
			return
		}
	}
	resp.Header().Add("Content-Type", "text/html; charset=utf-8")
	if req.RequestURI == "/" {
		resp.WriteHeader(http.StatusOK)
		_ = w.html.Execute(resp, map[string]any{
			"code":    http.StatusOK,
			"path":    req.RequestURI,
			"request": req,
			"routes":  w.sortedTargets,
		})
	} else {
		resp.WriteHeader(http.StatusNotFound)
		_ = w.html.Execute(resp, map[string]any{
			"code":    http.StatusNotFound,
			"path":    req.RequestURI,
			"request": req,
		})
	}
}

func (w *Worker) Close() error {
	w.closed.Swap(true)
	w.locker.RLock()
	defer w.locker.RUnlock()
	w.metaTracker.Stop()
	w.blobTracker.Stop()

	for name, target := range w.targets {
		log.Printf("销毁代理目标 %s ,可能存在请求，需等待所有请求结束", name)
		if err := target.Close(); err != nil {
			log.Printf("销毁 %s 失败: %v", name, err)
		}
	}
	return nil
}

func (w *Worker) BlobRefresh() {
	err := w.blobs.Gc()
	if err != nil {
		zap.L().Warn("blob gc 失败", zap.Error(err))
	}
}

func (w *Worker) MetaRefresh() {
	for name, target := range w.targets {
		if err := target.Gc(); err != nil {
			zap.L().Error("Meta GC 执行失败", zap.String("target", name), zap.Error(err))
		}
	}
}

func (w *Worker) SetHtmlPage(tmpl *template.Template) {
	w.html = tmpl
}
