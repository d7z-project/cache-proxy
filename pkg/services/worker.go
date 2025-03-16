package services

import (
	_ "embed"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"code.d7z.net/d7z-team/blobfs"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

//go:embed default.tmpl
var data []byte

type Worker struct {
	baseDir       string
	locker        sync.RWMutex
	blobs         *blobfs.FSBlob
	targets       map[string]*Target
	sortedTargets []string
	metaTicker    *time.Ticker
	blobTicker    *time.Ticker
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
		baseDir:    baseDir,
		metaTicker: time.NewTicker(metaGc),
		blobTicker: time.NewTicker(blobGc),
		locker:     sync.RWMutex{},
		targets:    make(map[string]*Target),
		html:       tmpl,
	}
	w.blobs, err = blobfs.BlobFS(baseDir)
	if err != nil {
		w.metaTicker.Stop()
		w.blobTicker.Stop()
		return nil, err
	}
	go func(t *Worker) {
		for {
			<-t.metaTicker.C
			w.MetaRefresh()
		}
	}(w)
	go func(t *Worker) {
		for {
			<-t.blobTicker.C
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
	target.storage = w.blobs.Child(name)
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
			path := req.RequestURI[len(target):]
			zap.L().Debug("转发到目标", zap.String("target", target), zap.String("path", path))
			forward, err := w.targets[target].forward(req.Context(), path)
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
	w.metaTicker.Stop()
	w.blobTicker.Stop()

	for name, target := range w.targets {
		log.Printf("销毁代理目标 %s", name)
		if err := target.Close(); err != nil {
			log.Printf("销毁 %s 失败: %v", name, err)
		}
	}
	return nil
}

func (w *Worker) BlobRefresh() {
	err := w.blobs.BlobGC()
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
