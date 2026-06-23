package app

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	httpproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const (
	DefaultBackend     = "/tmp/cache-proxy"
	DefaultBind        = "127.0.0.1:18080"
	DefaultMetricsPath = "/metrics"
)

type App struct {
	config       *config.Document
	store        *blobfs.Store
	stats        *httpproxy.Stats
	metricsReg   *prometheus.Registry
	entries      map[string]*proxyruntime.Entry
	handlers     []proxyruntime.Instance
	pathHandlers map[string]http.Handler
	pathPrefixes []string
	bindHandlers map[string]http.Handler
	bindServers  map[string]*http.Server
	mainServer   *http.Server
	lifecycleCtx context.Context
	stopRuntime  context.CancelFunc
	started      bool
	closed       atomic.Bool
	gcDone       chan struct{}
	cleanupDone  chan struct{}
}

func Load(path string) (*config.Document, error) {
	return config.LoadFile(path)
}

func Validate(doc *config.Document) error {
	dir, err := os.MkdirTemp("", "cache-proxy-validate-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	copy := *doc
	copy.Server.Backend = dir
	store, err := blobfs.Open(dir, blobfs.DefaultConfig())
	if err != nil {
		return err
	}
	defer store.Close()

	registry := prometheus.NewRegistry()
	stats := httpproxy.NewStats(registry)
	_, err = planEntries(context.Background(), &copy, store, stats)
	return err
}

func Open(ctx context.Context, doc *config.Document) (*App, error) {
	if doc == nil {
		return nil, errors.New("config document is nil")
	}
	normalizeDocument(doc)
	utils.CleanStaleTempFiles(24 * time.Hour)
	if err := validateServerConfig(doc); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(doc.Server.Backend, 0o755); err != nil {
		return nil, err
	}

	store, err := blobfs.Open(doc.Server.Backend, blobfs.DefaultConfig())
	if err != nil {
		return nil, err
	}
	metricsReg := prometheus.NewRegistry()
	metricsReg.MustRegister(metrics.NewBlobFSCollector(store))
	stats := httpproxy.NewStats(metricsReg)
	entries, err := planEntries(ctx, doc, store, stats)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	lifecycleCtx, stopRuntime := context.WithCancel(context.Background())
	app := &App{
		config:       doc,
		store:        store,
		stats:        stats,
		metricsReg:   metricsReg,
		entries:      entries,
		pathHandlers: map[string]http.Handler{},
		bindHandlers: map[string]http.Handler{},
		bindServers:  map[string]*http.Server{},
		lifecycleCtx: lifecycleCtx,
		stopRuntime:  stopRuntime,
		gcDone:       make(chan struct{}),
		cleanupDone:  make(chan struct{}),
	}
	if err := app.prepareHandlers(lifecycleCtx); err != nil {
		_ = store.Close()
		return nil, err
	}
	app.mainServer = &http.Server{Addr: doc.Server.Bind, Handler: app}
	go app.gcLoop()
	go app.cleanupLoop()
	return app, nil
}

func (a *App) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet && req.URL.Path == "/" {
		a.serveHome(w, req)
		return
	}
	if req.URL.Path == a.config.Metrics.Path {
		metricsAuthMiddleware(a.config.Metrics.Token, promhttp.HandlerFor(
			prometheus.Gatherers{prometheus.DefaultGatherer, a.metricsReg},
			promhttp.HandlerOpts{},
		)).ServeHTTP(w, req)
		return
	}
	prefix := a.matchProxyPrefix(req.URL.Path)
	if prefix == "" {
		http.NotFound(w, req)
		return
	}
	next := req.Clone(req.Context())
	next.Header = req.Header.Clone()
	next.Header.Set("X-Cache-Proxy-Prefix", prefix)
	http.StripPrefix(prefix, a.pathHandlers[prefix]).ServeHTTP(w, next)
}

func (a *App) Start() error {
	if a.started {
		return nil
	}
	mainListener, err := net.Listen("tcp", a.config.Server.Bind)
	if err != nil {
		return fmt.Errorf("listen %s: %w", a.config.Server.Bind, err)
	}
	prepared := make(map[string]net.Listener, len(a.bindHandlers))
	for addr := range a.bindHandlers {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			_ = mainListener.Close()
			for _, item := range prepared {
				_ = item.Close()
			}
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		prepared[addr] = listener
	}

	a.started = true
	go func() {
		if err := a.mainServer.Serve(mainListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("main server error", "addr", a.config.Server.Bind, "err", err)
		}
	}()
	for addr, listener := range prepared {
		server := &http.Server{Addr: addr, Handler: a.bindHandlers[addr]}
		a.bindServers[addr] = server
		go func(server *http.Server, listener net.Listener) {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("bind server error", "addr", server.Addr, "err", err)
			}
		}(server, listener)
	}
	return nil
}

func (a *App) Close(ctx context.Context) error {
	if a.closed.Swap(true) {
		return nil
	}
	a.stopRuntime()

	var joined error
	if a.mainServer != nil {
		joined = errors.Join(joined, a.mainServer.Shutdown(ctx))
	}
	for _, server := range a.bindServers {
		joined = errors.Join(joined, server.Shutdown(ctx))
	}
	for _, handler := range a.handlers {
		joined = errors.Join(joined, handler.Stop(ctx))
	}
	select {
	case <-a.gcDone:
	case <-ctx.Done():
		joined = errors.Join(joined, ctx.Err())
	}
	select {
	case <-a.cleanupDone:
	case <-ctx.Done():
		joined = errors.Join(joined, ctx.Err())
	}
	if a.store != nil {
		joined = errors.Join(joined, a.store.Close())
	}
	return joined
}

func (a *App) prepareHandlers(ctx context.Context) error {
	for _, name := range sortedEntryNames(a.entries) {
		entry := a.entries[name]
		if !entry.Enabled || entry.Runtime == nil {
			continue
		}
		if err := entry.Runtime.Start(ctx); err != nil {
			a.stopHandlers()
			return fmt.Errorf("instance %s: %w", entry.Name, err)
		}
		a.handlers = append(a.handlers, entry.Runtime)
		if entry.Path != "" {
			a.pathHandlers[entry.Path] = entry.Runtime
			a.pathPrefixes = append(a.pathPrefixes, entry.Path)
			continue
		}
		a.bindHandlers[entry.Bind] = entry.Runtime
	}
	sort.Slice(a.pathPrefixes, func(i, j int) bool {
		if len(a.pathPrefixes[i]) == len(a.pathPrefixes[j]) {
			return a.pathPrefixes[i] > a.pathPrefixes[j]
		}
		return len(a.pathPrefixes[i]) > len(a.pathPrefixes[j])
	})
	return nil
}

func (a *App) stopHandlers() {
	for _, handler := range a.handlers {
		_ = handler.Stop(context.Background())
	}
	a.handlers = nil
}

func (a *App) matchProxyPrefix(target string) string {
	for _, prefix := range a.pathPrefixes {
		if target == prefix || strings.HasPrefix(target, prefix+"/") {
			return prefix
		}
	}
	return ""
}

func metricsAuthMiddleware(token string, next http.Handler) http.HandlerFunc {
	if token == "" {
		return next.ServeHTTP
	}
	expected := sha256.Sum256([]byte(token))
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		actual := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))
		if !hmac.Equal(expected[:], actual[:]) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}
}
