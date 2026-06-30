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
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const (
	DefaultBackend     = "/tmp/cache-proxy"
	DefaultBind        = "127.0.0.1:18080"
	DefaultMetricsPath = "/metrics"
	drainTimeout       = 10 * time.Second
)

type App struct {
	config     *config.Document
	configPath string
	store      *blobfs.Store
	stats      *httpcache.Stats
	downloads  *httpcache.DownloadLimiter
	metricsReg *prometheus.Registry

	scheduler *scheduler.Scheduler
	bus       *bus.Bus

	entries       map[string]*proxyruntime.Entry
	handlers      []proxyruntime.Instance
	routesMu      sync.RWMutex
	pathHandlers  map[string]http.Handler
	pathPrefixes  []string
	bindHandlers  map[string]http.Handler
	bindServers   map[string]*http.Server
	bindListeners map[string]net.Listener
	mainServer    *http.Server
	mainListener  net.Listener
	lifecycleMu   sync.Mutex
	lifecycleCtx  context.Context
	stopRuntime   context.CancelFunc
	started       bool
	ready         atomic.Bool
	closed        atomic.Bool

	tenantUsageMu         sync.Mutex
	tenantUsageCachedAt   time.Time
	tenantUsageCache      map[string]int64
	tenantUsageRefreshing atomic.Bool
}

func (a *App) tenantUsage(ctx context.Context, tenants []string) map[string]int64 {
	a.tenantUsageMu.Lock()
	prev := a.tenantUsageCachedAt
	result := cloneUsage(a.tenantUsageCache)
	a.tenantUsageMu.Unlock()
	if time.Since(prev) >= 5*time.Minute {
		a.refreshTenantUsage(tenants)
	}
	return result
}

func (a *App) refreshTenantUsage(tenants []string) {
	if a.store == nil || !a.tenantUsageRefreshing.CompareAndSwap(false, true) {
		return
	}
	names := append([]string(nil), tenants...)
	go func() {
		defer a.tenantUsageRefreshing.Store(false)
		ctx, cancel := context.WithTimeout(a.lifecycleCtx, 30*time.Second)
		defer cancel()
		usage := collectTenantUsage(ctx, names, a.store)
		a.tenantUsageMu.Lock()
		a.tenantUsageCache = usage
		a.tenantUsageCachedAt = time.Now()
		a.tenantUsageMu.Unlock()
	}()
}

func cloneUsage(src map[string]int64) map[string]int64 {
	if len(src) == 0 {
		return map[string]int64{}
	}
	dst := make(map[string]int64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func Load(path string) (*config.Document, error) {
	return config.LoadFile(path)
}

func Validate(doc *config.Document) error {
	dir, err := os.MkdirTemp("", "cache-proxy-validate-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	copy := *doc
	copy.Server.Backend = dir
	normalizeDocument(&copy)
	store, err := blobfs.Open(dir, appBlobFSConfig())
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	registry := prometheus.NewRegistry()
	stats := httpcache.NewStats(registry)
	downloads := httpcache.NewDownloadLimiter(copy.Storage.Download.MaxActive, copy.Storage.Download.MaxActivePerInstance)

	b := bus.NewWithRegisterer(registry)
	sched := scheduler.New(b, store, registry)
	validateCtx, validateCancel := context.WithCancel(context.Background())
	sched.Start(validateCtx)
	defer validateCancel()
	defer func() { sched.Stop(validateCtx) }()
	_, err = planEntries(context.Background(), &copy, store, stats, downloads, sched, b)
	return err
}

func Open(ctx context.Context, doc *config.Document, configPath string) (*App, error) {
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

	store, err := blobfs.Open(doc.Server.Backend, appBlobFSConfig())
	if err != nil {
		return nil, err
	}
	metricsReg := prometheus.NewRegistry()
	metricsReg.MustRegister(metrics.NewBlobFSCollector(store))
	stats := httpcache.NewStats(metricsReg)
	downloads := httpcache.NewDownloadLimiter(doc.Storage.Download.MaxActive, doc.Storage.Download.MaxActivePerInstance)

	b := bus.NewWithRegisterer(metricsReg)
	sched := scheduler.New(b, store, metricsReg)

	lifecycleCtx, stopRuntime := context.WithCancel(context.Background())
	sched.Start(lifecycleCtx)
	cleanupOpenFailure := func() {
		stopRuntime()
		stopCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		_ = sched.Stop(stopCtx)
		_ = store.Close()
	}

	entries, err := planEntries(ctx, doc, store, stats, downloads, sched, b)
	if err != nil {
		cleanupOpenFailure()
		return nil, err
	}

	app := &App{
		config:        doc,
		configPath:    configPath,
		store:         store,
		stats:         stats,
		downloads:     downloads,
		metricsReg:    metricsReg,
		scheduler:     sched,
		bus:           b,
		entries:       entries,
		pathHandlers:  map[string]http.Handler{},
		bindHandlers:  map[string]http.Handler{},
		bindServers:   map[string]*http.Server{},
		bindListeners: map[string]net.Listener{},
		lifecycleCtx:  lifecycleCtx,
		stopRuntime:   stopRuntime,
	}
	if err := app.prepareHandlers(lifecycleCtx); err != nil {
		cleanupOpenFailure()
		return nil, err
	}

	sched.Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey("_system", scheduler.TypeBlobGC, ""),
		Interval: doc.Storage.GC.Blob.Duration(),
		Handler: func(ctx context.Context) error {
			_, err := app.store.RunGC(ctx, blobfs.GCOptions{Compact: true})
			return err
		},
	})

	app.mainServer = &http.Server{Addr: doc.Server.Bind, Handler: app}
	app.checkOrphans(lifecycleCtx)
	return app, nil
}

func (a *App) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !a.ready.Load() {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "proxy not ready", http.StatusServiceUnavailable)
		return
	}
	a.routesMu.RLock()
	defer a.routesMu.RUnlock()

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
	handler := a.pathHandlers[prefix]
	if handler == nil {
		http.NotFound(w, req)
		return
	}
	next := req.Clone(req.Context())
	next.Header = req.Header.Clone()
	next.Header.Set("X-Cache-Proxy-Prefix", prefix)
	http.StripPrefix(prefix, handler).ServeHTTP(w, next)
}

func (a *App) Start() error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed.Load() {
		return errors.New("app is closed")
	}
	if a.started {
		return nil
	}
	mainListener, err := net.Listen("tcp", a.config.Server.Bind)
	if err != nil {
		return fmt.Errorf("listen %s: %w", a.config.Server.Bind, err)
	}
	a.mainListener = mainListener
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
		a.bindListeners[addr] = listener
	}

	a.started = true
	a.ready.Store(true)
	go func() {
		if err := a.mainServer.Serve(mainListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("main server error", "addr", a.config.Server.Bind, "err", err)
		}
	}()
	for addr, listener := range prepared {
		server := &http.Server{Addr: addr, Handler: bindDispatchHandler{app: a, addr: addr}}
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
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed.Swap(true) {
		return nil
	}
	a.ready.Store(false)
	a.stopRuntime()

	var joined error
	if a.scheduler != nil {
		joined = errors.Join(joined, a.scheduler.Stop(ctx))
	}
	if a.mainServer != nil {
		joined = errors.Join(joined, a.mainServer.Shutdown(ctx))
	}
	for _, server := range a.bindServers {
		joined = errors.Join(joined, server.Shutdown(ctx))
	}
	a.routesMu.RLock()
	handlers := make([]proxyruntime.Instance, len(a.handlers))
	copy(handlers, a.handlers)
	a.routesMu.RUnlock()
	for _, handler := range handlers {
		joined = errors.Join(joined, handler.Stop(ctx))
	}
	if a.store != nil {
		joined = errors.Join(joined, a.store.Close())
	}
	return joined
}

type bindHomeHandler struct {
	app   *App
	entry *proxyruntime.Entry
	next  http.Handler
}

func (h bindHomeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if (req.Method == http.MethodGet || req.Method == http.MethodHead) && req.URL.Path == "/" {
		h.app.serveBindHome(w, req, h.entry)
		return
	}
	h.next.ServeHTTP(w, req)
}

type bindDispatchHandler struct {
	app  *App
	addr string
}

func (h bindDispatchHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !h.app.ready.Load() {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "proxy not ready", http.StatusServiceUnavailable)
		return
	}
	h.app.routesMu.RLock()
	next := h.app.bindHandlers[h.addr]
	h.app.routesMu.RUnlock()
	if next == nil {
		http.NotFound(w, req)
		return
	}
	next.ServeHTTP(w, req)
}

func (a *App) prepareHandlers(ctx context.Context) error {
	for _, name := range proxyruntime.SortedNames(a.entries) {
		entry := a.entries[name]
		if !entry.Enabled || entry.Runtime == nil {
			continue
		}
		entryCtx, entryCancel := context.WithCancel(ctx)
		entry.Ctx = entryCtx
		entry.Cancel = entryCancel
		if err := entry.Runtime.Start(entryCtx); err != nil {
			entryCancel()
			a.stopHandlers()
			return fmt.Errorf("instance %s: %w", entry.Name, err)
		}
		a.handlers = append(a.handlers, entry.Runtime)
		if entry.Path != "" {
			a.pathHandlers[entry.Path] = entry.Runtime
			a.pathPrefixes = append(a.pathPrefixes, entry.Path)
			continue
		}
		a.bindHandlers[entry.Bind] = bindHomeHandler{
			app:   a,
			entry: entry,
			next:  entry.Runtime,
		}
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
	for _, entry := range a.entries {
		if entry.Cancel != nil {
			entry.Cancel()
		}
	}
	for _, handler := range a.handlers {
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		_ = handler.Stop(ctx)
		cancel()
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
