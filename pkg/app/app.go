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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	DefaultBackend             = "/tmp/cache-proxy"
	DefaultBind                = "127.0.0.1:18080"
	DefaultMetricsPath         = "/metrics"
	drainTimeout               = 10 * time.Second
	downstreamWriteIdleTimeout = 2 * time.Minute
)

type App struct {
	config          *config.Document
	stores          map[string]*blobfs.Store
	backendLock     *backendLock
	stats           *metrics.Stats
	upstreamGate    *proxyruntime.UpstreamGate
	metricsRegistry *prometheus.Registry

	scheduler *scheduler.Scheduler
	status    *appStatus

	entries      map[string]*proxyruntime.Entry
	handlers     []proxyruntime.Instance
	routesMu     sync.RWMutex
	pathHandlers map[string]http.Handler
	pathPrefixes []string
	bindHandlers map[string]http.Handler
	bindServers  map[string]*http.Server
	mainServer   *http.Server
	lifecycleMu  sync.Mutex
	lifecycleCtx context.Context
	stopRuntime  context.CancelFunc
	started      bool
	ready        atomic.Bool
	closed       atomic.Bool

	tenantUsageMu         sync.Mutex
	tenantUsageCachedAt   time.Time
	tenantUsageCache      map[string]int64
	tenantUsageRefreshing atomic.Bool
	tenantUsageClosing    bool
	tenantUsageWG         sync.WaitGroup
}

func (a *App) tenantUsage(ctx context.Context, tenants []string) map[string]int64 {
	a.tenantUsageMu.Lock()
	prev := a.tenantUsageCachedAt
	result := make(map[string]int64, len(a.tenantUsageCache))
	for tenant, bytes := range a.tenantUsageCache {
		result[tenant] = bytes
	}
	a.tenantUsageMu.Unlock()
	if time.Since(prev) >= 5*time.Minute {
		a.refreshTenantUsage(ctx, tenants)
	}
	return result
}

func (a *App) refreshTenantUsage(parent context.Context, tenants []string) {
	if len(a.stores) == 0 || !a.tenantUsageRefreshing.CompareAndSwap(false, true) {
		return
	}
	a.tenantUsageMu.Lock()
	if a.tenantUsageClosing {
		a.tenantUsageMu.Unlock()
		a.tenantUsageRefreshing.Store(false)
		return
	}
	a.tenantUsageWG.Add(1)
	a.tenantUsageMu.Unlock()
	names := append([]string(nil), tenants...)
	go func() {
		defer a.tenantUsageWG.Done()
		defer a.tenantUsageRefreshing.Store(false)
		baseCtx := a.lifecycleCtx
		if baseCtx == nil {
			baseCtx = parent
		}
		if baseCtx == nil {
			baseCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
		defer cancel()
		usage := collectInstanceUsage(ctx, names, a.stores)
		a.tenantUsageMu.Lock()
		a.tenantUsageCache = usage
		a.tenantUsageCachedAt = time.Now()
		a.tenantUsageMu.Unlock()
	}()
}

func Validate(doc *config.Document) error {
	if doc == nil {
		return errors.New("config document is nil")
	}
	dir, err := os.MkdirTemp("", "cache-proxy-validate-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	docCopy := *doc
	docCopy.Server.Backend = dir
	normalizeDocument(&docCopy)
	if err := validateServerConfig(&docCopy); err != nil {
		return err
	}
	registry := prometheus.NewRegistry()
	stats := metrics.NewStats(registry)
	upstreamGate := proxyruntime.NewUpstreamGate(upstreamGateConfig(docCopy.Storage.Download))

	taskScheduler, err := scheduler.NewPersistent(filepath.Join(docCopy.Server.Backend, "scheduler.json"))
	if err != nil {
		return err
	}
	result, err := planEntries(context.Background(), &docCopy, stats, upstreamGate, taskScheduler)
	if result != nil {
		defer func() { _ = closeStores(result.Stores) }()
	}
	return err
}

func Open(ctx context.Context, doc *config.Document) (*App, error) {
	if doc == nil {
		return nil, errors.New("config document is nil")
	}
	normalizeDocument(doc)
	if err := validateServerConfig(doc); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(doc.Server.Backend, 0o755); err != nil {
		return nil, err
	}

	backendLock, err := lockBackend(doc.Server.Backend)
	if err != nil {
		return nil, err
	}
	storeio.CleanStaleWorkFiles(filepath.Join(doc.Server.Backend, "instances"), 24*time.Hour)
	metricsRegistry := prometheus.NewRegistry()
	stats := metrics.NewStats(metricsRegistry)
	upstreamGate := proxyruntime.NewUpstreamGate(upstreamGateConfig(doc.Storage.Download))

	taskScheduler, err := scheduler.NewPersistent(filepath.Join(doc.Server.Backend, "scheduler.json"))
	if err != nil {
		_ = backendLock.Close()
		return nil, err
	}
	lifecycleCtx, stopRuntime := context.WithCancel(context.Background())
	status := newAppStatus(doc.Server.Status)
	taskScheduler.SetRunObserver(status.observeTaskRun)

	cleanupOpenFailure := func() {
		stopRuntime()
		stopCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		_ = taskScheduler.Stop(stopCtx)
		_ = backendLock.Close()
	}

	result, err := planEntries(ctx, doc, stats, upstreamGate, taskScheduler)
	if err != nil {
		cleanupOpenFailure()
		return nil, err
	}

	entries := make(map[string]*proxyruntime.Entry, len(result.Entries))
	for _, entry := range result.Entries {
		entries[entry.Name] = entry
	}
	for name, store := range result.Stores {
		prometheus.WrapRegistererWith(prometheus.Labels{"instance": name}, metricsRegistry).
			MustRegister(metrics.NewBlobFSCollector(store))
	}

	app := &App{
		config:          doc,
		stores:          result.Stores,
		backendLock:     backendLock,
		stats:           stats,
		upstreamGate:    upstreamGate,
		metricsRegistry: metricsRegistry,
		scheduler:       taskScheduler,
		status:          status,
		entries:         entries,
		pathHandlers:    map[string]http.Handler{},
		bindHandlers:    map[string]http.Handler{},
		bindServers:     map[string]*http.Server{},
		lifecycleCtx:    lifecycleCtx,
		stopRuntime:     stopRuntime,
	}
	if err := app.prepareHandlers(lifecycleCtx); err != nil {
		_ = closeStores(result.Stores)
		cleanupOpenFailure()
		return nil, err
	}
	taskScheduler.Start(lifecycleCtx)
	status.start(lifecycleCtx, app)

	for name, store := range app.stores {
		instanceName, instanceStore := name, store
		taskScheduler.Register(scheduler.TaskDef{
			Key:      scheduler.NewTaskKey(instanceName, scheduler.TypeBlobGC, ""),
			Interval: doc.Storage.GC.Blob.Duration(),
			Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
				_, err := instanceStore.RunGC(ctx, blobfs.GCOptions{Compact: true})
				return nil, err
			},
		})
	}

	app.mainServer = newHTTPServer(doc.Server.Bind, app)
	app.checkOrphans(lifecycleCtx)
	return app, nil
}

func (a *App) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !a.ready.Load() {
		w.Header().Set("Retry-After", "5")
		proxyruntime.WriteError(w, http.StatusServiceUnavailable)
		return
	}
	if req.Method == http.MethodGet && req.URL.Path == "/" {
		a.serveHome(w, req)
		return
	}
	if strings.HasPrefix(req.URL.Path, statusAPIPath) {
		a.serveStatus(w, req)
		return
	}
	if req.URL.Path == a.config.Metrics.Path {
		metricsAuthMiddleware(a.config.Metrics.Token, promhttp.HandlerFor(
			prometheus.Gatherers{prometheus.DefaultGatherer, a.metricsRegistry},
			promhttp.HandlerOpts{},
		)).ServeHTTP(w, req)
		return
	}
	a.routesMu.RLock()
	prefix := a.matchProxyPrefix(req.URL.Path)
	handler := a.pathHandlers[prefix]
	a.routesMu.RUnlock()
	if handler == nil {
		http.NotFound(w, req)
		return
	}
	if req.URL.Path == prefix && (req.Method == http.MethodGet || req.Method == http.MethodHead) {
		target := prefix + "/"
		if req.URL.RawQuery != "" {
			target += "?" + req.URL.RawQuery
		}
		http.Redirect(w, req, target, http.StatusPermanentRedirect)
		return
	}
	next := req.Clone(req.Context())
	next.Header = req.Header.Clone()
	next = proxyruntime.WithExternalBaseURL(next, strings.TrimRight(a.publicBaseURL(req), "/")+prefix)
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
	bindListeners := make(map[string]net.Listener, len(a.bindHandlers))
	for addr := range a.bindHandlers {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			_ = mainListener.Close()
			for _, item := range bindListeners {
				_ = item.Close()
			}
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		bindListeners[addr] = listener
	}

	a.started = true
	a.ready.Store(true)
	go func() {
		if err := a.mainServer.Serve(mainListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("main server error", "addr", a.config.Server.Bind, "err", err)
		}
	}()
	for addr, listener := range bindListeners {
		server := newHTTPServer(addr, bindDispatchHandler{app: a, addr: addr})
		a.bindServers[addr] = server
		go func(server *http.Server, listener net.Listener) {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("bind server error", "addr", server.Addr, "err", err)
			}
		}(server, listener)
	}
	return nil
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           downstreamWriteIdle(handler, downstreamWriteIdleTimeout),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
}

type idleWriteResponseWriter struct {
	http.ResponseWriter
	timeout time.Duration
}

func (w *idleWriteResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *idleWriteResponseWriter) WriteHeader(status int) {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
	w.ResponseWriter.WriteHeader(status)
}

func (w *idleWriteResponseWriter) Write(data []byte) (int, error) {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
	return w.ResponseWriter.Write(data)
}

func downstreamWriteIdle(next http.Handler, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		controller := http.NewResponseController(w)
		defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
		next.ServeHTTP(&idleWriteResponseWriter{ResponseWriter: w, timeout: timeout}, request)
	})
}

func (a *App) Close(ctx context.Context) error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed.Load() {
		return nil
	}
	a.ready.Store(false)
	var joined error
	drained := true
	if a.scheduler != nil {
		if err := a.scheduler.Stop(ctx); err != nil {
			joined = errors.Join(joined, err)
			drained = false
		}
	}
	if a.mainServer != nil {
		if err := a.mainServer.Shutdown(ctx); err != nil {
			joined = errors.Join(joined, err)
			drained = false
		}
	}
	for _, server := range a.bindServers {
		if err := server.Shutdown(ctx); err != nil {
			joined = errors.Join(joined, err)
			drained = false
		}
	}
	if a.stopRuntime != nil {
		a.stopRuntime()
	}
	a.tenantUsageMu.Lock()
	a.tenantUsageClosing = true
	a.tenantUsageMu.Unlock()
	if err := waitForGroup(ctx, &a.tenantUsageWG); err != nil {
		joined = errors.Join(joined, err)
		drained = false
	}
	a.routesMu.RLock()
	handlers := make([]proxyruntime.Instance, len(a.handlers))
	copy(handlers, a.handlers)
	a.routesMu.RUnlock()
	for _, handler := range handlers {
		if err := handler.Stop(ctx); err != nil {
			joined = errors.Join(joined, err)
			drained = false
		}
	}
	if a.status != nil {
		if err := a.status.stop(ctx); err != nil {
			joined = errors.Join(joined, err)
			drained = false
		}
	}
	if drained {
		joined = errors.Join(joined, closeStores(a.stores))
		joined = errors.Join(joined, a.backendLock.Close())
		if joined == nil {
			a.closed.Store(true)
		}
	}
	return joined
}

func waitForGroup(ctx context.Context, group *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func closeStores(stores map[string]*blobfs.Store) error {
	var joined error
	for name, store := range stores {
		joined = errors.Join(joined, store.Close())
		delete(stores, name)
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
		proxyruntime.WriteError(w, http.StatusServiceUnavailable)
		return
	}
	h.app.routesMu.RLock()
	next := h.app.bindHandlers[h.addr]
	h.app.routesMu.RUnlock()
	if next == nil {
		http.NotFound(w, req)
		return
	}
	next.ServeHTTP(w, proxyruntime.WithExternalBaseURL(req, h.app.publicBaseURL(req)))
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
