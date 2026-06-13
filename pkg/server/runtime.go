package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	_ "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	_ "gopkg.d7z.net/cache-proxy/pkg/proxy/gomod"
	_ "gopkg.d7z.net/cache-proxy/pkg/proxy/npm"
	_ "gopkg.d7z.net/cache-proxy/pkg/proxy/oci"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
	"gopkg.d7z.net/cache-proxy/web"
)

const (
	systemTenant       = "_system"
	globalConfigPath   = "config/global/current.yaml"
	instanceIndexPath  = "config/instances/index.yaml"
	revisionStatePath  = "config/revision.json"
	defaultConfigLimit = 1 << 20
)

var (
	DefaultBackend     = "/tmp/cache-proxy"
	DefaultBind        = "127.0.0.1:18080"
	DefaultMetricsPath = "/-/metrics"
	DefaultGCInterval  = "24h"
	DefaultExpireAfter = "720h"
)

type Runtime struct {
	bind         string
	backend      string
	password     string
	metricsToken string
	store        *blobfs.Store
	stats        *proxy.Stats
	metricsReg   *prometheus.Registry
	registry     *proxydriver.Registry

	global     *config.GlobalConfig
	instances  map[string]config.InstanceSpec
	generation uint64

	mu sync.RWMutex

	handlers     []closeableHandler
	pathHandlers map[string]http.Handler
	bindHandlers map[string]http.Handler

	mainListener net.Listener
	mainSrv      *http.Server
	bindServers  map[string]*http.Server

	loginLimiter      *loginRateLimiter
	defaultMetrics    string
	defaultGCInterval time.Duration
	started           bool
	closed            atomic.Bool
	gcStop            chan struct{}
	gcDone            chan struct{}
}

type serverState struct {
	handlers     []closeableHandler
	pathHandlers map[string]http.Handler
	bindHandlers map[string]http.Handler
}

type closeableHandler interface {
	http.Handler
	Close()
}

type preparedServer struct {
	server   *http.Server
	listener net.Listener
}

type runtimeHandler struct {
	http.Handler
	close func()
}

func (h runtimeHandler) Close() {
	if h.close != nil {
		h.close()
	}
}

type Options struct {
	Backend      string
	Bind         string
	Password     string
	MetricsToken string
	MetricsPath  string
	GCInterval   time.Duration
}

type revisionState struct {
	Generation uint64 `json:"generation"`
}

type instanceIndexDocument struct {
	Instances []config.InstanceSummary `json:"instances" yaml:"instances"`
}

func DefaultOptions() Options {
	return Options{
		Backend:      DefaultBackend,
		Bind:         DefaultBind,
		MetricsPath:  DefaultMetricsPath,
		GCInterval:   mustDefaultDuration("DefaultGCInterval", DefaultGCInterval),
		Password:     "",
		MetricsToken: "",
	}
}

func mustDefaultDuration(name, value string) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		panic(fmt.Sprintf("invalid %s %q: %v", name, value, err))
	}
	return duration
}

func OpenWithOptions(ctx context.Context, options Options) (*Runtime, error) {
	if options.Backend == "" {
		return nil, errors.New("backend is empty")
	}
	if options.Bind == "" {
		options.Bind = DefaultBind
	}
	if options.MetricsPath == "" {
		options.MetricsPath = DefaultMetricsPath
	}
	if options.GCInterval <= 0 {
		options.GCInterval = mustDefaultDuration("DefaultGCInterval", DefaultGCInterval)
	}
	if err := os.MkdirAll(options.Backend, 0o755); err != nil {
		return nil, err
	}
	store, err := blobfs.Open(options.Backend, blobfs.DefaultConfig())
	if err != nil {
		return nil, err
	}
	global, instances, generation, err := loadOrInitState(ctx, store, proxydriver.Default, options.MetricsPath, options.GCInterval)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	metricsReg := prometheus.NewRegistry()
	metricsReg.MustRegister(newBlobfsCollector(store))
	runtime := &Runtime{
		bind:              options.Bind,
		backend:           options.Backend,
		password:          options.Password,
		metricsToken:      options.MetricsToken,
		store:             store,
		stats:             proxy.NewStats(metricsReg),
		metricsReg:        metricsReg,
		registry:          proxydriver.Default,
		global:            global,
		instances:         instances,
		generation:        generation,
		bindServers:       map[string]*http.Server{},
		loginLimiter:      newLoginRateLimiter(),
		defaultMetrics:    options.MetricsPath,
		defaultGCInterval: options.GCInterval,
		gcStop:            make(chan struct{}),
		gcDone:            make(chan struct{}),
	}
	state, err := runtime.buildServerState(instances)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	runtime.applyServerState(state)
	runtime.mainSrv = &http.Server{Addr: runtime.bind, Handler: http.HandlerFunc(runtime.serveMain)}
	go runtime.gcLoop()
	return runtime, nil
}

func (r *Runtime) Start() error {
	r.mu.RLock()
	if r.started {
		r.mu.RUnlock()
		return nil
	}
	bindHandlers := make(map[string]http.Handler, len(r.bindHandlers))
	for addr, handler := range r.bindHandlers {
		bindHandlers[addr] = handler
	}
	r.mu.RUnlock()

	mainLn, err := net.Listen("tcp", r.bind)
	if err != nil {
		return fmt.Errorf("listen %s: %w", r.bind, err)
	}
	prepared := map[string]preparedServer{}
	for addr := range bindHandlers {
		listener, listenErr := net.Listen("tcp", addr)
		if listenErr != nil {
			_ = mainLn.Close()
			closePreparedServers(prepared)
			return fmt.Errorf("listen %s: %w", addr, listenErr)
		}
		prepared[addr] = preparedServer{
			server:   &http.Server{Addr: addr, Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { r.serveBind(addr, w, req) })},
			listener: listener,
		}
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		_ = mainLn.Close()
		closePreparedServers(prepared)
		return nil
	}
	r.mainListener = mainLn
	r.started = true
	for addr, item := range prepared {
		r.bindServers[addr] = item.server
	}
	r.mu.Unlock()

	go func() {
		if err := r.mainSrv.Serve(mainLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("main server error", "addr", r.bind, "err", err)
		}
	}()
	for _, item := range prepared {
		go func(server *http.Server, listener net.Listener) {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("bind server error", "addr", server.Addr, "err", err)
			}
		}(item.server, item.listener)
	}
	return nil
}

func (r *Runtime) Close(ctx context.Context) error {
	if r.closed.Swap(true) {
		return nil
	}
	close(r.gcStop)
	<-r.gcDone
	r.loginLimiter.close()

	var joined error
	r.mu.RLock()
	mainSrv := r.mainSrv
	bindServers := make([]*http.Server, 0, len(r.bindServers))
	for _, srv := range r.bindServers {
		bindServers = append(bindServers, srv)
	}
	handlers := append([]closeableHandler(nil), r.handlers...)
	r.mu.RUnlock()

	if mainSrv != nil {
		joined = errors.Join(joined, mainSrv.Shutdown(ctx))
	}
	for _, srv := range bindServers {
		joined = errors.Join(joined, srv.Shutdown(ctx))
	}
	for _, handler := range handlers {
		handler.Close()
	}
	return errors.Join(joined, r.store.Close())
}

func (r *Runtime) serveMain(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/-/api/login":
		r.loginHandler(w, req)
		return
	case req.Method == http.MethodPost && req.URL.Path == "/-/api/logout":
		r.logoutHandler(w, req)
		return
	case req.Method == http.MethodGet && req.URL.Path == "/-/api/public/instances":
		r.publicInstancesAPI(w, req)
		return
	case req.Method == http.MethodGet && req.URL.Path == r.metricsPath():
		metricsAuthMiddleware(r.metricsToken, promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, r.metricsReg}, promhttp.HandlerOpts{})).ServeHTTP(w, req)
		return
	case strings.HasPrefix(req.URL.Path, "/-/api/"):
		adminAuthMiddleware(r.password, http.HandlerFunc(r.serveAPI)).ServeHTTP(w, req)
		return
	}
	if prefix := r.matchProxyPrefix(req.URL.Path); prefix != "" {
		r.servePathProxyAt(w, req, prefix)
		return
	}
	r.serveSPA(w, req)
}

func (r *Runtime) serveSPA(w http.ResponseWriter, req *http.Request) {
	assets := web.Assets()
	filePath := strings.TrimPrefix(req.URL.Path, "/")
	if filePath == "" {
		filePath = "index.html"
	}
	if stat, err := fs.Stat(assets, filePath); err == nil && !stat.IsDir() {
		http.FileServerFS(assets).ServeHTTP(w, req)
		return
	}
	req.URL.Path = "/index.html"
	http.FileServerFS(assets).ServeHTTP(w, req)
}

func (r *Runtime) metricsPath() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.global != nil && strings.TrimSpace(r.global.Metrics.Path) != "" {
		return r.global.Metrics.Path
	}
	return r.defaultMetrics
}

func (r *Runtime) gcInterval() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.global != nil && r.global.Storage.GC.Blob > 0 {
		return r.global.Storage.GC.Blob.Duration()
	}
	return r.defaultGCInterval
}

func (r *Runtime) gcLoop() {
	defer close(r.gcDone)
	for {
		interval := r.gcInterval()
		if interval <= 0 {
			interval = r.defaultGCInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-r.gcStop:
			timer.Stop()
			return
		case <-timer.C:
			if _, err := r.store.RunGC(context.Background(), blobfs.GCOptions{Compact: true}); err != nil {
				slog.Warn("blob gc failed", "err", err)
			}
		}
	}
}

func (r *Runtime) buildServerState(instances map[string]config.InstanceSpec) (*serverState, error) {
	handlers := []closeableHandler{}
	pathHandlers := map[string]http.Handler{}
	bindHandlers := map[string]http.Handler{}
	for _, name := range sortedInstanceNames(instances) {
		spec := instances[name]
		if !spec.Meta.Enabled {
			continue
		}
		handler, err := r.newInstanceHandler(name, spec)
		if err != nil {
			for _, current := range handlers {
				current.Close()
			}
			return nil, err
		}
		handlers = append(handlers, handler)
		if spec.Route.Path != "" {
			pathHandlers["/"+strings.Trim(spec.Route.Path, "/")] = handler
		} else {
			bindHandlers[spec.Route.Bind] = handler
		}
	}
	return &serverState{handlers: handlers, pathHandlers: pathHandlers, bindHandlers: bindHandlers}, nil
}

func (r *Runtime) applyServerState(state *serverState) {
	r.handlers = state.handlers
	r.pathHandlers = state.pathHandlers
	r.bindHandlers = state.bindHandlers
}

func (r *Runtime) matchProxyPrefix(target string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	prefixes := make([]string, 0, len(r.pathHandlers))
	for prefix := range r.pathHandlers {
		prefixes = append(prefixes, prefix)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(prefixes)))
	for _, prefix := range prefixes {
		if target == prefix || strings.HasPrefix(target, prefix+"/") {
			return prefix
		}
	}
	return ""
}

func (r *Runtime) servePathProxyAt(w http.ResponseWriter, req *http.Request, prefix string) {
	r.mu.RLock()
	handler := r.pathHandlers[prefix]
	r.mu.RUnlock()
	if handler == nil {
		http.NotFound(w, req)
		return
	}
	next := req.Clone(req.Context())
	next.Header = req.Header.Clone()
	next.Header.Set("X-Cache-Proxy-Prefix", prefix)
	http.StripPrefix(prefix, handler).ServeHTTP(w, next)
}

func (r *Runtime) serveBind(addr string, w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	handler := r.bindHandlers[addr]
	r.mu.RUnlock()
	if handler == nil {
		http.NotFound(w, req)
		return
	}
	handler.ServeHTTP(w, req)
}

func (r *Runtime) newInstanceHandler(name string, spec config.InstanceSpec) (closeableHandler, error) {
	resolved, err := r.registry.Resolve(spec)
	if err != nil {
		return nil, err
	}
	handler, closeFn, err := resolved.Driver.NewHandler(name, resolved, r.store, r.stats)
	if err != nil {
		return nil, err
	}
	return runtimeHandler{Handler: handler, close: closeFn}, nil
}

func (r *Runtime) prepareBindServers(state *serverState) (map[string]preparedServer, []*http.Server, error) {
	r.mu.RLock()
	started := r.started
	current := make(map[string]*http.Server, len(r.bindServers))
	for addr, srv := range r.bindServers {
		current[addr] = srv
	}
	r.mu.RUnlock()
	if !started {
		return nil, nil, nil
	}
	added := map[string]preparedServer{}
	for addr := range state.bindHandlers {
		if current[addr] != nil {
			continue
		}
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			closePreparedServers(added)
			return nil, nil, fmt.Errorf("listen %s: %w", addr, err)
		}
		added[addr] = preparedServer{
			server:   &http.Server{Addr: addr, Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { r.serveBind(addr, w, req) })},
			listener: listener,
		}
	}
	removed := []*http.Server{}
	for addr, srv := range current {
		if state.bindHandlers[addr] == nil {
			removed = append(removed, srv)
		}
	}
	return added, removed, nil
}

func (p preparedServer) start() {
	go func() {
		if err := p.server.Serve(p.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("bind server error", "addr", p.server.Addr, "err", err)
		}
	}()
}

func closePreparedServers(servers map[string]preparedServer) {
	for _, server := range servers {
		_ = server.listener.Close()
	}
}

func shutdownServerListWithTimeout(servers []*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(ctx)
	}
}

func (r *Runtime) replaceState(ctx context.Context, generation uint64, global *config.GlobalConfig, instances map[string]config.InstanceSpec, changed []config.InstanceSpec, removed []string) (uint64, error) {
	r.mu.RLock()
	currentGeneration := r.generation
	r.mu.RUnlock()
	if generation != currentGeneration {
		return 0, conflictError("config generation changed")
	}
	if err := validateGlobalConfig(global, r.defaultMetrics, r.defaultGCInterval); err != nil {
		return 0, err
	}
	if err := validateInstances(instances, r.registry, r.bind, global.Metrics.Path); err != nil {
		return 0, err
	}
	state, err := r.buildServerState(instances)
	if err != nil {
		return 0, err
	}
	addedServers, removedServers, err := r.prepareBindServers(state)
	if err != nil {
		for _, handler := range state.handlers {
			handler.Close()
		}
		return 0, err
	}

	if err := writeYAMLObject(ctx, r.store, systemTenant, globalConfigPath, global); err != nil {
		closePreparedServers(addedServers)
		for _, handler := range state.handlers {
			handler.Close()
		}
		return 0, err
	}
	for _, spec := range changed {
		if err := writeInstanceSpec(ctx, r.store, r.registry, spec); err != nil {
			closePreparedServers(addedServers)
			for _, handler := range state.handlers {
				handler.Close()
			}
			return 0, err
		}
	}
	for _, name := range removed {
		if err := deleteInstanceConfig(ctx, r.store, name); err != nil {
			closePreparedServers(addedServers)
			for _, handler := range state.handlers {
				handler.Close()
			}
			return 0, err
		}
	}
	if err := writeYAMLObject(ctx, r.store, systemTenant, instanceIndexPath, buildIndexDocument(instances)); err != nil {
		closePreparedServers(addedServers)
		for _, handler := range state.handlers {
			handler.Close()
		}
		return 0, err
	}
	nextGeneration := currentGeneration + 1
	if err := writeJSONObject(ctx, r.store, systemTenant, revisionStatePath, revisionState{Generation: nextGeneration}); err != nil {
		closePreparedServers(addedServers)
		for _, handler := range state.handlers {
			handler.Close()
		}
		return 0, err
	}

	r.mu.Lock()
	oldHandlers := append([]closeableHandler(nil), r.handlers...)
	r.global = config.CloneGlobal(global)
	r.instances = config.CloneInstances(instances)
	r.generation = nextGeneration
	r.applyServerState(state)
	if r.started {
		for addr, item := range addedServers {
			item.start()
			r.bindServers[addr] = item.server
		}
		for _, srv := range removedServers {
			delete(r.bindServers, srv.Addr)
		}
	}
	r.mu.Unlock()

	shutdownServerListWithTimeout(removedServers)
	for _, handler := range oldHandlers {
		handler.Close()
	}
	for _, name := range removed {
		if err := r.store.DeleteTenant(ctx, name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("delete instance tenant failed", "instance", name, "err", err)
		}
	}
	return nextGeneration, nil
}
