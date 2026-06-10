package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	containername "github.com/google/go-containerregistry/pkg/name"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	fileproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	npmproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/npm"
	ociproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/oci"
	"gopkg.d7z.net/cache-proxy/web"
)

const (
	systemTenant = "_system"
	configPath   = "config.yaml"
)

var (
	DefaultBackend        = "/tmp/cache-proxy"
	DefaultBind           = "127.0.0.1:18080"
	DefaultMetricsPath    = "/-/metrics"
	DefaultGCInterval     = "24h"
	DefaultExpireAfter    = "720h"
	DefaultConfigMaxBytes = "1048576"
)

type Runtime struct {
	bind         string
	backend      string
	password     string
	metricsToken string
	metricsPath  string
	gcInterval   time.Duration
	store        *blobfs.Store
	stats        *proxy.Stats
	metricsReg   *prometheus.Registry
	config       *config.Config
	generation   uint64
	mu           sync.RWMutex

	apiMux       *http.ServeMux
	mainHandler  http.Handler
	handlers     []*proxy.Handler
	pathHandlers map[string]*proxy.Handler
	bindHandlers map[string]*proxy.Handler

	mainListener net.Listener
	mainSrv      *http.Server
	bindServers  []*http.Server

	loginLimiter *loginRateLimiter
	started      bool
	closed       atomic.Bool
	gcStop       chan struct{}
	gcDone       chan struct{}
}

type serverState struct {
	handlers     []*proxy.Handler
	pathHandlers map[string]*proxy.Handler
	bindHandlers map[string]*proxy.Handler
}

type preparedServer struct {
	server   *http.Server
	listener net.Listener
}

type ConfigSnapshot struct {
	Generation uint64         `json:"generation"`
	Config     *config.Config `json:"config"`
	YAML       string         `json:"yaml"`
}

type Options struct {
	Backend      string
	Bind         string
	Password     string
	MetricsToken string
	MetricsPath  string
	GCInterval   time.Duration
}

func DefaultOptions() Options {
	return Options{
		Backend:      DefaultBackend,
		Bind:         DefaultBind,
		Password:     "",
		MetricsToken: "",
		MetricsPath:  DefaultMetricsPath,
		GCInterval:   mustDefaultDuration("DefaultGCInterval", DefaultGCInterval),
	}
}

func mustDefaultDuration(name, value string) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		panic(fmt.Sprintf("invalid %s %q: %v", name, value, err))
	}
	return duration
}

func defaultConfigMaxBytes() int {
	value, err := strconv.Atoi(DefaultConfigMaxBytes)
	if err != nil || value <= 0 {
		panic(fmt.Sprintf("invalid DefaultConfigMaxBytes %q", DefaultConfigMaxBytes))
	}
	return value
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
	cfg, generation, err := loadOrInitConfig(ctx, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	metricsReg := prometheus.NewRegistry()
	metricsReg.MustRegister(newBlobfsCollector(store))
	runtime := &Runtime{
		bind:         options.Bind,
		backend:      options.Backend,
		password:     options.Password,
		metricsToken: options.MetricsToken,
		metricsPath:  options.MetricsPath,
		gcInterval:   options.GCInterval,
		store:        store,
		stats:        proxy.NewStats(),
		metricsReg:   metricsReg,
		config:       cfg,
		generation:   generation,
		loginLimiter: newLoginRateLimiter(),
		gcStop:       make(chan struct{}),
		gcDone:       make(chan struct{}),
	}
	if err := runtime.buildMainHandler(cfg); err != nil {
		_ = store.Close()
		return nil, err
	}
	runtime.mainSrv = &http.Server{Addr: runtime.bind, Handler: runtime.mainHandler}
	go runtime.gcLoop(options.GCInterval)
	return runtime, nil
}

func (r *Runtime) Start() error {
	r.mu.RLock()
	if r.started {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	mainLn, err := net.Listen("tcp", r.bind)
	if err != nil {
		return fmt.Errorf("listen %s: %w", r.bind, err)
	}

	bindServers := []*http.Server{}
	bindListeners := []net.Listener{}
	r.mu.RLock()
	for addr := range r.bindHandlers {
		bindServers = append(bindServers, &http.Server{Addr: addr, Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			r.serveBind(addr, w, req)
		})})
	}
	r.mu.RUnlock()
	for _, srv := range bindServers {
		ln, listenErr := net.Listen("tcp", srv.Addr)
		if listenErr != nil {
			_ = mainLn.Close()
			for _, opened := range bindListeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen %s: %w", srv.Addr, listenErr)
		}
		bindListeners = append(bindListeners, ln)
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		_ = mainLn.Close()
		for _, ln := range bindListeners {
			_ = ln.Close()
		}
		return nil
	}
	r.mainListener = mainLn
	r.bindServers = bindServers
	r.started = true
	r.mu.Unlock()

	go func() {
		if err := r.mainSrv.Serve(mainLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("main server error", "addr", r.bind, "err", err)
		}
	}()
	for i, srv := range bindServers {
		go func(server *http.Server, ln net.Listener) {
			if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("bind server error", "addr", server.Addr, "err", err)
			}
		}(srv, bindListeners[i])
	}
	return nil
}

func (r *Runtime) Close(ctx context.Context) error {
	if r.closed.Swap(true) {
		return nil
	}
	close(r.gcStop)
	<-r.gcDone

	var joined error
	r.mu.RLock()
	mainSrv := r.mainSrv
	bindServers := append([]*http.Server(nil), r.bindServers...)
	handlers := append([]*proxy.Handler(nil), r.handlers...)
	r.mu.RUnlock()

	joined = errors.Join(joined, mainSrv.Shutdown(ctx))
	for _, srv := range bindServers {
		joined = errors.Join(joined, srv.Shutdown(ctx))
	}
	for _, handler := range handlers {
		handler.Close()
	}
	return errors.Join(joined, r.store.Close())
}

func (r *Runtime) Snapshot(ctx context.Context) (*ConfigSnapshot, error) {
	data, generation, err := readConfigObject(ctx, r.store)
	if err != nil {
		return nil, err
	}
	cfg, err := parseConfig(data)
	if err != nil {
		return nil, err
	}
	return &ConfigSnapshot{Generation: generation, Config: cfg, YAML: string(data)}, nil
}

func (r *Runtime) UpdateConfig(ctx context.Context, generation uint64, cfg *config.Config) (*ConfigSnapshot, error) {
	r.mu.RLock()
	currentGeneration := r.generation
	r.mu.RUnlock()
	if generation != currentGeneration {
		return nil, conflictError("config generation changed")
	}
	r.preserveStartupOnlyConfig(cfg)
	r.preserveMaskedCredentials(cfg)
	state, err := r.buildServerState(cfg)
	if err != nil {
		return nil, err
	}
	newBindServers, removedBindServers, err := r.prepareBindServers(state)
	if err != nil {
		return nil, err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		closePreparedServers(newBindServers)
		return nil, err
	}
	if len(data) > defaultConfigMaxBytes() {
		closePreparedServers(newBindServers)
		return nil, errors.New("config is too large")
	}
	if _, err = r.store.Put(ctx, systemTenant, configPath, bytes.NewReader(data), map[string]string{"type": "config", "updated-at": time.Now().UTC().Format(time.RFC3339)}); err != nil {
		closePreparedServers(newBindServers)
		return nil, err
	}
	next, nextGen, err := loadConfig(ctx, r.store)
	if err != nil {
		closePreparedServers(newBindServers)
		return nil, err
	}
	r.mu.Lock()
	oldHandlers := append([]*proxy.Handler(nil), r.handlers...)
	r.config = next
	r.generation = nextGen
	r.applyServerState(state)
	if r.started {
		for _, prepared := range newBindServers {
			prepared.start()
			r.bindServers = append(r.bindServers, prepared.server)
		}
		for _, removed := range removedBindServers {
			for i, srv := range r.bindServers {
				if srv.Addr == removed.Addr {
					r.bindServers = append(r.bindServers[:i], r.bindServers[i+1:]...)
					break
				}
			}
		}
	}
	r.mu.Unlock()

	shutdownServerListWithTimeout(removedBindServers)
	for _, handler := range oldHandlers {
		handler.Close()
	}
	return &ConfigSnapshot{Generation: nextGen, Config: next, YAML: string(data)}, nil
}

func (r *Runtime) buildMainHandler(cfg *config.Config) error {
	state, err := r.buildServerState(cfg)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.applyServerState(state)
	r.mu.Unlock()

	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/-/api/runtime", r.runtimeAPI)
	apiMux.HandleFunc("/-/api/config", r.configAPI)
	apiMux.HandleFunc("/-/api/config/validate", r.validateAPI)
	apiMux.HandleFunc("/-/api/config/reset", r.resetAPI)
	apiMux.HandleFunc("/-/api/instances", r.instancesAPI)
	apiMux.HandleFunc("/-/api/instances/export", r.instancesExportAPI)
	apiMux.HandleFunc("/-/api/instances/import", r.instancesImportAPI)
	apiMux.HandleFunc("/-/api/metrics/stats", r.metricsStatsAPI)
	apiMux.HandleFunc("/-/api/storage/stats", r.storageStatsAPI)
	apiMux.HandleFunc("/-/api/storage/gc", r.storageGCAPI)
	apiMux.HandleFunc("/-/api/cache/lookup", r.cacheLookupAPI)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /-/api/login", r.loginHandler)
	mux.HandleFunc("POST /-/api/logout", r.logoutHandler)
	mux.HandleFunc("GET /-/api/public/instances", r.publicInstancesAPI)
	mux.Handle("GET /-/metrics", metricsAuthMiddleware(r.metricsToken,
		promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, r.metricsReg}, promhttp.HandlerOpts{})))
	mux.Handle("/-/api/", adminAuthMiddleware(r.password, apiMux))
	mux.HandleFunc("/", r.serveRoot)

	r.mu.Lock()
	r.apiMux = apiMux
	r.mainHandler = mux
	r.mu.Unlock()
	return nil
}

func (r *Runtime) serveRoot(w http.ResponseWriter, req *http.Request) {
	if prefix := r.matchProxyPrefix(req.URL.Path); prefix != "" {
		r.servePathProxyAt(w, req, prefix)
		return
	}
	r.serveSPA(w, req)
}

func (r *Runtime) matchProxyPrefix(target string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	prefixes := make([]string, 0, len(r.pathHandlers))
	for p := range r.pathHandlers {
		prefixes = append(prefixes, p)
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

func (r *Runtime) buildServerState(cfg *config.Config) (*serverState, error) {
	if err := r.validateConfig(cfg); err != nil {
		return nil, err
	}
	handlers := []*proxy.Handler{}
	pathHandlers := map[string]*proxy.Handler{}
	bindHandlers := map[string]*proxy.Handler{}
	for _, instanceName := range sortedInstanceNames(cfg.Instances) {
		instance := cfg.Instances[instanceName]
		resolver, err := newResolver(instance)
		if err != nil {
			return nil, err
		}
		handler := proxy.NewHandler(instanceName, instance, r.store, resolver, r.stats)
		handlers = append(handlers, handler)
		if instance.Listen.Path != "" {
			pathHandlers["/"+strings.Trim(instance.Listen.Path, "/")] = handler
		} else {
			bindHandlers[instance.Listen.Bind] = handler
		}
	}
	return &serverState{handlers: handlers, pathHandlers: pathHandlers, bindHandlers: bindHandlers}, nil
}

func (r *Runtime) applyServerState(state *serverState) {
	r.handlers = state.handlers
	r.pathHandlers = state.pathHandlers
	r.bindHandlers = state.bindHandlers
}

func (r *Runtime) prepareBindServers(state *serverState) (map[string]preparedServer, []*http.Server, error) {
	r.mu.RLock()
	started := r.started
	currentAddrs := map[string]*http.Server{}
	for _, srv := range r.bindServers {
		currentAddrs[srv.Addr] = srv
	}
	r.mu.RUnlock()
	if !started {
		return nil, nil, nil
	}
	newServers := map[string]preparedServer{}
	for addr := range state.bindHandlers {
		if currentAddrs[addr] != nil {
			continue
		}
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			closePreparedServers(newServers)
			return nil, nil, fmt.Errorf("listen %s: %w", addr, err)
		}
		newServers[addr] = preparedServer{
			server:   &http.Server{Addr: addr, Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { r.serveBind(addr, w, req) })},
			listener: listener,
		}
	}
	removed := []*http.Server{}
	for addr, srv := range currentAddrs {
		if state.bindHandlers[addr] == nil {
			removed = append(removed, srv)
		}
	}
	return newServers, removed, nil
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

func (p preparedServer) start() {
	go func() {
		if err := p.server.Serve(p.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("bind server error", "addr", p.server.Addr, "err", err)
		}
	}()
}

func closePreparedServers(servers map[string]preparedServer) {
	for _, prepared := range servers {
		_ = prepared.listener.Close()
	}
}

func shutdownServerListWithTimeout(servers []*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(ctx)
	}
}

func (r *Runtime) preserveStartupOnlyConfig(cfg *config.Config) {
	r.mu.RLock()
	current := r.config
	r.mu.RUnlock()
	if cfg == nil || current == nil {
		return
	}
	cfg.Version = current.Version
	cfg.Server = current.Server
	cfg.Storage = current.Storage
}

func (r *Runtime) preserveMaskedCredentials(cfg *config.Config) {
	r.mu.RLock()
	current := r.config
	r.mu.RUnlock()
	if cfg == nil || current == nil {
		return
	}
	for name, inst := range cfg.Instances {
		if inst.OCI != nil && inst.OCI.Auth != nil {
			auth := inst.OCI.Auth
			if currentInst, ok := current.Instances[name]; ok && currentInst.OCI != nil && currentInst.OCI.Auth != nil {
				currentAuth := currentInst.OCI.Auth
				if auth.Password == "***" || (auth.Password == "" && currentAuth.Password != "") {
					auth.Password = currentAuth.Password
				}
				if auth.Token == "***" || (auth.Token == "" && currentAuth.Token != "") {
					auth.Token = currentAuth.Token
				}
			}
			cfg.Instances[name] = inst
		}
	}
}

func (r *Runtime) validateConfig(cfg *config.Config) error {
	return validateConfig(cfg, r.bind)
}

func newResolver(cfg config.InstanceConfig) (proxy.Resolver, error) {
	switch cfg.Mode {
	case config.ModeFile:
		return fileproxy.New(cfg.Cache), nil
	case config.ModeOCI:
		return ociproxy.New(cfg.OCI), nil
	case config.ModeNPM:
		return npmproxy.New(cfg.NPM), nil
	default:
		return nil, fmt.Errorf("unsupported mode %s", cfg.Mode)
	}
}

func (r *Runtime) gcLoop(interval time.Duration) {
	defer close(r.gcDone)
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.gcStop:
			return
		case <-ticker.C:
			if _, err := r.store.RunGC(context.Background(), blobfs.GCOptions{Compact: true}); err != nil {
				slog.Warn("blob gc failed", "err", err)
			}
		}
	}
}

func loadOrInitConfig(ctx context.Context, store *blobfs.Store) (*config.Config, uint64, error) {
	cfg, generation, err := loadConfig(ctx, store)
	if err == nil {
		return cfg, generation, nil
	}
	cfg = DefaultConfig()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, 0, err
	}
	if _, err = store.Put(ctx, systemTenant, configPath, bytes.NewReader(data), map[string]string{"type": "config", "updated-at": time.Now().UTC().Format(time.RFC3339)}); err != nil {
		return nil, 0, err
	}
	return loadConfig(ctx, store)
}

func loadConfig(ctx context.Context, store *blobfs.Store) (*config.Config, uint64, error) {
	data, generation, err := readConfigObject(ctx, store)
	if err != nil {
		return nil, 0, err
	}
	cfg, err := parseConfig(data)
	return cfg, generation, err
}

func readConfigObject(ctx context.Context, store *blobfs.Store) ([]byte, uint64, error) {
	reader, err := store.OpenObject(ctx, systemTenant, configPath)
	if err != nil {
		return nil, 0, err
	}
	defer reader.Close()
	info := reader.Info()
	maxBytes := defaultConfigMaxBytes()
	data, err := io.ReadAll(io.LimitReader(reader, int64(maxBytes)+1))
	if err != nil {
		return nil, 0, err
	}
	if len(data) > maxBytes {
		return nil, 0, errors.New("config is too large")
	}
	return data, info.Generation, nil
}

func parseConfig(data []byte) (*config.Config, error) {
	cfg := &config.Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func DefaultConfig() *config.Config {
	return &config.Config{
		Version: 1,
		Server:  config.ServerConfig{Metrics: config.MetricsConfig{Path: DefaultMetricsPath}},
		Storage: config.StorageConfig{GC: config.GCConfig{Blob: config.Duration(mustDefaultDuration("DefaultGCInterval", DefaultGCInterval))}},
		Instances: map[string]config.InstanceConfig{"example-files": {
			Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
			ExpireAfter: config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter)),
			Cache: config.CacheConfig{
				DefaultPolicy: config.PolicyBypass,
				Rules: []config.CacheRule{
					{Match: "**/*.iso", Policy: config.PolicyImmutable, ExpireAfter: config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter))},
					{Match: "**/repodata/**", Policy: config.PolicyRevalidate},
				},
			},
		}},
	}
}

func cloneConfig(cfg *config.Config) *config.Config {
	next := *cfg
	next.Instances = map[string]config.InstanceConfig{}
	for name, instance := range cfg.Instances {
		next.Instances[name] = instance
	}
	return &next
}

func ValidateConfig(cfg *config.Config, bind string) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	clone := cloneConfig(cfg)
	return validateConfig(clone, bind)
}

func validateConfig(cfg *config.Config, bind string) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	if cfg.Storage.GC.Blob <= 0 {
		cfg.Storage.GC.Blob = config.Duration(mustDefaultDuration("DefaultGCInterval", DefaultGCInterval))
	}
	if cfg.Server.Metrics.Path == "" {
		cfg.Server.Metrics.Path = DefaultMetricsPath
	}
	binds := map[string]string{}
	if bind != "" {
		binds[bind] = "main"
	}
	paths := map[string]string{}
	for instanceName, inst := range cfg.Instances {
		if inst.ExpireAfter <= 0 {
			inst.ExpireAfter = config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter))
		}
		if instanceName == "" || strings.ContainsAny(instanceName, `/\`) || instanceName == "." || instanceName == ".." {
			return fmt.Errorf("invalid instance name %q", instanceName)
		}
		if inst.Mode != config.ModeFile && inst.Mode != config.ModeOCI && inst.Mode != config.ModeNPM {
			return fmt.Errorf("instance %s has unsupported mode %q", instanceName, inst.Mode)
		}
		if inst.Mode == config.ModeOCI && inst.Listen.Bind == "" {
			return fmt.Errorf("instance %s in oci mode must use independent listen bind", instanceName)
		}
		if (inst.Listen.Path == "") == (inst.Listen.Bind == "") {
			return fmt.Errorf("instance %s must set exactly one listen path or bind", instanceName)
		}
		if inst.Listen.Path != "" && (!strings.HasPrefix(inst.Listen.Path, "/") || strings.Contains(inst.Listen.Path, "//")) {
			return fmt.Errorf("instance %s has invalid listen path", instanceName)
		}
		if inst.Listen.Path != "" {
			listenPath := "/" + strings.Trim(inst.Listen.Path, "/")
			if owner := paths[listenPath]; owner != "" {
				return fmt.Errorf("listen path %s conflicts between %s and %s", listenPath, owner, instanceName)
			}
			paths[listenPath] = instanceName
		}
		if inst.Listen.Bind != "" {
			if existing := binds[inst.Listen.Bind]; existing != "" {
				return fmt.Errorf("listen bind %s conflicts between %s and %s", inst.Listen.Bind, existing, instanceName)
			}
			binds[inst.Listen.Bind] = instanceName
		}
		if len(inst.Upstreams) == 0 {
			return fmt.Errorf("instance %s has no upstreams", instanceName)
		}
		if inst.Mode == config.ModeOCI && len(inst.Upstreams) != 1 {
			return fmt.Errorf("instance %s in oci mode must set exactly one upstream", instanceName)
		}
		if inst.Mode == config.ModeNPM && len(inst.Upstreams) != 1 {
			return fmt.Errorf("instance %s in npm mode must set exactly one upstream", instanceName)
		}
		if inst.Mode != config.ModeFile && len(inst.PassHeaders) > 0 {
			return fmt.Errorf("instance %s pass headers are only supported in file mode", instanceName)
		}
		if err := validatePassHeaders(inst.PassHeaders); err != nil {
			return fmt.Errorf("instance %s: %w", instanceName, err)
		}
		if err := validateTransport(inst.Transport); err != nil {
			return fmt.Errorf("instance %s: %w", instanceName, err)
		}
		for _, rawURL := range inst.Upstreams {
			parsed, err := url.Parse(rawURL)
			if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
				return fmt.Errorf("instance %s has invalid upstream %q", instanceName, rawURL)
			}
			if inst.Mode == config.ModeOCI {
				if _, err := containername.NewRegistry(parsed.Host); err != nil {
					return fmt.Errorf("instance %s has invalid OCI registry %q: %w", instanceName, parsed.Host, err)
				}
			}
		}
		if err := validateCache(inst.Cache); err != nil {
			return fmt.Errorf("instance %s: %w", instanceName, err)
		}
		if err := validateOCI(inst); err != nil {
			return fmt.Errorf("instance %s: %w", instanceName, err)
		}
		if err := validateNPM(inst); err != nil {
			return fmt.Errorf("instance %s: %w", instanceName, err)
		}
		cfg.Instances[instanceName] = inst
	}
	return nil
}

func validateTransport(transport *config.TransportConfig) error {
	if transport == nil {
		return nil
	}
	if transport.Proxy != "" {
		parsed, err := url.Parse(transport.Proxy)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "socks5") {
			return fmt.Errorf("invalid upstream proxy %q", transport.Proxy)
		}
	}
	return nil
}

func validatePassHeaders(headers []string) error {
	for _, header := range headers {
		name := strings.TrimSpace(header)
		if name == "" || strings.ContainsAny(name, " \t\r\n:") {
			return fmt.Errorf("invalid pass header %q", header)
		}
	}
	return nil
}

func validateOCI(inst config.InstanceConfig) error {
	if inst.Mode != config.ModeOCI || inst.OCI == nil {
		return nil
	}
	if inst.OCI.DefaultPolicy != "" && !validPolicy(inst.OCI.DefaultPolicy) {
		return fmt.Errorf("invalid oci default policy %q", inst.OCI.DefaultPolicy)
	}
	for i, rule := range inst.OCI.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("oci rule %d: match is empty", i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("oci rule %d: invalid match %q", i, rule.Match)
		}
		if rule.Policy != "" && !validPolicy(rule.Policy) {
			return fmt.Errorf("oci rule %d: invalid policy %q", i, rule.Policy)
		}
		if rule.ResourcePolicy != "" && rule.ResourcePolicy != "*" && rule.ResourcePolicy != "blob" && rule.ResourcePolicy != "manifest" && rule.ResourcePolicy != "tag" {
			return fmt.Errorf("oci rule %d: invalid resource_policy %q", i, rule.ResourcePolicy)
		}
	}
	if inst.OCI.Auth == nil {
		return nil
	}
	switch strings.ToLower(inst.OCI.Auth.Type) {
	case "", "none":
		inst.OCI.Auth = nil
	case "basic":
		if inst.OCI.Auth.Username == "" {
			return errors.New("oci basic auth username is empty")
		}
	case "bearer":
		if inst.OCI.Auth.Token == "" {
			return errors.New("oci bearer auth token is empty")
		}
	default:
		return fmt.Errorf("unsupported oci auth type %q", inst.OCI.Auth.Type)
	}
	return nil
}

func validateNPM(inst config.InstanceConfig) error {
	if inst.Mode != config.ModeNPM || inst.NPM == nil {
		return nil
	}
	if inst.NPM.DefaultPolicy != "" && !validPolicy(inst.NPM.DefaultPolicy) {
		return fmt.Errorf("invalid npm default policy %q", inst.NPM.DefaultPolicy)
	}
	for i, rule := range inst.NPM.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("npm rule %d: match is empty", i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("npm rule %d: invalid match %q", i, rule.Match)
		}
		if rule.Policy != "" && !validPolicy(rule.Policy) {
			return fmt.Errorf("npm rule %d: invalid policy %q", i, rule.Policy)
		}
		if rule.ResourcePolicy != "" && rule.ResourcePolicy != "*" && rule.ResourcePolicy != "metadata" && rule.ResourcePolicy != "tarball" {
			return fmt.Errorf("npm rule %d: invalid resource_policy %q", i, rule.ResourcePolicy)
		}
	}
	return nil
}

func validateCache(cache config.CacheConfig) error {
	if cache.DefaultPolicy != "" && !validPolicy(cache.DefaultPolicy) {
		return fmt.Errorf("invalid default policy %q", cache.DefaultPolicy)
	}
	if cache.FreshFor < 0 {
		return errors.New("cache fresh_for must not be negative")
	}
	if cache.BusyPolicy != "" && cache.BusyPolicy != config.BusyPolicyBypass && cache.BusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid cache busy policy %q", cache.BusyPolicy)
	}
	for _, rule := range cache.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return errors.New("cache rule match is empty")
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("invalid cache rule match %q", rule.Match)
		}
		if !validPolicy(rule.Policy) {
			return fmt.Errorf("invalid cache policy %q", rule.Policy)
		}
	}
	return nil
}

func validPolicy(policy string) bool {
	return policy == "" || policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate
}

func sortedInstanceNames(instances map[string]config.InstanceConfig) []string {
	names := make([]string, 0, len(instances))
	for name := range instances {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type conflictError string

func (e conflictError) Error() string { return string(e) }
