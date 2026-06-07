package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
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
)

const (
	systemTenant = "_system"
	configPath   = "config.yaml"
)

var (
	DefaultBackend        = "/tmp/cache-proxy"
	DefaultAdminBind      = "127.0.0.1:18080"
	DefaultProxyBind      = "127.0.0.1:18081"
	DefaultMetricsBind    = "127.0.0.1:8911"
	DefaultMetricsPath    = "/metrics"
	DefaultGCInterval     = "24h"
	DefaultExpireAfter    = "720h"
	DefaultConfigMaxBytes = "1048576"
)

type Runtime struct {
	adminBind   string
	proxyBind   string
	backend     string
	metricsBind string
	metricsPath string
	gcInterval  time.Duration
	store       *blobfs.Store
	stats       *proxy.Stats
	metrics     *prometheus.Registry
	config      *config.Config
	generation  uint64
	mu          sync.RWMutex

	servers      []*http.Server
	handlers     []*proxy.Handler
	apiMux       *http.ServeMux
	pathHandlers map[string]*proxy.Handler
	bindHandlers map[string]*proxy.Handler

	activeServers     map[string]*http.Server
	activeBindServers map[string]*http.Server
	started           bool
	closed            atomic.Bool
	gcStop            chan struct{}
	gcDone            chan struct{}
}

type serverState struct {
	servers      []*http.Server
	handlers     []*proxy.Handler
	apiMux       *http.ServeMux
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
	Backend     string
	AdminBind   string
	ProxyBind   string
	MetricsBind string
	MetricsPath string
	GCInterval  time.Duration
}

func DefaultOptions() Options {
	return Options{
		Backend:     DefaultBackend,
		AdminBind:   DefaultAdminBind,
		ProxyBind:   DefaultProxyBind,
		MetricsBind: DefaultMetricsBind,
		MetricsPath: DefaultMetricsPath,
		GCInterval:  mustDefaultDuration("DefaultGCInterval", DefaultGCInterval),
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

func Open(ctx context.Context, backend, adminBind string) (*Runtime, error) {
	options := DefaultOptions()
	options.Backend = backend
	options.AdminBind = adminBind
	return OpenWithOptions(ctx, options)
}

func OpenWithOptions(ctx context.Context, options Options) (*Runtime, error) {
	if options.Backend == "" {
		return nil, errors.New("backend is empty")
	}
	if options.AdminBind == "" {
		return nil, errors.New("admin bind is empty")
	}
	if options.ProxyBind == "" {
		options.ProxyBind = DefaultProxyBind
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
	runtime := &Runtime{
		adminBind:   options.AdminBind,
		proxyBind:   options.ProxyBind,
		backend:     options.Backend,
		metricsBind: options.MetricsBind,
		metricsPath: options.MetricsPath,
		gcInterval:  options.GCInterval,
		store:       store,
		stats:       proxy.NewStats(),
		metrics:     prometheus.NewRegistry(),
		config:      cfg,
		generation:  generation,
		gcStop:      make(chan struct{}),
		gcDone:      make(chan struct{}),
	}
	runtime.metrics.MustRegister(newBlobfsCollector(store))
	if err := runtime.buildServers(); err != nil {
		_ = store.Close()
		return nil, err
	}
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

	listeners := make([]net.Listener, 0, len(r.servers))
	r.mu.RLock()
	servers := append([]*http.Server(nil), r.servers...)
	bindHandlers := map[string]*proxy.Handler{}
	for addr, handler := range r.bindHandlers {
		bindHandlers[addr] = handler
	}
	r.mu.RUnlock()
	for _, srv := range servers {
		listener, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen %s: %w", srv.Addr, err)
		}
		listeners = append(listeners, listener)
	}
	activeServers := map[string]*http.Server{}
	activeBindServers := map[string]*http.Server{}
	for i, srv := range servers {
		listener := listeners[i]
		activeServers[srv.Addr] = srv
		if _, ok := bindHandlers[srv.Addr]; ok {
			activeBindServers[srv.Addr] = srv
		}
		go func(server *http.Server) {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("server stopped with error", "addr", server.Addr, "err", err)
			}
		}(srv)
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		return nil
	}
	r.activeServers = activeServers
	r.activeBindServers = activeBindServers
	r.started = true
	r.mu.Unlock()
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
	activeServers := make([]*http.Server, 0, len(r.activeServers))
	handlers := append([]*proxy.Handler(nil), r.handlers...)
	if len(r.activeServers) > 0 {
		for _, srv := range r.activeServers {
			activeServers = append(activeServers, srv)
		}
	} else {
		activeServers = append(activeServers, r.servers...)
	}
	r.mu.RUnlock()
	for _, srv := range activeServers {
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
	if err := r.validateConfig(cfg); err != nil {
		return nil, err
	}
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
	next, generation, err := loadConfig(ctx, r.store)
	if err != nil {
		closePreparedServers(newBindServers)
		return nil, err
	}
	r.mu.Lock()
	oldHandlers := append([]*proxy.Handler(nil), r.handlers...)
	r.config = next
	r.generation = generation
	r.applyServerState(state)
	if r.started {
		if r.activeServers == nil {
			r.activeServers = map[string]*http.Server{}
		}
		if r.activeBindServers == nil {
			r.activeBindServers = map[string]*http.Server{}
		}
		for addr, prepared := range newBindServers {
			prepared.start()
			r.activeServers[addr] = prepared.server
			r.activeBindServers[addr] = prepared.server
		}
		for _, srv := range removedBindServers {
			delete(r.activeServers, srv.Addr)
			delete(r.activeBindServers, srv.Addr)
		}
	}
	r.mu.Unlock()
	shutdownServerListWithTimeout(removedBindServers)
	for _, handler := range oldHandlers {
		handler.Close()
	}
	return &ConfigSnapshot{Generation: generation, Config: next, YAML: string(data)}, nil
}

func (r *Runtime) buildServers() error {
	state, err := r.buildServerState(r.config)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.applyServerState(state)
	r.mu.Unlock()
	return nil
}

func (r *Runtime) buildServerState(cfg *config.Config) (*serverState, error) {
	if err := r.validateConfig(cfg); err != nil {
		return nil, err
	}
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/config", r.configAPI)
	apiMux.HandleFunc("/api/config/validate", r.validateAPI)
	apiMux.HandleFunc("/api/config/reset", r.resetAPI)
	apiMux.HandleFunc("/api/instances", r.instancesAPI)
	apiMux.HandleFunc("/api/instances/export", r.instancesExportAPI)
	apiMux.HandleFunc("/api/instances/import", r.instancesImportAPI)
	apiMux.HandleFunc("/api/runtime", r.runtimeAPI)
	apiMux.HandleFunc("/api/metrics/stats", r.metricsStatsAPI)
	apiMux.HandleFunc("/api/storage/stats", r.storageStatsAPI)
	apiMux.HandleFunc("/api/storage/gc", r.storageGCAPI)

	servers := []*http.Server{{Addr: r.adminBind, Handler: r}, {Addr: r.proxyBind, Handler: http.HandlerFunc(r.servePathProxy)}}
	handlers := []*proxy.Handler{}
	pathHandlers := map[string]*proxy.Handler{}
	bindHandlers := map[string]*proxy.Handler{}
	if r.metricsBind != "" && r.metricsPath != "" {
		mux := http.NewServeMux()
		mux.Handle(r.metricsPath, promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, r.metrics}, promhttp.HandlerOpts{}))
		servers = append(servers, &http.Server{Addr: r.metricsBind, Handler: mux})
	}
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
			addr := instance.Listen.Bind
			bindHandlers[addr] = handler
			servers = append(servers, &http.Server{Addr: addr, Handler: http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
				r.serveBind(addr, resp, req)
			})})
		}
	}
	return &serverState{servers: servers, handlers: handlers, apiMux: apiMux, pathHandlers: pathHandlers, bindHandlers: bindHandlers}, nil
}

func (r *Runtime) applyServerState(state *serverState) {
	r.servers = state.servers
	r.handlers = state.handlers
	r.apiMux = state.apiMux
	r.pathHandlers = state.pathHandlers
	r.bindHandlers = state.bindHandlers
}

func (r *Runtime) serveBind(addr string, resp http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	handler := r.bindHandlers[addr]
	r.mu.RUnlock()
	if handler == nil {
		http.NotFound(resp, req)
		return
	}
	handler.ServeHTTP(resp, req)
}

func (p preparedServer) start() {
	go func() {
		if err := p.server.Serve(p.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped with error", "addr", p.server.Addr, "err", err)
		}
	}()
}

func (r *Runtime) prepareBindServers(state *serverState) (map[string]preparedServer, []*http.Server, error) {
	r.mu.RLock()
	started := r.started
	activeBindServers := map[string]*http.Server{}
	for addr, srv := range r.activeBindServers {
		activeBindServers[addr] = srv
	}
	r.mu.RUnlock()
	if !started {
		return nil, nil, nil
	}
	newServers := map[string]preparedServer{}
	for addr := range state.bindHandlers {
		if activeBindServers[addr] != nil {
			continue
		}
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			closePreparedServers(newServers)
			return nil, nil, fmt.Errorf("listen %s: %w", addr, err)
		}
		server := &http.Server{Addr: addr, Handler: http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			r.serveBind(addr, resp, req)
		})}
		newServers[addr] = preparedServer{server: server, listener: listener}
	}
	removedServers := []*http.Server{}
	for addr, srv := range activeBindServers {
		if state.bindHandlers[addr] == nil {
			removedServers = append(removedServers, srv)
		}
	}
	return newServers, removedServers, nil
}

func closePreparedServers(servers map[string]preparedServer) {
	for _, prepared := range servers {
		_ = prepared.listener.Close()
	}
}

func shutdownServerList(ctx context.Context, servers []*http.Server) {
	for _, srv := range servers {
		_ = srv.Shutdown(ctx)
	}
}

func shutdownServerListWithTimeout(servers []*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownServerList(ctx, servers)
}

var pathIndexTemplate = template.Must(template.New("path-index").Parse(`<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>Cache Proxy</title></head>
<body>
<h1>可用代理路径</h1>
{{if .}}
<ul>{{range .}}<li><a href="{{.}}">{{.}}</a></li>{{end}}</ul>
{{else}}
<p>暂无路径代理实例。</p>
{{end}}
</body>
</html>`))

func (r *Runtime) pathIndex(resp http.ResponseWriter, req *http.Request, prefixes []string) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		resp.Header().Set("Allow", "GET, HEAD")
		http.Error(resp, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	resp.Header().Set("Content-Type", "text/html; charset=utf-8")
	if req.Method == http.MethodHead {
		return
	}
	if err := pathIndexTemplate.Execute(resp, prefixes); err != nil {
		slog.Warn("render path index failed", "err", err)
	}
}

func (r *Runtime) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	apiMux := r.apiMux
	r.mu.RUnlock()
	if strings.HasPrefix(req.URL.Path, "/api/") {
		apiMux.ServeHTTP(resp, req)
		return
	}
	r.webUI(resp, req)
}

func (r *Runtime) servePathProxy(resp http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	pathHandlers := r.pathHandlers
	prefixes := make([]string, 0, len(pathHandlers))
	for prefix := range pathHandlers {
		prefixes = append(prefixes, prefix)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(prefixes)))
	r.mu.RUnlock()
	for _, prefix := range prefixes {
		if req.URL.Path == prefix || strings.HasPrefix(req.URL.Path, prefix+"/") {
			next := req.Clone(req.Context())
			next.Header = req.Header.Clone()
			next.Header.Set("X-Cache-Proxy-Prefix", prefix)
			http.StripPrefix(prefix, pathHandlers[prefix]).ServeHTTP(resp, next)
			return
		}
	}
	r.pathIndex(resp, req, prefixes)
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

func (r *Runtime) validateConfig(cfg *config.Config) error {
	return validateConfig(cfg, r.adminBind, r.proxyBind, r.metricsBind)
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
		Server:  config.ServerConfig{Metrics: config.MetricsConfig{Bind: DefaultMetricsBind, Path: DefaultMetricsPath}},
		Storage: config.StorageConfig{GC: config.GCConfig{Blob: config.Duration(mustDefaultDuration("DefaultGCInterval", DefaultGCInterval))}},
		Instances: map[string]config.InstanceConfig{"example-files": {
			Mode: config.ModeFile, Listen: config.ListenConfig{Path: "/files"}, Upstreams: []string{"https://example.com"},
			ExpireAfter: config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter)),
			Cache:       config.CacheConfig{DefaultPolicy: config.PolicyBypass, Rules: []config.CacheRule{{Match: "**/*.iso", Policy: config.PolicyImmutable}, {Match: "**/repodata/**", Policy: config.PolicyRevalidate}}},
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

func ValidateConfig(cfg *config.Config, adminBind string) error {
	return validateConfig(cfg, adminBind, "", "")
}

func validateConfig(cfg *config.Config, adminBind, proxyBind, metricsBind string) error {
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
	listens := map[string]string{}
	addListen := func(addr, owner string) error {
		if addr == "" {
			return nil
		}
		if existing := listens[addr]; existing != "" {
			return fmt.Errorf("listen bind %s conflicts between %s and %s", addr, existing, owner)
		}
		listens[addr] = owner
		return nil
	}
	if err := addListen(adminBind, "admin"); err != nil {
		return err
	}
	if err := addListen(proxyBind, "proxy"); err != nil {
		return err
	}
	if metricsBind != "" {
		if err := addListen(metricsBind, "metrics"); err != nil {
			return err
		}
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
			if err := addListen(inst.Listen.Bind, instanceName); err != nil {
				return err
			}
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
	if inst.OCI.BlobPolicy != "" && !validPolicy(inst.OCI.BlobPolicy) {
		return fmt.Errorf("invalid oci blob policy %q", inst.OCI.BlobPolicy)
	}
	if inst.OCI.ManifestPolicy != "" && !validPolicy(inst.OCI.ManifestPolicy) {
		return fmt.Errorf("invalid oci manifest policy %q", inst.OCI.ManifestPolicy)
	}
	if inst.OCI.TagPolicy != "" && !validPolicy(inst.OCI.TagPolicy) {
		return fmt.Errorf("invalid oci tag policy %q", inst.OCI.TagPolicy)
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
	if inst.NPM.MetadataPolicy != "" && !validPolicy(inst.NPM.MetadataPolicy) {
		return fmt.Errorf("invalid npm metadata policy %q", inst.NPM.MetadataPolicy)
	}
	if inst.NPM.TarballPolicy != "" && !validPolicy(inst.NPM.TarballPolicy) {
		return fmt.Errorf("invalid npm tarball policy %q", inst.NPM.TarballPolicy)
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
