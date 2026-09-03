package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

type Instance interface {
	http.Handler
	Start(context.Context) error
	Stop(context.Context) error
}

// StatusSource allows an Instance to provide custom dashboard status.
type StatusSource interface {
	DashboardStatus() (color, label, extra string)
}

type HandlerInstance struct {
	Handler      http.Handler
	CloseContext func(context.Context) error
}

func (h HandlerInstance) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.Handler.ServeHTTP(w, req)
}

func (h HandlerInstance) Start(context.Context) error { return nil }

func (h HandlerInstance) Stop(ctx context.Context) error {
	if h.CloseContext != nil {
		return h.CloseContext(ctx)
	}
	return nil
}

type ModeDriver interface {
	Mode() string
	Plan(context.Context, *InstancePlan) error
}

type Entry struct {
	Name       string
	Mode       string
	Enabled    bool
	Path       string
	Bind       string
	DisplayURL string
	Runtime    Instance
	Ctx        context.Context
	Cancel     context.CancelFunc
}

type Result struct {
	Entries []*Entry
	Stores  map[string]*blobfs.Store
}

type PlanContext struct {
	backend              string
	stores               map[string]*blobfs.Store
	stats                *metrics.Stats
	upstreamGate         *UpstreamGate
	spoolBudget          *SpoolBudget
	maxCacheObjectSize   int64
	cleanup              config.CleanupConfig
	mainBind             string
	metricsPath          string
	entries              map[string]*Entry
	pathOwners           map[string]string
	reservedPathPrefixes map[string]string
	bindOwners           map[string]string
	scheduler            *scheduler.Scheduler
}

type InstancePlan struct {
	ctx      *PlanContext
	decl     config.Instance
	selected config.SelectedMode
	entry    *Entry
	store    *blobfs.Store
	root     string
	bound    bool
}

func NewPlanContext(
	backend string,
	stats *metrics.Stats,
	upstreamGate *UpstreamGate,
	spoolBudget *SpoolBudget,
	maxCacheObjectSize int64,
	cleanup config.CleanupConfig,
	mainBind string,
	metricsPath string,
	taskScheduler *scheduler.Scheduler,
) *PlanContext {
	return &PlanContext{
		backend:              backend,
		stores:               map[string]*blobfs.Store{},
		stats:                stats,
		upstreamGate:         upstreamGate,
		spoolBudget:          spoolBudget,
		maxCacheObjectSize:   maxCacheObjectSize,
		cleanup:              cleanup,
		mainBind:             mainBind,
		metricsPath:          metricsPath,
		entries:              map[string]*Entry{},
		pathOwners:           map[string]string{},
		reservedPathPrefixes: map[string]string{},
		bindOwners:           map[string]string{mainBind: "main"},
		scheduler:            taskScheduler,
	}
}

func (p *PlanContext) Instance(decl config.Instance, selected config.SelectedMode) (*InstancePlan, error) {
	name := strings.TrimSpace(selected.Name)
	if !config.ValidInstanceName(name) {
		return nil, fmt.Errorf("invalid instance name %q", selected.Name)
	}
	if _, exists := p.entries[name]; exists {
		return nil, fmt.Errorf("duplicate instance name %q", name)
	}
	entry := &Entry{
		Name:    name,
		Mode:    selected.Mode,
		Enabled: selected.Enabled,
	}
	p.entries[name] = entry
	plan := &InstancePlan{ctx: p, decl: decl, selected: selected, entry: entry}
	if !entry.Enabled {
		return plan, nil
	}
	root := filepath.Join(p.backend, "instances", name, selected.Mode)
	for _, directory := range []string{"blobs", "state", "work"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o755); err != nil {
			return nil, fmt.Errorf("instance %s: create %s directory: %w", name, directory, err)
		}
	}
	store, err := blobfs.Open(filepath.Join(root, "blobs"), instanceBlobFSConfig())
	if err != nil {
		return nil, fmt.Errorf("instance %s: open blob store: %w", name, err)
	}
	plan.store = store
	plan.root = root
	p.stores[name] = store
	return plan, nil
}

func (p *PlanContext) Finalize() (*Result, error) {
	entries := make([]*Entry, 0, len(p.entries))
	for _, name := range SortedNames(p.entries) {
		entry := p.entries[name]
		if !entry.Enabled {
			entries = append(entries, entry)
			continue
		}
		if entry.Runtime == nil {
			return nil, fmt.Errorf("instance %s: runtime not configured", entry.Name)
		}
		if (entry.Path == "") == (entry.Bind == "") {
			return nil, fmt.Errorf("instance %s: must bind exactly one path or address", entry.Name)
		}
		entries = append(entries, entry)
	}
	return &Result{Entries: entries, Stores: p.stores}, nil
}

func (p *PlanContext) CloseStores() {
	for name, store := range p.stores {
		_ = store.Close()
		delete(p.stores, name)
	}
}

func (p *PlanContext) ReservePathPrefix(pathValue, owner string) {
	if normalized := normalizeRoutePath(pathValue); normalized != "" && normalized != "/" {
		p.reservedPathPrefixes[normalized] = owner
	}
}

func (i *InstancePlan) Name() string                        { return i.entry.Name }
func (i *InstancePlan) Enabled() bool                       { return i.entry.Enabled }
func (i *InstancePlan) Store() *blobfs.Store                { return i.store }
func (i *InstancePlan) StoreRoot() string                   { return i.root }
func (i *InstancePlan) Stats() *metrics.Stats               { return i.ctx.stats }
func (i *InstancePlan) UpstreamGate() *UpstreamGate         { return i.ctx.upstreamGate }
func (i *InstancePlan) SpoolBudget() *SpoolBudget           { return i.ctx.spoolBudget }
func (i *InstancePlan) MaxCacheObjectSize() int64           { return i.ctx.maxCacheObjectSize }
func (i *InstancePlan) CleanupConfig() config.CleanupConfig { return i.ctx.cleanup }
func (i *InstancePlan) Scheduler() *scheduler.Scheduler     { return i.ctx.scheduler }
func (i *InstancePlan) Path() string                        { return strings.TrimSpace(i.decl.Path) }
func (i *InstancePlan) Bind() string                        { return strings.TrimSpace(i.decl.Bind) }
func (i *InstancePlan) DisplayURL() string                  { return strings.TrimSpace(i.decl.DisplayURL) }
func (i *InstancePlan) Transport() *config.TransportConfig  { return i.decl.Transport }

func instanceBlobFSConfig() blobfs.Config {
	cfg := blobfs.DefaultConfig()
	cfg.MaxOpenWriteSessions = 128
	cfg.Chunking.MaxSize = 4 << 20
	if cfg.Chunking.AvgSize > cfg.Chunking.MaxSize {
		cfg.Chunking.AvgSize = cfg.Chunking.MaxSize
	}
	if cfg.Chunking.MinSize > cfg.Chunking.AvgSize {
		cfg.Chunking.MinSize = cfg.Chunking.AvgSize
	}
	return cfg
}

func (i *InstancePlan) Upstream() string { return strings.TrimSpace(i.decl.Upstream) }

func (i *InstancePlan) Decode(target any) error {
	if i.selected.Options == nil {
		return nil
	}
	if err := i.selected.Options.DecodeStrict(target); err != nil {
		return fmt.Errorf("instance %s: %w", i.entry.Name, err)
	}
	return nil
}

func (i *InstancePlan) RejectOptions() error {
	return i.Decode(&struct{}{})
}

func (i *InstancePlan) BindPath(pathValue string, runtime Instance) error {
	if err := i.bind(pathValue, "", runtime); err != nil {
		return fmt.Errorf("instance %s: %w", i.entry.Name, err)
	}
	return nil
}

func (i *InstancePlan) BindAddr(addr string, runtime Instance) error {
	if err := i.bind("", addr, runtime); err != nil {
		return fmt.Errorf("instance %s: %w", i.entry.Name, err)
	}
	return nil
}

func (i *InstancePlan) SetHomeDisplayURL(url string) {
	i.entry.DisplayURL = strings.TrimSpace(url)
}

func (i *InstancePlan) bind(pathValue, addr string, runtime Instance) error {
	if !i.entry.Enabled {
		return nil
	}
	if i.bound {
		return errors.New("instance already declared a mount")
	}
	if runtime == nil {
		return errors.New("runtime is nil")
	}
	if pathValue != "" {
		normalized := normalizeRoutePath(pathValue)
		if normalized == "" || normalized == "/" || strings.Contains(normalized, "//") {
			return fmt.Errorf("invalid listen path %q", pathValue)
		}
		if owner := i.ctx.pathOwners[normalized]; owner != "" {
			return fmt.Errorf("listen path %s conflicts between %s and %s", normalized, owner, i.entry.Name)
		}
		for reserved, owner := range i.ctx.reservedPathPrefixes {
			if normalized == reserved || strings.HasPrefix(normalized, reserved+"/") || strings.HasPrefix(reserved, normalized+"/") {
				return fmt.Errorf("listen path %s conflicts with %s at %s", normalized, owner, reserved)
			}
		}
		if normalized == i.ctx.metricsPath {
			return fmt.Errorf("listen path %s conflicts with metrics path", normalized)
		}
		i.ctx.pathOwners[normalized] = i.entry.Name
		i.entry.Path = normalized
	} else {
		trimmed := strings.TrimSpace(addr)
		if err := ValidateBindAddress(trimmed); err != nil {
			return err
		}
		if owner := i.ctx.bindOwners[trimmed]; owner != "" {
			return fmt.Errorf("listen bind %s conflicts between %s and %s", trimmed, owner, i.entry.Name)
		}
		i.ctx.bindOwners[trimmed] = i.entry.Name
		i.entry.Bind = trimmed
	}
	i.entry.Runtime = runtime
	i.bound = true
	return nil
}

func normalizeRoutePath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	normalized := path.Clean("/" + strings.TrimPrefix(trimmed, "/"))
	if normalized == "." {
		return ""
	}
	return normalized
}

func ValidateBindAddress(bind string) error {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return fmt.Errorf("invalid listen bind %q: must be host:port format", bind)
	}
	if host != "" && host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("invalid listen bind %q: invalid host %q", bind, host)
	}
	if port == "" {
		return fmt.Errorf("invalid listen bind %q: missing port", bind)
	}
	return nil
}

func SortedNames[T any](items map[string]T) []string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
