package runtime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path"
	"sort"
	"strings"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type Instance interface {
	http.Handler
	Start(context.Context) error
	Stop(context.Context) error
	Cleanup(context.Context) error
}

type HandlerInstance struct {
	Handler   http.Handler
	Close     func() error
	CleanupFn func(context.Context) error
}

func (h HandlerInstance) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.Handler.ServeHTTP(w, req)
}

func (h HandlerInstance) Start(context.Context) error { return nil }

func (h HandlerInstance) Stop(context.Context) error {
	if h.Close != nil {
		return h.Close()
	}
	return nil
}

func (h HandlerInstance) Cleanup(ctx context.Context) error {
	if h.CleanupFn != nil {
		return h.CleanupFn(ctx)
	}
	return nil
}

type ModeDriver interface {
	Mode() string
	Plan(context.Context, *InstancePlan) error
}

type HomeEntry struct {
	Name    string
	Mode    string
	Snippet string
}

type Entry struct {
	Name        string
	Mode        string
	Enabled     bool
	Path        string
	Bind        string
	ExpireAfter config.Expiration
	Runtime     Instance
	Home        HomeEntry
}

type Result struct {
	Entries []*Entry
}

type PlanContext struct {
	store       *blobfs.Store
	stats       *httpcache.Stats
	mainBind    string
	metricsPath string
	entries     map[string]*Entry
	pathOwners  map[string]string
	bindOwners  map[string]string
}

type InstancePlan struct {
	ctx      *PlanContext
	decl     config.Instance
	selected config.SelectedMode
	entry    *Entry
	bound    bool
}

func NewPlanContext(store *blobfs.Store, stats *httpcache.Stats, mainBind, metricsPath string) *PlanContext {
	return &PlanContext{
		store:       store,
		stats:       stats,
		mainBind:    mainBind,
		metricsPath: metricsPath,
		entries:     map[string]*Entry{},
		pathOwners:  map[string]string{},
		bindOwners:  map[string]string{mainBind: "main"},
	}
}

func (p *PlanContext) Instance(decl config.Instance, selected config.SelectedMode) (*InstancePlan, error) {
	name := strings.TrimSpace(selected.Name)
	if name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return nil, fmt.Errorf("invalid instance name %q", selected.Name)
	}
	if _, exists := p.entries[name]; exists {
		return nil, fmt.Errorf("duplicate instance name %q", name)
	}
	entry := &Entry{
		Name:    name,
		Mode:    selected.Mode,
		Enabled: selected.Enabled,
		Home: HomeEntry{
			Name: name,
			Mode: selected.Mode,
		},
	}
	p.entries[name] = entry
	return &InstancePlan{ctx: p, decl: decl, selected: selected, entry: entry}, nil
}

func (p *PlanContext) Finalize() (*Result, error) {
	entries := make([]*Entry, 0, len(p.entries))
	for _, name := range sortedNames(p.entries) {
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
	return &Result{Entries: entries}, nil
}

func (p *PlanContext) Store() *blobfs.Store    { return p.store }
func (p *PlanContext) Stats() *httpcache.Stats { return p.stats }

func (i *InstancePlan) Name() string            { return i.entry.Name }
func (i *InstancePlan) Mode() string            { return i.entry.Mode }
func (i *InstancePlan) Enabled() bool           { return i.entry.Enabled }
func (i *InstancePlan) Store() *blobfs.Store    { return i.ctx.store }
func (i *InstancePlan) Stats() *httpcache.Stats { return i.ctx.stats }

func (i *InstancePlan) Decode(target any) error {
	if i.selected.Block == nil {
		return fmt.Errorf("instance %s: missing %s block", i.entry.Name, i.entry.Mode)
	}
	if err := i.selected.Block.DecodeStrict(target); err != nil {
		return fmt.Errorf("instance %s: %w", i.entry.Name, err)
	}
	return nil
}

func (i *InstancePlan) BindPath(pathValue string, expireAfter config.Expiration, runtime Instance) error {
	if err := i.bind(pathValue, "", expireAfter, runtime); err != nil {
		return fmt.Errorf("instance %s: %w", i.entry.Name, err)
	}
	return nil
}

func (i *InstancePlan) BindAddr(addr string, expireAfter config.Expiration, runtime Instance) error {
	if err := i.bind("", addr, expireAfter, runtime); err != nil {
		return fmt.Errorf("instance %s: %w", i.entry.Name, err)
	}
	return nil
}

func (i *InstancePlan) SetHomeSnippet(snippet string) {
	i.entry.Home.Snippet = strings.TrimSpace(snippet)
}

func (i *InstancePlan) RenderSnippet() string {
	data, err := yaml.Marshal(i.decl)
	if err != nil {
		return "unable to render instance YAML"
	}
	return strings.TrimSpace(string(data))
}

func (i *InstancePlan) bind(pathValue, addr string, expireAfter config.Expiration, runtime Instance) error {
	if !i.entry.Enabled {
		i.entry.ExpireAfter = expireAfter
		return nil
	}
	if i.bound {
		return fmt.Errorf("instance already declared a mount")
	}
	if runtime == nil {
		return fmt.Errorf("runtime is nil")
	}
	if pathValue != "" {
		normalized := normalizeRoutePath(pathValue)
		if normalized == "" || normalized == "/" || strings.Contains(normalized, "//") {
			return fmt.Errorf("invalid listen path %q", pathValue)
		}
		if owner := i.ctx.pathOwners[normalized]; owner != "" {
			return fmt.Errorf("listen path %s conflicts between %s and %s", normalized, owner, i.entry.Name)
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
	i.entry.ExpireAfter = expireAfter
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

func sortedNames[T any](items map[string]T) []string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
