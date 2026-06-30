package gomod

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"golang.org/x/mod/module"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const defaultCleanupInterval = 6 * time.Hour

type SumDBConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Name    string `json:"name,omitempty" yaml:"name,omitempty"`
	URL     string `json:"url,omitempty" yaml:"url,omitempty"`
}

type Config struct {
	SumDB                    *SumDBConfig     `json:"sumdb,omitempty" yaml:"sumdb,omitempty"`
	GOPrivate                []string         `json:"goprivate,omitempty" yaml:"goprivate,omitempty"`
	DisableModuleFetchHeader bool             `json:"disableModuleFetchHeader,omitempty" yaml:"disable_module_fetch_header,omitempty"`
	ModulePolicy             string           `json:"modulePolicy,omitempty" yaml:"module_policy,omitempty"`
	ModuleFreshFor           config.Freshness `json:"moduleFreshFor,omitempty" yaml:"module_fresh_for,omitempty"`
	ModuleBusyPolicy         string           `json:"moduleBusyPolicy,omitempty" yaml:"module_busy_policy,omitempty"`
	ZipPolicy                string           `json:"zipPolicy,omitempty" yaml:"zip_policy,omitempty"`
	SumDBFreshFor            config.Freshness `json:"sumdbFreshFor,omitempty" yaml:"sumdb_fresh_for,omitempty"`
	SumDBBusyPolicy          string           `json:"sumdbBusyPolicy,omitempty" yaml:"sumdb_busy_policy,omitempty"`
}

type Policy = Config

type Block struct {
	ExpireAfter config.Expiration `yaml:"expire_after"`
	Route       struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
	Proxies   []string                `yaml:"proxies"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Config    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeGo }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	applyDefaults(&block.Config)
	if err := validateBlock(block.Proxies, &block.Config); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler, err := NewHandler(plan.Name(), expireAfter, block.Proxies, block.Transport, &block.Config, plan.Store(), plan.Stats(), plan.Downloads())
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: defaultCleanupInterval,
		Handler: func(ctx context.Context) error {
			return handler.Cleanup(ctx, plan.CleanupConfig())
		},
	})
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:      handler,
		Close:        func() error { handler.Close(); return nil },
		CloseContext: handler.CloseContext,
		CleanupFn:    handler.Cleanup,
	})
}

func applyDefaults(cfg *Config) {
	if cfg.ModulePolicy == "" {
		cfg.ModulePolicy = config.PolicyRevalidate
	}
	if cfg.ModuleFreshFor == 0 {
		cfg.ModuleFreshFor = config.Freshness(time.Minute)
	}
	if cfg.ModuleBusyPolicy == "" {
		cfg.ModuleBusyPolicy = config.BusyPolicyStale
	}
	if cfg.ZipPolicy == "" {
		cfg.ZipPolicy = config.PolicyImmutable
	}
	if cfg.SumDBFreshFor == 0 {
		cfg.SumDBFreshFor = config.Freshness(30 * time.Second)
	}
	if cfg.SumDBBusyPolicy == "" {
		cfg.SumDBBusyPolicy = config.BusyPolicyBypass
	}
	if cfg.SumDB == nil {
		cfg.SumDB = &SumDBConfig{Enabled: true, Name: "sum.golang.org", URL: "https://sum.golang.org"}
		return
	}
	if !cfg.SumDB.Enabled {
		cfg.SumDB.Name = ""
		cfg.SumDB.URL = ""
		return
	}
	if strings.TrimSpace(cfg.SumDB.Name) == "" {
		cfg.SumDB.Name = "sum.golang.org"
	}
	if strings.TrimSpace(cfg.SumDB.URL) == "" {
		cfg.SumDB.URL = "https://sum.golang.org"
	}
}

func validateBlock(proxies []string, cfg *Config) error {
	if len(proxies) == 0 {
		return errors.New("go proxy requires at least one proxy")
	}
	for i, raw := range proxies {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("go proxy %d must be a valid absolute URL", i)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("go proxy %d must use http or https", i)
		}
	}
	if cfg.SumDB != nil && cfg.SumDB.Enabled {
		name := strings.TrimSpace(cfg.SumDB.Name)
		if name == "" {
			return errors.New("go sumdb name is required when sumdb proxying is enabled")
		}
		if strings.ContainsAny(name, "\r\n\t ") {
			return errors.New("go sumdb name must not contain spaces or line breaks")
		}
		rawURL := strings.TrimSpace(cfg.SumDB.URL)
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return errors.New("go sumdb upstream must be a valid absolute URL")
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return errors.New("go sumdb upstream must use http or https")
		}
	}
	for i, pattern := range cfg.GOPrivate {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			return fmt.Errorf("go goprivate %d is empty", i)
		}
		if strings.ContainsAny(pattern, "\r\n") {
			return fmt.Errorf("go goprivate %d must not contain line breaks", i)
		}
	}
	for _, value := range []string{cfg.ModulePolicy, cfg.ZipPolicy} {
		if value != config.PolicyBypass && value != config.PolicyImmutable && value != config.PolicyRevalidate {
			return fmt.Errorf("invalid go cache policy %q", value)
		}
	}
	for _, value := range []string{cfg.ModuleBusyPolicy, cfg.SumDBBusyPolicy} {
		if value != config.BusyPolicyBypass && value != config.BusyPolicyStale {
			return fmt.Errorf("invalid go busy policy %q", value)
		}
	}
	if cfg.ModuleFreshFor > 0 && cfg.ModuleFreshFor.Duration() < time.Second {
		return fmt.Errorf("go module fresh_for must be at least 1s")
	}
	if cfg.SumDBFreshFor > 0 && cfg.SumDBFreshFor.Duration() < time.Second {
		return fmt.Errorf("go sumdb fresh_for must be at least 1s")
	}
	return nil
}

func matchesPrivateModule(cfg *Config, modulePath string) bool {
	if cfg == nil || len(cfg.GOPrivate) == 0 || modulePath == "" {
		return false
	}
	patterns := make([]string, 0, len(cfg.GOPrivate))
	for _, pattern := range cfg.GOPrivate {
		pattern = strings.TrimSpace(pattern)
		if pattern != "" {
			patterns = append(patterns, pattern)
		}
	}
	if len(patterns) == 0 {
		return false
	}
	return module.MatchPrefixPatterns(strings.Join(patterns, ","), modulePath)
}

func modulePathFromTarget(target string) (string, bool) {
	target = strings.TrimPrefix(path.Clean("/"+target), "/")
	if target == "." || target == "" || strings.HasPrefix(target, "sumdb/") {
		return "", false
	}
	modulePath, _, ok := strings.Cut(target, "/@")
	if !ok || modulePath == "" {
		return "", false
	}
	unescaped, err := module.UnescapePath(modulePath)
	if err != nil || unescaped == "" {
		return "", false
	}
	return unescaped, true
}
