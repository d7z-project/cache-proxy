package gomod

import (
	"context"
	"errors"
	"fmt"
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
	SumDB                    *SumDBConfig `json:"sumdb,omitempty" yaml:"sumdb,omitempty"`
	GOPrivate                []string     `json:"goprivate,omitempty" yaml:"goprivate,omitempty"`
	DisableModuleFetchHeader bool         `json:"disableModuleFetchHeader,omitempty" yaml:"disable_module_fetch_header,omitempty"`
}

type Options = Config

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeGo }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var options Config
	if err := plan.Decode(&options); err != nil {
		return err
	}
	applyDefaults(&options)
	if err := validateBlock(plan.Upstreams(), &options); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	if !plan.Enabled() {
		return nil
	}
	handler, err := NewHandler(plan.Name(), plan.Retention(), plan.MetadataTTL(), plan.Upstreams(), plan.Transport(), &options, plan.Store(), plan.Stats(), plan.UpstreamGate())
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: defaultCleanupInterval,
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			return nil, handler.Cleanup(ctx, plan.CleanupConfig())
		},
	})
	return plan.BindPath(plan.Path(), plan.Retention(), proxyruntime.HandlerInstance{
		Handler:      handler,
		CloseContext: handler.CloseContext,
	})
}

func applyDefaults(cfg *Config) {
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
		if err := config.ValidateHTTPUpstream(raw); err != nil {
			return fmt.Errorf("go proxy %d: %w", i, err)
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
		if err := config.ValidateHTTPUpstream(cfg.SumDB.URL); err != nil {
			return fmt.Errorf("go sumdb upstream: %w", err)
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
