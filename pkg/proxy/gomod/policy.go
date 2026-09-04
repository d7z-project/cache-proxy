package gomod

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/mod/module"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type SumDBConfig struct {
	Enabled bool   `yaml:"enabled"`
	Name    string `yaml:"name,omitempty"`
	URL     string `yaml:"url,omitempty"`
}

type Config struct {
	SumDB                    *SumDBConfig `yaml:"sumdb,omitempty"`
	GOPrivate                []string     `yaml:"goprivate,omitempty"`
	DisableModuleFetchHeader bool         `yaml:"disable_module_fetch_header,omitempty"`
}

func Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var options Config
	if err := plan.Decode(&options); err != nil {
		return err
	}
	applyDefaults(&options)
	if err := validateOptions(&options); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	if !plan.Enabled() {
		return nil
	}
	workDir := filepath.Join(plan.StoreRoot(), "work")
	spooler := storeio.NewSpooler(workDir, plan.MaxCacheObjectSize(), plan.SpoolBudget())
	handler, err := newHandler(plan.Name(), plan.Upstream(), plan.Transport(), &options, plan.Store(), plan.Stats(), plan.UpstreamGate(), spooler)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), goTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{
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

func validateOptions(cfg *Config) error {
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
