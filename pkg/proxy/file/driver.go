package file

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type Options struct {
	PassHeaders []string `yaml:"pass_headers,omitempty"`
	Rules       []Rule   `yaml:"rules"`
}

type Rule struct {
	Match  string `yaml:"match"`
	Policy string `yaml:"policy"`
}

func Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var options Options
	if err := plan.Decode(&options); err != nil {
		return err
	}
	for _, header := range options.PassHeaders {
		if strings.TrimSpace(header) == "" || http.CanonicalHeaderKey(header) == "" {
			return fmt.Errorf("instance %s: invalid pass header %q", plan.Name(), header)
		}
	}
	for index, rule := range options.Rules {
		if strings.TrimSpace(rule.Match) == "" || !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("instance %s: file rule %d has invalid match", plan.Name(), index)
		}
		switch rule.Policy {
		case "http_cache", "immutable", "passthrough":
		default:
			return fmt.Errorf("instance %s: file rule %d has invalid policy %q", plan.Name(), index, rule.Policy)
		}
	}
	if !plan.Enabled() {
		return nil
	}
	client, err := transport.NewPlanClient(plan, config.ModeFile)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler, err := newHandler(handlerConfig{
		name:        plan.Name(),
		upstream:    plan.Upstream(),
		passHeaders: options.PassHeaders,
		rules:       options.Rules,
		workDir:     filepath.Join(plan.StoreRoot(), "work"),
		store:       plan.Store(),
		client:      client,
		stats:       plan.Stats(),
	})
	if err != nil {
		return err
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), objectTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{
		Handler:      handler,
		CloseContext: handler.CloseContext,
	})
}
