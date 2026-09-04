package pypi

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

var projectSeparator = regexp.MustCompile(`[-_.]+`)

func Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if err := plan.RejectOptions(); err != nil {
		return err
	}
	if !plan.Enabled() {
		return nil
	}
	origin, err := url.Parse(plan.Upstream())
	if err != nil {
		return fmt.Errorf("instance %s: parse PyPI upstream: %w", plan.Name(), err)
	}
	client, err := transport.NewPlanClient(plan, config.ModePyPI)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler, err := newHandler(origin, filepath.Join(plan.StoreRoot(), "state"), filepath.Join(plan.StoreRoot(), "work"), plan.Store(), client)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), pypiTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: handler.CloseContext})
}

func normalizeProjectName(name string) string {
	return projectSeparator.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-")
}
