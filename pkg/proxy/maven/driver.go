package maven

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if err := plan.RejectOptions(); err != nil {
		return err
	}
	if !plan.Enabled() {
		return nil
	}
	origin, err := url.Parse(plan.Upstream())
	if err != nil {
		return fmt.Errorf("instance %s: parse Maven upstream: %w", plan.Name(), err)
	}
	client, err := transport.NewPlanClient(plan, config.ModeMaven)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler := newHandler(origin, filepath.Join(plan.StoreRoot(), "work"), plan.Store(), client)
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), mavenTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: handler.CloseContext})
}
