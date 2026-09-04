package npm

import (
	"context"
	"fmt"
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
	client, err := transport.NewPlanClient(plan, config.ModeNPM)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler, err := newHandler(
		plan.Name(), plan.Upstream(), filepath.Join(plan.StoreRoot(), "state"), filepath.Join(plan.StoreRoot(), "work"),
		plan.Store(), client, plan.Stats(),
	)
	if err != nil {
		return err
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), npmTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: handler.CloseContext})
}
