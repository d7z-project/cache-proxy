package deb

import (
	"context"
	"path/filepath"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeDEB }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if err := plan.RejectOptions(); err != nil {
		return err
	}
	if !plan.Enabled() {
		return nil
	}
	client, err := transport.NewPlanClient(plan, config.ModeDEB)
	if err != nil {
		return err
	}
	handler, err := newHandler(
		plan.Name(), plan.Upstream(), filepath.Join(plan.StoreRoot(), "state"),
		filepath.Join(plan.StoreRoot(), "work"), plan.Store(),
		client,
		plan.Stats(),
		plan.Scheduler(),
	)
	if err != nil {
		return err
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), debArtifactTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{
		Handler:      handler,
		CloseContext: handler.CloseContext,
	})
}
