package flatpak

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

const maxDescriptorSize = 1 << 20

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeFlatpak }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	origin, err := url.Parse(plan.Upstream())
	if err != nil {
		return fmt.Errorf("instance %s: parse Flatpak upstream: %w", plan.Name(), err)
	}
	client, err := transport.NewPlanClient(plan, config.ModeFlatpak)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler, err := newHandler(plan.Name(), filepath.Join(plan.StoreRoot(), "state"), origin, filepath.Join(plan.StoreRoot(), "work"), plan.Store(), client, plan.Scheduler())
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), flatpakTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: handler.CloseContext})
}
