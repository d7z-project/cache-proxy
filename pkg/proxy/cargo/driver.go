package cargo

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

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeCargo }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if err := plan.RejectOptions(); err != nil {
		return err
	}
	if !plan.Enabled() {
		return nil
	}
	origin, err := url.Parse(plan.Upstream())
	if err != nil {
		return fmt.Errorf("instance %s: parse Cargo upstream: %w", plan.Name(), err)
	}
	client, err := transport.NewPlanClient(plan, config.ModeCargo)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler, err := newHandler(origin, filepath.Join(plan.StoreRoot(), "state"), filepath.Join(plan.StoreRoot(), "work"), plan.Store(), client)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), cargoTenant, plan.Store(), plan.CleanupConfig())
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: handler.CloseContext})
}

func cratePrefix(name string) string {
	characters := []rune(name)
	switch len(characters) {
	case 0:
		return ""
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3/" + string(characters[:1])
	default:
		return string(characters[:2]) + "/" + string(characters[2:4])
	}
}
