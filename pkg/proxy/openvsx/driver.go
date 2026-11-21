package openvsx

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	responseTenant    = "openvsx"
	metadataFreshness = 10 * time.Minute
	artifactFreshness = 24 * time.Hour
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
		return fmt.Errorf("instance %s: parse Open VSX upstream: %w", plan.Name(), err)
	}
	publicBase, err := url.Parse(plan.DisplayURL())
	if err != nil {
		return fmt.Errorf("instance %s: parse Open VSX display URL: %w", plan.Name(), err)
	}
	origin.Path, origin.RawPath = strings.TrimSuffix(origin.Path, "/"), ""
	publicBase.Path, publicBase.RawPath = strings.TrimSuffix(publicBase.Path, "/"), ""
	client, err := transport.NewPlanClient(plan, config.ModeOpenVSX)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler := newHandler(handlerConfig{
		name:       plan.Name(),
		origin:     origin,
		publicBase: publicBase,
		workDir:    filepath.Join(plan.StoreRoot(), "work"),
		store:      plan.Store(),
		client:     client,
		stats:      plan.Stats(),
		freshness:  plan.RefreshInterval(metadataFreshness),
	})
	handler.uncached = !plan.CacheEnabled()
	if plan.CacheEnabled() {
		storeio.RegisterResponseCleanup(plan.Scheduler(), plan.Name(), responseTenant, plan.Store(), plan.CleanupConfig())
	}
	plan.SetHomeDisplayURL(plan.DisplayURL())
	return plan.BindAddrRoot(plan.Bind(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: handler.CloseContext})
}
