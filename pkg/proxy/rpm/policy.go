package rpm

import (
	"context"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeRPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode: config.ModeRPM, ExpireAfter: plan.Retention(), MetadataTTL: plan.MetadataTTL(),
		Upstreams: plan.Upstreams(), Transport: plan.Transport(), UpstreamGate: plan.UpstreamGate(), VerifyFunc: verifyAnchoredObject,
	}, plan.Store(), anchoredResolver{}, plan.Stats())
	return plan.BindCachePath(plan.Path(), plan.Retention(), handler, &anchoredHandler{base: handler})
}
