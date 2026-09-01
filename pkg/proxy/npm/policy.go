package npm

import (
	"context"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeNPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	upstream := strings.TrimSpace(plan.Upstreams()[0])
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:              config.ModeNPM,
		ExpireAfter:       plan.Retention(),
		MetadataTTL:       plan.MetadataTTL(),
		Upstreams:         []string{upstream},
		Transport:         plan.Transport(),
		UpstreamGate:      plan.UpstreamGate(),
		ResponseTransform: httpcache.NPMResponseTransform([]string{upstream}),
	}, plan.Store(), New(), plan.Stats())
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), handler)
}
