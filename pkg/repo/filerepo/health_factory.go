package filerepo

import (
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func NewServiceHealth(transport *config.TransportConfig, upstreams []string, plan *proxyruntime.InstancePlan, mode string) *health.ServiceHealth {
	cfg := health.DefaultConfig()
	if transport != nil {
		cfg = health.ApplyConfigPatch(cfg, transport.Health)
	}
	return health.New(plan.Name(), mode, cfg, upstreams, plan.Stats(), httpcache.ModeUserAgent(mode))
}
