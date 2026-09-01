package apk

import (
	"context"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeAPK }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode: config.ModeAPK, ExpireAfter: plan.Retention(), MetadataTTL: plan.MetadataTTL(),
		Upstreams: plan.Upstreams(), Transport: plan.Transport(), UpstreamGate: plan.UpstreamGate(),
	}, plan.Store(), httpcache.NewPathResolver("apk", func(cleanPath string) httpcache.ObjectClass {
		name := path.Base(cleanPath)
		if name == "APKINDEX.tar.gz" || name == "APKINDEX.tar.gz.sig" || strings.HasPrefix(name, "APKINDEX.") {
			return httpcache.ClassMetadata
		}
		return httpcache.ClassContent
	}), plan.Stats())
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), handler)
}
