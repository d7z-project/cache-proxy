package deb

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
func (Driver) Mode() string              { return config.ModeDEB }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode: config.ModeDEB, ExpireAfter: plan.Retention(), MetadataTTL: plan.MetadataTTL(),
		Upstreams: plan.Upstreams(), Transport: plan.Transport(), UpstreamGate: plan.UpstreamGate(), VerifyFunc: verifyAnchoredObject,
	}, plan.Store(), anchoredResolver{}, plan.Stats())
	return plan.BindCachePath(plan.Path(), plan.Retention(), handler, &anchoredHandler{base: handler})
}

func classifyPath(cleanPath string) httpcache.ObjectClass {
	name := path.Base(cleanPath)
	if strings.Contains(cleanPath, "/dists/") || strings.HasPrefix(cleanPath, "dists/") {
		return httpcache.ClassMetadata
	}
	for _, prefix := range []string{"Packages", "Sources", "Contents-", "Translation-"} {
		if strings.HasPrefix(name, prefix) {
			return httpcache.ClassMetadata
		}
	}
	switch name {
	case "Release", "InRelease", "Release.gpg", "Release.sig", "Release.asc":
		return httpcache.ClassMetadata
	default:
		return httpcache.ClassContent
	}
}
