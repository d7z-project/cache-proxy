package maven

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeMaven }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:         config.ModeMaven,
		ExpireAfter:  plan.Retention(),
		MetadataTTL:  plan.MetadataTTL(),
		Upstreams:    plan.Upstreams(),
		Transport:    plan.Transport(),
		UpstreamGate: plan.UpstreamGate(),
	}, plan.Store(), &resolver{}, plan.Stats())
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), handler)
}

type resolver struct{}

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	lookupPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if !httpcache.SafePath(lookupPath) {
		return httpcache.Route{}, errors.New("invalid maven request path")
	}
	if lookupPath == "." || lookupPath == "" {
		return httpcache.Route{}, errors.New("path is required")
	}
	route := httpcache.Route{
		ObjectPath:   "maven/" + lookupPath,
		UpstreamPath: lookupPath,
		Class:        httpcache.ClassContent,
	}
	if isMetadataPath(lookupPath) || isSnapshotPath(lookupPath) || strings.Contains(lookupPath, "maven-metadata.xml.") {
		route.Class = httpcache.ClassMetadata
	}
	return route, nil
}

func isSnapshotPath(lookupPath string) bool {
	return strings.Contains(lookupPath, "-SNAPSHOT") || strings.Contains(lookupPath, "/SNAPSHOT/")
}

func isMetadataPath(lookupPath string) bool {
	return strings.HasSuffix(lookupPath, "maven-metadata.xml")
}
