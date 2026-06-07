package npm

import (
	"errors"
	"net/http"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

type Resolver struct {
	cfg *config.NPMConfig
}

func New(cfg *config.NPMConfig) *Resolver {
	return &Resolver{cfg: cfg}
}

func (r *Resolver) Resolve(req *http.Request) (proxy.Route, error) {
	cleanPath := strings.TrimLeft(req.URL.Path, "/")
	if !proxy.SafePath(cleanPath) {
		return proxy.Route{}, errors.New("invalid npm request path")
	}
	upstreamPath := cleanPath
	objectPath := cleanPath
	if req.URL.RawQuery != "" {
		upstreamPath += "?" + req.URL.RawQuery
		objectPath += "/" + proxy.HashKey(req.URL.RawQuery)
	}
	if strings.HasSuffix(cleanPath, ".tgz") {
		return proxy.Route{ObjectPath: "npm/tarballs/" + objectPath, UpstreamPath: upstreamPath, Policy: r.tarballPolicy()}, nil
	}
	return proxy.Route{ObjectPath: "npm/metadata/" + proxy.HashKey(objectPath), UpstreamPath: upstreamPath, Policy: r.metadataPolicy(), RewriteNPMMetadata: true}, nil
}

func (r *Resolver) metadataPolicy() string {
	if r.cfg != nil && r.cfg.MetadataPolicy != "" {
		return r.cfg.MetadataPolicy
	}
	return config.PolicyRevalidate
}

func (r *Resolver) tarballPolicy() string {
	if r.cfg != nil && r.cfg.TarballPolicy != "" {
		return r.cfg.TarballPolicy
	}
	return config.PolicyImmutable
}
