package npm

import (
	"errors"
	"net/http"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type Resolver struct {
	cfg *Policy
}

func New(cfg *Policy) *Resolver {
	return &Resolver{cfg: cfg}
}

func (r *Resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimLeft(req.URL.Path, "/")
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid npm request path")
	}
	upstreamPath := cleanPath
	objectPath := cleanPath
	if req.URL.RawQuery != "" {
		upstreamPath += "?" + req.URL.RawQuery
		objectPath += "/" + httpcache.HashKey(req.URL.RawQuery)
	}
	if strings.HasSuffix(cleanPath, ".tgz") {
		match := r.resolveResource("tarball")
		return httpcache.Route{
			ObjectPath:   "npm/tarballs/" + objectPath,
			UpstreamPath: upstreamPath,
			Policy:       match.policy,
			FreshFor:     match.freshFor,
			ExpireAfter:  match.expireAfter,
		}, nil
	}
	match := r.resolveResource("metadata")
	return httpcache.Route{
		ObjectPath:   "npm/metadata/" + httpcache.HashKey(objectPath),
		UpstreamPath: upstreamPath,
		Policy:       match.policy,
		FreshFor:     match.freshFor,
		ExpireAfter:  match.expireAfter,
		RewriteKind:  "npm-metadata",
	}, nil
}

type npmMatch struct {
	policy      string
	freshFor    config.Freshness
	expireAfter config.Expiration
}

func (r *Resolver) resolveResource(resourceType string) npmMatch {
	switch resourceType {
	case "tarball":
		return npmMatch{policy: r.cfg.TarballPolicy}
	default:
		return npmMatch{
			policy:   r.cfg.MetadataPolicy,
			freshFor: r.cfg.MetadataFreshFor,
		}
	}
}
