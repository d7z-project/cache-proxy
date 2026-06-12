package npm

import (
	"errors"
	"net/http"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

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
	packageName := extractPackageName(cleanPath)
	if strings.HasSuffix(cleanPath, ".tgz") {
		match := r.resolveResource(packageName, "tarball")
		return proxy.Route{
			ObjectPath:   "npm/tarballs/" + objectPath,
			UpstreamPath: upstreamPath,
			Policy:       match.policy,
			FreshFor:     match.freshFor,
			ExpireAfter:  match.expireAfter,
		}, nil
	}
	match := r.resolveResource(packageName, "metadata")
	return proxy.Route{
		ObjectPath:         "npm/metadata/" + proxy.HashKey(objectPath),
		UpstreamPath:       upstreamPath,
		Policy:             match.policy,
		FreshFor:           match.freshFor,
		ExpireAfter:        match.expireAfter,
		RewriteNPMMetadata: true,
	}, nil
}

type npmMatch struct {
	policy      string
	freshFor    config.Duration
	expireAfter config.Duration
}

func (r *Resolver) resolveResource(packageName, resourceType string) npmMatch {
	for _, rule := range r.cfg.Rules {
		if rule.ResourcePolicy != "*" && rule.ResourcePolicy != resourceType {
			continue
		}
		if !doublestar.MatchUnvalidated(rule.Match, packageName) {
			continue
		}
		return npmMatch{
			policy:      rule.Policy,
			freshFor:    rule.FreshFor,
			expireAfter: rule.ExpireAfter,
		}
	}
	return npmMatch{policy: r.cfg.DefaultPolicy}
}

func extractPackageName(cleanPath string) string {
	cleanPath = strings.TrimPrefix(cleanPath, "/")
	if strings.HasPrefix(cleanPath, "@") {
		parts := strings.SplitN(cleanPath, "/", 3)
		if len(parts) >= 2 {
			return parts[0] + "/" + parts[1]
		}
	}
	parts := strings.SplitN(cleanPath, "/", 2)
	return parts[0]
}
