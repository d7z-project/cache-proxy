package npm

import (
	"errors"
	"net/http"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

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
	packageName := extractPackageName(cleanPath)
	if strings.HasSuffix(cleanPath, ".tgz") {
		match := r.resolveResource(packageName, "tarball")
		return httpcache.Route{
			ObjectPath:   "npm/tarballs/" + objectPath,
			UpstreamPath: upstreamPath,
			Policy:       match.policy,
			FreshFor:     match.freshFor,
			ExpireAfter:  match.expireAfter,
		}, nil
	}
	match := r.resolveResource(packageName, "metadata")
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
	match := npmMatch{policy: r.cfg.DefaultPolicy}
	if match.policy == "" {
		match.policy = config.PolicyBypass
	}
	return match
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
