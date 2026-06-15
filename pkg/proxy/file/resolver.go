package file

import (
	"errors"
	"net/http"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

type Resolver struct {
	policy *Policy
}

func New(policy *Policy) *Resolver {
	return &Resolver{policy: policy}
}

func (r *Resolver) Resolve(req *http.Request) (proxy.Route, error) {
	cleanPath := strings.TrimLeft(req.URL.Path, "/")
	if cleanPath == "" {
		cleanPath = "index"
	}
	if !proxy.SafePath(cleanPath) {
		return proxy.Route{}, errors.New("invalid request path")
	}
	match := r.match(cleanPath)
	return proxy.Route{
		ObjectPath:   "file/" + cleanPath,
		UpstreamPath: cleanPath,
		Policy:       match.policy,
		FreshFor:     match.freshFor,
		ExpireAfter:  match.expireAfter,
	}, nil
}

type fileMatch struct {
	policy      string
	freshFor    config.Freshness
	expireAfter config.Expiration
}

func (r *Resolver) match(cleanPath string) fileMatch {
	if r.policy == nil {
		return fileMatch{policy: config.PolicyBypass}
	}
	for _, rule := range r.policy.Rules {
		if doublestar.MatchUnvalidated(rule.Match, cleanPath) {
			return fileMatch{
				policy:      rule.Policy,
				freshFor:    rule.FreshFor,
				expireAfter: rule.ExpireAfter,
			}
		}
	}
	policy := r.policy.DefaultPolicy
	if policy == "" {
		policy = config.PolicyBypass
	}
	return fileMatch{policy: policy}
}
