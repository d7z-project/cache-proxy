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
	cache config.CacheConfig
}

func New(cache config.CacheConfig) *Resolver {
	return &Resolver{cache: cache}
}

func (r *Resolver) Resolve(req *http.Request) (proxy.Route, error) {
	cleanPath := strings.TrimLeft(req.URL.Path, "/")
	if cleanPath == "" {
		cleanPath = "index"
	}
	if !proxy.SafePath(cleanPath) {
		return proxy.Route{}, errors.New("invalid request path")
	}
	return proxy.Route{ObjectPath: "file/" + cleanPath, UpstreamPath: cleanPath, Policy: r.policy(cleanPath)}, nil
}

func (r *Resolver) policy(cleanPath string) string {
	for _, rule := range r.cache.Rules {
		if doublestar.MatchUnvalidated(rule.Match, cleanPath) {
			return rule.Policy
		}
	}
	if r.cache.DefaultPolicy != "" {
		return r.cache.DefaultPolicy
	}
	return config.PolicyBypass
}
