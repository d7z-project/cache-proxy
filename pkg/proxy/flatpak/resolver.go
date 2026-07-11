package flatpak

import (
	"errors"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type resolver struct {
	policy *Policy
}

func (r resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "." || cleanPath == "" {
		cleanPath = "summary"
	}
	return routeForPath(r.policy, cleanPath)
}

func routeForPath(policy *Policy, cleanPath string) (httpcache.Route, error) {
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid flatpak request path")
	}
	switch {
	case isMetadataPath(cleanPath):
		return httpcache.Route{
			ObjectPath:   "flatpak/metadata-cache/" + cleanPath,
			UpstreamPath: cleanPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     policy.MetadataFreshFor,
			BusyPolicy:   policy.MetadataBusyPolicy,
		}, nil
	case isObjectPath(cleanPath):
		return httpcache.Route{
			ObjectPath:   "flatpak/" + cleanPath,
			UpstreamPath: cleanPath,
			Policy:       config.PolicyImmutable,
		}, nil
	case isDeltaPath(cleanPath):
		if policy.CacheDeltas == nil || !*policy.CacheDeltas {
			return httpcache.Route{
				ObjectPath:   "flatpak/" + cleanPath,
				UpstreamPath: cleanPath,
				Policy:       config.PolicyBypass,
			}, nil
		}
		return httpcache.Route{
			ObjectPath:   "flatpak/" + cleanPath,
			UpstreamPath: cleanPath,
			Policy:       config.PolicyImmutable,
			ExpireAfter:  policy.DeltaExpireAfter,
		}, nil
	case isDescriptorPath(cleanPath):
		route := httpcache.Route{
			ObjectPath:   "flatpak/descriptors/" + httpcache.HashKey(cleanPath) + path.Ext(cleanPath),
			UpstreamPath: cleanPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     config.Freshness(defaultDescriptorFreshFor),
			BusyPolicy:   config.BusyPolicyStale,
		}
		return route, nil
	case strings.HasPrefix(cleanPath, "refs/heads/"):
		return httpcache.Route{
			ObjectPath:   "flatpak/" + cleanPath,
			UpstreamPath: cleanPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     policy.MetadataFreshFor,
			BusyPolicy:   policy.MetadataBusyPolicy,
		}, nil
	default:
		return httpcache.Route{
			ObjectPath:   "flatpak/auxiliary/" + httpcache.HashKey(cleanPath),
			UpstreamPath: cleanPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     config.Freshness(defaultDescriptorFreshFor),
			BusyPolicy:   config.BusyPolicyBypass,
		}, nil
	}
}
