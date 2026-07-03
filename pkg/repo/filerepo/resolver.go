package filerepo

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type ResourceClass string

const (
	ResourceMetadata  ResourceClass = "metadata"
	ResourceArtifact  ResourceClass = "artifact"
	ResourceAuxiliary ResourceClass = "auxiliary"
	ResourceUnknown   ResourceClass = "unknown"
)

type CacheProfile struct {
	Policy      string
	FreshFor    config.Freshness
	BusyPolicy  string
	ExpireAfter config.Expiration
}

type generationResolver struct {
	handler *IndexedHandler
	policy  *Policy
}

func (r *generationResolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "." || cleanPath == "" {
		return httpcache.Route{}, errors.New("path is required")
	}
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid request path")
	}

	class := r.handler.inspect(cleanPath).Class
	profile := r.profileFor(class)
	route := httpcache.Route{
		ObjectPath:   path.Join(r.handler.objectRoot, cleanPath),
		UpstreamPath: cleanPath,
		Policy:       profile.Policy,
		FreshFor:     profile.FreshFor,
		BusyPolicy:   profile.BusyPolicy,
		ExpireAfter:  profile.ExpireAfter,
	}
	if class == ResourceArtifact || class == ResourceAuxiliary {
		if snap := r.handler.currentSnapshot(); snap != nil && snap.Upstream != "" {
			route.PreferredUpstream = snap.Upstream
		}
	}
	return route, nil
}

func (r *generationResolver) profileFor(class ResourceClass) CacheProfile {
	switch class {
	case ResourceArtifact:
		return CacheProfile{
			Policy:      r.policy.ArtifactPolicy,
			FreshFor:    r.policy.ArtifactFreshFor,
			BusyPolicy:  r.policy.ArtifactBusyPolicy,
			ExpireAfter: r.policy.ArtifactExpireAfter,
		}
	default:
		return CacheProfile{
			Policy:      r.policy.AuxiliaryPolicy,
			FreshFor:    r.policy.AuxiliaryFreshFor,
			BusyPolicy:  r.policy.AuxiliaryBusyPolicy,
			ExpireAfter: r.policy.AuxiliaryExpireAfter,
		}
	}
}

func ValidatePassHeaders(headers []string) error {
	for _, header := range headers {
		name := strings.TrimSpace(header)
		if name == "" || strings.ContainsAny(name, " \t\r\n:") {
			return fmt.Errorf("invalid pass header %q", header)
		}
	}
	return nil
}

func ValidatePolicy(mode, policy string) error {
	if policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate {
		return nil
	}
	return fmt.Errorf("invalid %s policy %q", mode, policy)
}

func ValidateBusyPolicy(mode, policy string) error {
	if policy == "" || policy == config.BusyPolicyBypass || policy == config.BusyPolicyStale {
		return nil
	}
	return fmt.Errorf("invalid %s busy policy %q", mode, policy)
}
