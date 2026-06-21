package filerepo

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type ResourceClass string

const (
	ResourceAny       ResourceClass = "*"
	ResourceMetadata  ResourceClass = "metadata"
	ResourceArtifact  ResourceClass = "artifact"
	ResourceAuxiliary ResourceClass = "auxiliary"
)

type Rule struct {
	Match         string
	ResourceClass ResourceClass
	Policy        string
	FreshFor      config.Freshness
	BusyPolicy    string
	ExpireAfter   config.Expiration
}

type CacheProfile struct {
	Policy      string
	FreshFor    config.Freshness
	BusyPolicy  string
	ExpireAfter config.Expiration
}

type Defaults struct {
	Metadata  CacheProfile
	Artifact  CacheProfile
	Auxiliary CacheProfile
}

type Config struct {
	ObjectRoot string
	EmptyPath  string
	Defaults   Defaults
	Rules      []Rule
	Classifier func(string) ResourceClass
}

type Resolver struct {
	cfg Config
}

func NewResolver(cfg Config) *Resolver {
	return &Resolver{cfg: cfg}
}

func (r *Resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "." || cleanPath == "" {
		cleanPath = r.cfg.EmptyPath
	}
	if cleanPath == "" {
		return httpcache.Route{}, errors.New("path is required")
	}
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid request path")
	}

	class := ResourceAuxiliary
	if r.cfg.Classifier != nil {
		class = r.cfg.Classifier(cleanPath)
	}
	profile := r.profileFor(class)
	route := httpcache.Route{
		ObjectPath:   r.cfg.ObjectRoot + "/" + cleanPath,
		UpstreamPath: cleanPath,
		Policy:       profile.Policy,
		FreshFor:     profile.FreshFor,
		BusyPolicy:   profile.BusyPolicy,
		ExpireAfter:  profile.ExpireAfter,
	}
	for _, rule := range r.cfg.Rules {
		if !matchRule(rule, cleanPath, class) {
			continue
		}
		if rule.Policy != "" {
			route.Policy = rule.Policy
		}
		route.FreshFor = rule.FreshFor
		if rule.BusyPolicy != "" {
			route.BusyPolicy = rule.BusyPolicy
		}
		route.ExpireAfter = rule.ExpireAfter
	}
	return route, nil
}

func (r *Resolver) profileFor(class ResourceClass) CacheProfile {
	switch class {
	case ResourceMetadata:
		return r.cfg.Defaults.Metadata
	case ResourceArtifact:
		return r.cfg.Defaults.Artifact
	default:
		return r.cfg.Defaults.Auxiliary
	}
}

func matchRule(rule Rule, cleanPath string, class ResourceClass) bool {
	if rule.ResourceClass != "" && rule.ResourceClass != ResourceAny && rule.ResourceClass != class {
		return false
	}
	return doublestar.MatchUnvalidated(rule.Match, cleanPath)
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

func ValidateRules(mode string, rules []Rule) error {
	for i, rule := range rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("%s rule %d: match is empty", mode, i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("%s rule %d: invalid match %q", mode, i, rule.Match)
		}
		if rule.Policy != "" {
			if err := ValidatePolicy(mode, rule.Policy); err != nil {
				return err
			}
		}
		if err := ValidateBusyPolicy(mode, rule.BusyPolicy); err != nil {
			return err
		}
		switch rule.ResourceClass {
		case "", ResourceAny, ResourceMetadata, ResourceArtifact, ResourceAuxiliary:
		default:
			return fmt.Errorf("%s rule %d: invalid resource class %q", mode, i, rule.ResourceClass)
		}
	}
	return nil
}
