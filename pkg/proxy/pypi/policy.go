package pypi

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Policy struct {
	IndexPolicy         string           `json:"indexPolicy,omitempty" yaml:"index_policy,omitempty"`
	IndexFreshFor       config.Freshness `json:"indexFreshFor,omitempty" yaml:"index_fresh_for,omitempty"`
	IndexBusyPolicy     string           `json:"indexBusyPolicy,omitempty" yaml:"index_busy_policy,omitempty"`
	FilePolicy          string           `json:"filePolicy,omitempty" yaml:"file_policy,omitempty"`
	CompanionPolicy     string           `json:"companionPolicy,omitempty" yaml:"companion_policy,omitempty"`
	CompanionFreshFor   config.Freshness `json:"companionFreshFor,omitempty" yaml:"companion_fresh_for,omitempty"`
	CompanionBusyPolicy string           `json:"companionBusyPolicy,omitempty" yaml:"companion_busy_policy,omitempty"`
	ProxyJSON           *bool            `json:"proxyJson,omitempty" yaml:"proxy_json,omitempty"`
	ProxyCoreMetadata   bool             `json:"proxyCoreMetadata,omitempty" yaml:"proxy_core_metadata,omitempty"`
	ProxySignatures     bool             `json:"proxySignatures,omitempty" yaml:"proxy_signatures,omitempty"`
}

type Block struct {
	ExpireAfter config.Expiration `yaml:"expire_after"`
	Route       struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
	Upstream  string                  `yaml:"upstream"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Policy    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModePyPI }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if strings.TrimSpace(block.Upstream) == "" {
		return fmt.Errorf("instance %s: pypi mode requires one upstream", plan.Name())
	}
	applyDefaults(&block.Policy)
	if err := validate(&block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:            config.ModePyPI,
		ExpireAfter:     expireAfter,
		Upstreams:       []string{strings.TrimSpace(block.Upstream)},
		Transport:       block.Transport,
		BusyPolicy:      block.CompanionBusyPolicy,
		DefaultFreshFor: block.CompanionFreshFor,
	}, plan.Store(), &resolver{policy: &block.Policy}, plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:   handler,
		Close:     func() error { handler.Close(); return nil },
		CleanupFn: handler.Cleanup,
	})
}

func applyDefaults(policy *Policy) {
	if policy.IndexPolicy == "" {
		policy.IndexPolicy = config.PolicyRevalidate
	}
	if policy.IndexFreshFor == 0 {
		policy.IndexFreshFor = config.Freshness(time.Minute)
	}
	if policy.IndexBusyPolicy == "" {
		policy.IndexBusyPolicy = config.BusyPolicyStale
	}
	if policy.FilePolicy == "" {
		policy.FilePolicy = config.PolicyImmutable
	}
	if policy.CompanionPolicy == "" {
		policy.CompanionPolicy = config.PolicyRevalidate
	}
	if policy.CompanionFreshFor == 0 {
		policy.CompanionFreshFor = config.Freshness(30 * time.Second)
	}
	if policy.CompanionBusyPolicy == "" {
		policy.CompanionBusyPolicy = config.BusyPolicyStale
	}
	if policy.ProxyJSON == nil {
		enabled := true
		policy.ProxyJSON = &enabled
	}
}

func validate(policy *Policy) error {
	for _, value := range []string{policy.IndexPolicy, policy.FilePolicy, policy.CompanionPolicy} {
		if value != config.PolicyBypass && value != config.PolicyImmutable && value != config.PolicyRevalidate {
			return fmt.Errorf("invalid pypi policy %q", value)
		}
	}
	for _, value := range []string{policy.IndexBusyPolicy, policy.CompanionBusyPolicy} {
		if value != config.BusyPolicyBypass && value != config.BusyPolicyStale {
			return fmt.Errorf("invalid pypi busy policy %q", value)
		}
	}
	return nil
}

type resolver struct {
	policy *Policy
}

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	return routeForPath(r.policy, strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/"))
}

func routeForPath(policy *Policy, lookupPath string) (httpcache.Route, error) {
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "simple/"
	}
	switch {
	case lookupPath == "simple" || lookupPath == "simple/":
		return httpcache.Route{
			ObjectPath:   "pypi/simple/root.html",
			UpstreamPath: "simple/",
			Policy:       policy.IndexPolicy,
			FreshFor:     policy.IndexFreshFor,
			BusyPolicy:   policy.IndexBusyPolicy,
			RewriteKind:  "pypi-simple",
		}, nil
	case strings.HasPrefix(lookupPath, "simple/"):
		trimmed := strings.TrimPrefix(lookupPath, "simple/")
		if strings.HasSuffix(trimmed, "/json") {
			name := normalizeProjectName(strings.TrimSuffix(trimmed, "/json"))
			if !proxyJSONEnabled(policy) {
				return httpcache.Route{}, errors.New("json simple api is disabled")
			}
			return httpcache.Route{
				ObjectPath:     "pypi/simple/" + name + ".json",
				UpstreamPath:   "simple/" + name + "/",
				Policy:         policy.IndexPolicy,
				FreshFor:       policy.IndexFreshFor,
				BusyPolicy:     policy.IndexBusyPolicy,
				RequestHeaders: map[string]string{"Accept": "application/vnd.pypi.simple.v1+json"},
				RewriteKind:    "pypi-simple",
			}, nil
		}
		name := normalizeProjectName(strings.TrimSuffix(trimmed, "/"))
		return httpcache.Route{
			ObjectPath:   "pypi/simple/" + name + ".html",
			UpstreamPath: "simple/" + name + "/",
			Policy:       policy.IndexPolicy,
			FreshFor:     policy.IndexFreshFor,
			BusyPolicy:   policy.IndexBusyPolicy,
			RewriteKind:  "pypi-simple",
		}, nil
	case strings.HasPrefix(lookupPath, "files/"):
		sourceURL, err := decodeSourceURL(path.Base(lookupPath))
		if err != nil {
			return httpcache.Route{}, err
		}
		return fileRoute(policy, lookupPath, sourceURL), nil
	default:
		targetURL := strings.TrimPrefix(lookupPath, "/")
		if _, err := url.Parse(targetURL); err == nil && strings.Contains(targetURL, "://") {
			return fileRoute(policy, lookupPath, targetURL), nil
		}
		return fileRoute(policy, lookupPath, targetURL), nil
	}
}

func fileRoute(policy *Policy, lookupPath, rawURL string) httpcache.Route {
	objectPath := "pypi/files/" + path.Base(lookupPath)
	if !strings.HasPrefix(lookupPath, "files/") {
		objectPath = "pypi/files/" + encodeSourceURL(rawURL)
	}
	route := httpcache.Route{
		ObjectPath: objectPath,
		Policy:     policy.FilePolicy,
	}
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		route.TargetURL = rawURL
	} else {
		route.UpstreamPath = lookupPath
	}
	if isAuxiliaryPath(rawURL, policy) {
		route.Policy = policy.CompanionPolicy
		route.FreshFor = policy.CompanionFreshFor
		route.BusyPolicy = policy.CompanionBusyPolicy
	}
	return route
}

func isAuxiliaryPath(rawURL string, policy *Policy) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	cleanPath := strings.ToLower(parsed.Path)
	if policy.ProxySignatures {
		for _, suffix := range []string{".asc", ".sig", ".minisig"} {
			if strings.HasSuffix(cleanPath, suffix) {
				return true
			}
		}
	}
	if policy.ProxyCoreMetadata {
		for _, suffix := range []string{".metadata", ".json", ".attestation", ".provenance"} {
			if strings.HasSuffix(cleanPath, suffix) {
				return true
			}
		}
	}
	return false
}

func proxyJSONEnabled(policy *Policy) bool {
	return policy != nil && policy.ProxyJSON != nil && *policy.ProxyJSON
}

func normalizeProjectName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	return strings.ReplaceAll(name, ".", "-")
}

func encodeSourceURL(raw string) string {
	return hex.EncodeToString([]byte(raw))
}

func decodeSourceURL(value string) (string, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
