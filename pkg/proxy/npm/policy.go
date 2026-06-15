package npm

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy struct {
	DefaultPolicy string             `json:"defaultPolicy,omitempty" yaml:"default_policy,omitempty"`
	FreshFor      config.Freshness   `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	BusyPolicy    string             `json:"busyPolicy,omitempty" yaml:"busy_policy,omitempty"`
	Rules         []Rule             `json:"rules" yaml:"rules"`
}

type Rule struct {
	Match          string             `json:"match" yaml:"match"`
	ResourcePolicy string             `json:"resourcePolicy,omitempty" yaml:"resource_policy,omitempty"`
	Policy         string             `json:"policy,omitempty" yaml:"policy,omitempty"`
	FreshFor       config.Freshness   `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter    config.Expiration  `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModeNPM }

func (Driver) DecodeJSON(data json.RawMessage) (any, error) {
	policy := &Policy{}
	if len(data) == 0 || string(data) == "null" {
		return policy, nil
	}
	if err := json.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) {
	data, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func (Driver) DecodeYAML(data []byte) (any, error) {
	policy := &Policy{}
	if len(data) == 0 {
		return policy, nil
	}
	if err := yaml.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (Driver) EncodeYAML(policy any) ([]byte, error) { return yaml.Marshal(policy) }

func (Driver) ApplyDefaults(spec *proxydriver.ResolvedSpec) {
	policy := spec.Policy.(*Policy)
	if policy.DefaultPolicy == "" {
		policy.DefaultPolicy = config.PolicyBypass
	}
	if policy.BusyPolicy == "" {
		policy.BusyPolicy = config.BusyPolicyBypass
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) != 1 {
		return errors.New("npm mode requires exactly one upstream")
	}
	policy := spec.Policy.(*Policy)
	if !validPolicy(policy.DefaultPolicy) {
		return fmt.Errorf("invalid npm default policy %q", policy.DefaultPolicy)
	}
	if policy.BusyPolicy != config.BusyPolicyBypass && policy.BusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid npm busy policy %q", policy.BusyPolicy)
	}
	for i, rule := range policy.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("npm rule %d: match is empty", i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("npm rule %d: invalid match %q", i, rule.Match)
		}
		if rule.Policy == "" {
			rule.Policy = config.PolicyBypass
		}
		if !validPolicy(rule.Policy) {
			return fmt.Errorf("npm rule %d: invalid policy %q", i, rule.Policy)
		}
		if rule.ResourcePolicy == "" {
			rule.ResourcePolicy = "*"
		}
		if rule.ResourcePolicy != "*" && rule.ResourcePolicy != "metadata" && rule.ResourcePolicy != "tarball" {
			return fmt.Errorf("npm rule %d: invalid resource_policy %q", i, rule.ResourcePolicy)
		}
		policy.Rules[i] = rule
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness {
	return spec.Policy.(*Policy).FreshFor
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	policy := spec.Policy.(*Policy)
	handler := proxy.NewHandler(name, proxy.RuntimeConfig{
		Mode:            config.ModeNPM,
		ExpireAfter:     spec.Meta.ExpireAfter,
		Upstreams:       append([]string(nil), spec.Source.Upstreams...),
		Transport:       spec.Source.Transport,
		BusyPolicy:      policy.BusyPolicy,
		DefaultFreshFor: policy.FreshFor,
	}, store, New(policy), stats)
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	req, _ := http.NewRequest(http.MethodGet, "/"+strings.TrimPrefix(lookupPath, "/"), nil)
	return New(spec.Policy.(*Policy)).Resolve(req)
}

func validPolicy(policy string) bool {
	return policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate
}

func init() {
	proxydriver.Default.Register(Driver{})
}
