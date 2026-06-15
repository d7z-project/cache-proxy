package oci

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	containername "github.com/google/go-containerregistry/pkg/name"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy struct {
	Auth          *AuthConfig      `json:"auth,omitempty" yaml:"auth,omitempty"`
	DefaultPolicy string           `json:"defaultPolicy,omitempty" yaml:"default_policy,omitempty"`
	FreshFor      config.Freshness `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	BusyPolicy    string           `json:"busyPolicy,omitempty" yaml:"busy_policy,omitempty"`
	Rules         []Rule           `json:"rules" yaml:"rules"`
}

type Rule struct {
	Match       string            `json:"match" yaml:"match"`
	Policy      string            `json:"policy,omitempty" yaml:"policy,omitempty"`
	FreshFor    config.Freshness `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter config.Expiration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type AuthConfig struct {
	Type     string `json:"type" yaml:"type"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Token    string `json:"token,omitempty" yaml:"token,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModeOCI }

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
	if spec.Route.Bind == "" {
		return errors.New("oci mode requires bind route")
	}
	if len(spec.Source.Upstreams) != 1 {
		return errors.New("oci mode requires exactly one upstream")
	}
	for _, rawURL := range spec.Source.Upstreams {
		if host := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(rawURL, "https://"), "http://")); host != "" {
			host = strings.Split(host, "/")[0]
			if _, err := containername.NewRegistry(host); err != nil {
				return fmt.Errorf("invalid oci registry %q: %w", host, err)
			}
		}
	}
	policy := spec.Policy.(*Policy)
	if !validPolicy(policy.DefaultPolicy) {
		return fmt.Errorf("invalid oci default policy %q", policy.DefaultPolicy)
	}
	if policy.BusyPolicy != config.BusyPolicyBypass && policy.BusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid oci busy policy %q", policy.BusyPolicy)
	}
	for i, rule := range policy.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("oci rule %d: match is empty", i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("oci rule %d: invalid match %q", i, rule.Match)
		}
		if rule.Policy == "" {
			rule.Policy = config.PolicyBypass
		}
		if !validPolicy(rule.Policy) {
			return fmt.Errorf("oci rule %d: invalid policy %q", i, rule.Policy)
		}
		policy.Rules[i] = rule
	}
	if policy.Auth == nil {
		return nil
	}
	switch strings.ToLower(policy.Auth.Type) {
	case "", "none":
		policy.Auth = nil
	case "basic":
		if policy.Auth.Username == "" || policy.Auth.Password == "" {
			return errors.New("oci basic auth requires username and password")
		}
	case "bearer":
		if policy.Auth.Token == "" {
			return errors.New("oci bearer auth requires token")
		}
	default:
		return fmt.Errorf("unsupported oci auth type %q", policy.Auth.Type)
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness {
	return spec.Policy.(*Policy).FreshFor
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	policy := spec.Policy.(*Policy)
	var auth *proxy.OCIAuthConfig
	if policy.Auth != nil {
		auth = &proxy.OCIAuthConfig{
			Type:     policy.Auth.Type,
			Username: policy.Auth.Username,
			Password: policy.Auth.Password,
			Token:    policy.Auth.Token,
		}
	}
	handler := proxy.NewHandler(name, proxy.RuntimeConfig{
		Mode:            config.ModeOCI,
		ExpireAfter:     spec.Meta.ExpireAfter,
		Upstreams:       append([]string(nil), spec.Source.Upstreams...),
		Transport:       spec.Source.Transport,
		BusyPolicy:      policy.BusyPolicy,
		DefaultFreshFor: policy.FreshFor,
		OCIAuth:         auth,
	}, store, New(policy), stats)
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return LookupRef(spec.Policy.(*Policy), lookupPath)
}

func validPolicy(policy string) bool {
	return policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate
}

func init() {
	proxydriver.Default.Register(Driver{})
}
