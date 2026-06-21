package oci

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	containername "github.com/google/go-containerregistry/pkg/name"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
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
	FreshFor    config.Freshness  `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter config.Expiration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type AuthConfig struct {
	Type     string `json:"type" yaml:"type"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Token    string `json:"token,omitempty" yaml:"token,omitempty"`
}

type Block struct {
	Bind      string                  `yaml:"bind"`
	Upstream  string                  `yaml:"upstream"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Policy    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeOCI }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if block.Upstream == "" {
		return fmt.Errorf("instance %s: oci mode requires one upstream", plan.Name())
	}
	if block.DefaultPolicy == "" {
		block.DefaultPolicy = config.PolicyBypass
	}
	if block.BusyPolicy == "" {
		block.BusyPolicy = config.BusyPolicyBypass
	}
	if err := validate(block.Upstream, &block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	var auth *httpcache.OCIAuthConfig
	if block.Auth != nil {
		auth = &httpcache.OCIAuthConfig{
			Type:     block.Auth.Type,
			Username: block.Auth.Username,
			Password: block.Auth.Password,
			Token:    block.Auth.Token,
		}
	}
	expireAfter := config.DefaultExpireAfter
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:            config.ModeOCI,
		ExpireAfter:     expireAfter,
		Upstreams:       []string{block.Upstream},
		Transport:       block.Transport,
		BusyPolicy:      block.BusyPolicy,
		DefaultFreshFor: block.FreshFor,
		OCIAuth:         auth,
	}, plan.Store(), New(&block.Policy), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindAddr(block.Bind, expireAfter, proxyruntime.HandlerInstance{
		Handler: handler,
		Close:   func() error { handler.Close(); return nil },
	})
}

func validate(upstream string, policy *Policy) error {
	host := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(upstream, "https://"), "http://"))
	if host != "" {
		host = strings.Split(host, "/")[0]
		if _, err := containername.NewRegistry(host); err != nil {
			return fmt.Errorf("invalid oci registry %q: %w", host, err)
		}
	}
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

func validPolicy(policy string) bool {
	return policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate
}
