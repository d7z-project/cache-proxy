package oci

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	containername "github.com/google/go-containerregistry/pkg/name"

	"gopkg.d7z.net/cache-proxy/pkg/config"
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
	ExpireAfter config.Expiration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type AuthConfig struct {
	Type     string `json:"type" yaml:"type"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Token    string `json:"token,omitempty" yaml:"token,omitempty"`
}

type Block struct {
	ExpireAfter config.Expiration       `yaml:"expire_after"`
	Bind        string                  `yaml:"bind"`
	DisplayURL  string                  `yaml:"display_url,omitempty"`
	Upstream    string                  `yaml:"upstream"`
	Transport   *config.TransportConfig `yaml:"transport,omitempty"`
	Policy      `yaml:",inline"`
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
	expireAfter := config.DefaultExpireAfter
	if !block.ExpireAfter.IsUnset() {
		expireAfter = block.ExpireAfter
	}
	handler := newHandler(plan.Name(), block, expireAfter, plan.Store(), plan.Stats(), plan.Downloads())
	plan.SetHomeSnippet(plan.RenderSnippet())
	if block.DisplayURL != "" {
		plan.SetHomeDisplayURL(block.DisplayURL)
	}
	return plan.BindAddr(block.Bind, expireAfter, handler)
}

func validate(upstream string, policy *Policy) error {
	host := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(upstream, "https://"), "http://"))
	if host != "" {
		host = strings.Split(host, "/")[0]
		if _, err := containername.NewRegistry(host); err != nil {
			return fmt.Errorf("invalid oci registry %q: %w", host, err)
		}
	}
	if !config.ValidPolicy(policy.DefaultPolicy) {
		return fmt.Errorf("invalid oci default policy %q", policy.DefaultPolicy)
	}
	if !config.ValidBusyPolicy(policy.BusyPolicy) {
		return fmt.Errorf("invalid oci busy policy %q", policy.BusyPolicy)
	}
	if policy.FreshFor > 0 && policy.FreshFor.Duration() < time.Second {
		return fmt.Errorf("oci fresh_for must be at least 1s")
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
		if !config.ValidPolicy(rule.Policy) {
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
