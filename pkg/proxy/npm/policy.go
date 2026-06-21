package npm

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Policy struct {
	DefaultPolicy string           `json:"defaultPolicy,omitempty" yaml:"default_policy,omitempty"`
	FreshFor      config.Freshness `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	BusyPolicy    string           `json:"busyPolicy,omitempty" yaml:"busy_policy,omitempty"`
	Rules         []Rule           `json:"rules" yaml:"rules"`
}

type Rule struct {
	Match          string            `json:"match" yaml:"match"`
	ResourcePolicy string            `json:"resourcePolicy,omitempty" yaml:"resource_policy,omitempty"`
	Policy         string            `json:"policy,omitempty" yaml:"policy,omitempty"`
	FreshFor       config.Freshness  `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter    config.Expiration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type Block struct {
	ExpireAfter config.Expiration `yaml:"expire_after"`
	Route       struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
	Upstreams []string                `yaml:"upstreams"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Policy    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeNPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if len(block.Upstreams) != 1 {
		return fmt.Errorf("instance %s: npm mode requires exactly one upstream", plan.Name())
	}
	if block.DefaultPolicy == "" {
		block.DefaultPolicy = config.PolicyBypass
	}
	if block.BusyPolicy == "" {
		block.BusyPolicy = config.BusyPolicyBypass
	}
	if err := validate(&block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:            config.ModeNPM,
		ExpireAfter:     expireAfter,
		Upstreams:       append([]string(nil), block.Upstreams...),
		Transport:       block.Transport,
		BusyPolicy:      block.BusyPolicy,
		DefaultFreshFor: block.FreshFor,
	}, plan.Store(), New(&block.Policy), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler: handler,
		Close:   func() error { handler.Close(); return nil },
	})
}

func validate(policy *Policy) error {
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

func validPolicy(policy string) bool {
	return policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate
}
