package cargo

import (
	"context"
	"fmt"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Policy struct {
	IndexFreshFor   config.Freshness `json:"indexFreshFor,omitempty" yaml:"index_fresh_for,omitempty"`
	IndexBusyPolicy string           `json:"indexBusyPolicy,omitempty" yaml:"index_busy_policy,omitempty"`
	CratePolicy     string           `json:"cratePolicy,omitempty" yaml:"crate_policy,omitempty"`
	AuthRequired    bool             `json:"authRequired,omitempty" yaml:"auth_required,omitempty"`
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

func (Driver) Mode() string { return config.ModeCargo }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	applyDefaults(&block.Policy)
	if len(block.Upstreams) != 1 {
		return fmt.Errorf("instance %s: cargo mode requires exactly one upstream", plan.Name())
	}
	if err := validatePolicy(plan.Name(), &block.Policy); err != nil {
		return err
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler, err := newHandler(plan.Name(), block.Upstreams[0], block.Transport, &block.Policy, plan.Store(), plan.Stats())
	if err != nil {
		return err
	}
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}

func applyDefaults(policy *Policy) {
	if policy.IndexBusyPolicy == "" {
		policy.IndexBusyPolicy = config.BusyPolicyStale
	}
	if policy.CratePolicy == "" {
		policy.CratePolicy = config.PolicyImmutable
	}
}

func validatePolicy(instance string, policy *Policy) error {
	if policy.IndexBusyPolicy != config.BusyPolicyBypass && policy.IndexBusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("instance %s: invalid cargo index busy policy %q", instance, policy.IndexBusyPolicy)
	}
	if policy.CratePolicy != config.PolicyBypass && policy.CratePolicy != config.PolicyImmutable && policy.CratePolicy != config.PolicyRevalidate {
		return fmt.Errorf("instance %s: invalid cargo crate policy %q", instance, policy.CratePolicy)
	}
	return nil
}
