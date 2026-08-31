package cargo

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Policy struct {
	IndexFreshFor     config.Freshness `json:"indexFreshFor,omitempty" yaml:"index_fresh_for,omitempty"`
	IndexBusyPolicy   string           `json:"indexBusyPolicy,omitempty" yaml:"index_busy_policy,omitempty"`
	CratePolicy       string           `json:"cratePolicy,omitempty" yaml:"crate_policy,omitempty"`
	AuthRequired      bool             `json:"authRequired,omitempty" yaml:"auth_required,omitempty"`
	AllowedCrateHosts []string         `json:"allowedCrateHosts,omitempty" yaml:"allowed_crate_hosts,omitempty"`
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

func (Driver) Mode() string { return config.ModeCargo }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	applyDefaults(&block.Policy)
	if strings.TrimSpace(block.Upstream) == "" {
		return fmt.Errorf("instance %s: cargo mode requires one upstream", plan.Name())
	}
	if err := config.ValidateHTTPUpstream(block.Upstream); err != nil {
		return fmt.Errorf("instance %s: cargo upstream URL is invalid: %w", plan.Name(), err)
	}
	if err := config.ValidateTransport(block.Transport); err != nil {
		return fmt.Errorf("instance %s: cargo transport: %w", plan.Name(), err)
	}
	if err := validatePolicy(plan.Name(), &block.Policy); err != nil {
		return err
	}
	if !plan.Enabled() {
		return nil
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	runtime := httpcache.RuntimeConfig{
		Mode:               config.ModeCargo,
		ExpireAfter:        expireAfter,
		Upstreams:          []string{strings.TrimSpace(block.Upstream)},
		Transport:          block.Transport,
		BusyPolicy:         block.IndexBusyPolicy,
		DefaultFreshFor:    block.IndexFreshFor,
		UpstreamGate:       plan.UpstreamGate(),
		AllowedTargetHosts: append([]string(nil), block.AllowedCrateHosts...),
	}
	h := httpcache.NewHandler(
		plan.Name(),
		runtime,
		plan.Store(),
		newResolver(&block.Policy),
		plan.Stats(),
		nil,
	)
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindHTTPPath(block.Route.Path, expireAfter, h)
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
	if !config.ValidBusyPolicy(policy.IndexBusyPolicy) {
		return fmt.Errorf("instance %s: invalid cargo index busy policy %q", instance, policy.IndexBusyPolicy)
	}
	if policy.CratePolicy != config.PolicyBypass && policy.CratePolicy != config.PolicyImmutable && policy.CratePolicy != config.PolicyRevalidate {
		return fmt.Errorf("instance %s: invalid cargo crate policy %q", instance, policy.CratePolicy)
	}
	if policy.IndexFreshFor > 0 && policy.IndexFreshFor.Duration() < time.Second {
		return fmt.Errorf("instance %s: cargo index fresh_for must be at least 1s", instance)
	}
	seenHosts := make(map[string]struct{}, len(policy.AllowedCrateHosts))
	for index, host := range policy.AllowedCrateHosts {
		host = strings.TrimSpace(host)
		parsed, err := url.Parse("//" + host)
		if err != nil || host == "" || parsed.Host != host || parsed.Hostname() == "" || parsed.User != nil ||
			parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("instance %s: invalid cargo allowed_crate_hosts entry %q", instance, host)
		}
		host = strings.ToLower(parsed.Host)
		if _, exists := seenHosts[host]; exists {
			return fmt.Errorf("instance %s: duplicate cargo allowed_crate_hosts entry %q", instance, host)
		}
		seenHosts[host] = struct{}{}
		policy.AllowedCrateHosts[index] = host
	}
	return nil
}
