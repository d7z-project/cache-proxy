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
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const defaultCleanupInterval = 6 * time.Hour

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
	if _, err := url.Parse(block.Upstream); err != nil {
		return fmt.Errorf("instance %s: cargo upstream URL is invalid: %w", plan.Name(), err)
	}
	if err := validatePolicy(plan.Name(), &block.Policy); err != nil {
		return err
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	runtime := httpcache.RuntimeConfig{
		Mode:            config.ModeCargo,
		ExpireAfter:     expireAfter,
		Upstreams:       []string{strings.TrimSpace(block.Upstream)},
		Transport:       block.Transport,
		BusyPolicy:      block.IndexBusyPolicy,
		DefaultFreshFor: block.IndexFreshFor,
		DownloadLimiter: plan.Downloads(),
	}
	h := newHandler(plan.Name(), runtime, plan.Store(), newResolver(&block.Policy, plan.Store(), plan.Name()), plan.Stats())
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: defaultCleanupInterval,
		Handler: func(ctx context.Context) error {
			return h.Cleanup(ctx, plan.CleanupConfig())
		},
	})
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:      h,
		Close:        func() error { h.base.Close(); return nil },
		CloseContext: h.Stop,
		CleanupFn:    h.Cleanup,
	})
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
	if policy.IndexFreshFor > 0 && policy.IndexFreshFor.Duration() < time.Second {
		return fmt.Errorf("instance %s: cargo index fresh_for must be at least 1s", instance)
	}
	return nil
}
