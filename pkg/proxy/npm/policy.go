package npm

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
	MetadataPolicy     string           `json:"metadataPolicy,omitempty" yaml:"metadata_policy,omitempty"`
	MetadataFreshFor   config.Freshness `json:"metadataFreshFor,omitempty" yaml:"metadata_fresh_for,omitempty"`
	MetadataBusyPolicy string           `json:"metadataBusyPolicy,omitempty" yaml:"metadata_busy_policy,omitempty"`
	TarballPolicy      string           `json:"tarballPolicy,omitempty" yaml:"tarball_policy,omitempty"`
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
func (Driver) Mode() string              { return config.ModeNPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if strings.TrimSpace(block.Upstream) == "" {
		return fmt.Errorf("instance %s: npm mode requires one upstream", plan.Name())
	}
	if _, err := url.Parse(block.Upstream); err != nil {
		return fmt.Errorf("instance %s: npm upstream URL is invalid: %w", plan.Name(), err)
	}
	if block.MetadataPolicy == "" {
		block.MetadataPolicy = config.PolicyRevalidate
	}
	if block.MetadataBusyPolicy == "" {
		block.MetadataBusyPolicy = config.BusyPolicyStale
	}
	if block.TarballPolicy == "" {
		block.TarballPolicy = config.PolicyImmutable
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
		Upstreams:       []string{strings.TrimSpace(block.Upstream)},
		Transport:       block.Transport,
		BusyPolicy:      block.MetadataBusyPolicy,
		DefaultFreshFor: block.MetadataFreshFor,
		DownloadLimiter: plan.Downloads(),
	}, plan.Store(), New(&block.Policy), plan.Stats(), nil)
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: defaultCleanupInterval,
		Handler: func(ctx context.Context) (scheduler.TaskOutcome, error) {
			return scheduler.TaskOutcome{}, handler.Cleanup(ctx, plan.CleanupConfig())
		},
	})
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:      handler,
		Close:        func() error { handler.Close(); return nil },
		CloseContext: handler.CloseContext,
		CleanupFn:    handler.Cleanup,
	})
}

func validate(policy *Policy) error {
	if !config.ValidPolicy(policy.MetadataPolicy) {
		return fmt.Errorf("invalid npm metadata policy %q", policy.MetadataPolicy)
	}
	if !config.ValidPolicy(policy.TarballPolicy) {
		return fmt.Errorf("invalid npm tarball policy %q", policy.TarballPolicy)
	}
	if policy.MetadataBusyPolicy != config.BusyPolicyBypass && policy.MetadataBusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid npm metadata busy policy %q", policy.MetadataBusyPolicy)
	}
	if policy.MetadataFreshFor > 0 && policy.MetadataFreshFor.Duration() < time.Second {
		return fmt.Errorf("npm metadata fresh_for must be at least 1s")
	}
	return nil
}
