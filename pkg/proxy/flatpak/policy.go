package flatpak

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const (
	defaultCleanupInterval    = 6 * time.Hour
	defaultRefreshInterval    = 5 * time.Minute
	defaultDescriptorFreshFor = 5 * time.Minute
)

type Policy struct {
	DescriptorRewrite *bool             `json:"descriptorRewrite,omitempty" yaml:"descriptor_rewrite,omitempty"`
	VerifyObjects     *bool             `json:"verifyObjects,omitempty" yaml:"verify_objects,omitempty"`
	CacheDeltas       *bool             `json:"cacheDeltas,omitempty" yaml:"cache_deltas,omitempty"`
	DeltaExpireAfter  config.Expiration `json:"deltaExpireAfter,omitempty" yaml:"delta_expire_after,omitempty"`
}

type Block struct {
	ExpireAfter     config.Expiration       `yaml:"expire_after"`
	Route           struct{ Path string }   `yaml:"route"`
	Transport       *config.TransportConfig `yaml:"transport,omitempty"`
	Upstreams       []string                `yaml:"upstreams"`
	RefreshInterval config.Duration         `yaml:"refresh_interval,omitempty"`
	CleanupInterval config.Duration         `yaml:"cleanup_interval,omitempty"`
	Policy          `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeFlatpak }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	applyDefaults(&block.Policy)
	upstreams := collectUpstreams(block.Upstreams)
	if len(upstreams) == 0 {
		return fmt.Errorf("instance %s: flatpak mode requires at least one upstream", plan.Name())
	}
	for _, upstream := range upstreams {
		if err := config.ValidateHTTPUpstream(upstream); err != nil {
			return fmt.Errorf("instance %s: flatpak upstream URL is invalid: %w", plan.Name(), err)
		}
	}
	if err := config.ValidateTransport(block.Transport); err != nil {
		return fmt.Errorf("instance %s: flatpak transport: %w", plan.Name(), err)
	}
	if err := validatePolicy(&block.Policy, block.ExpireAfter); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	if !plan.Enabled() {
		return nil
	}

	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	refreshInterval := defaultRefreshInterval
	if block.RefreshInterval > 0 {
		refreshInterval = block.RefreshInterval.Duration()
	}
	cleanupInterval := defaultCleanupInterval
	if block.CleanupInterval > 0 {
		cleanupInterval = block.CleanupInterval.Duration()
	}
	healthCfg := health.DefaultConfig()
	if block.Transport != nil {
		healthCfg = health.ApplyConfigPatch(healthCfg, block.Transport.Health)
	}
	serviceHealth := health.New(plan.Name(), config.ModeFlatpak, healthCfg, upstreams, plan.Stats())

	runtimeCfg := httpcache.RuntimeConfig{
		Mode:         config.ModeFlatpak,
		ExpireAfter:  expireAfter,
		Upstreams:    upstreams,
		Transport:    block.Transport,
		BusyPolicy:   config.BusyPolicyJoin,
		UpstreamGate: plan.UpstreamGate(),
	}
	handler := NewHandler(
		plan.Name(),
		upstreams,
		block.Transport,
		expireAfter,
		refreshInterval,
		&block.Policy,
		plan.Store(),
		plan.Stats(),
		serviceHealth,
		plan.UpstreamGate(),
		runtimeCfg,
	)
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: cleanupInterval,
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			return nil, handler.Cleanup(ctx, plan.CleanupConfig())
		},
	})
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:            scheduler.NewTaskKey(plan.Name(), scheduler.TypeMetadataRefresh, ""),
		Interval:       refreshInterval,
		RunImmediately: true,
		Handler:        handler.RefreshTask,
	})
	handler.SetRefreshTrigger(func() {
		plan.Scheduler().Trigger(scheduler.NewTaskKey(plan.Name(), scheduler.TypeMetadataRefresh, ""))
	})
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeMetadataGC, ""),
		Interval: max(refreshInterval*3, 6*time.Hour),
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			return nil, handler.CleanupMetadata(ctx)
		},
	})
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}

func applyDefaults(policy *Policy) {
	if policy.DescriptorRewrite == nil {
		enabled := true
		policy.DescriptorRewrite = &enabled
	}
	if policy.VerifyObjects == nil {
		enabled := true
		policy.VerifyObjects = &enabled
	}
	if policy.CacheDeltas == nil {
		enabled := true
		policy.CacheDeltas = &enabled
	}
}

func collectUpstreams(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimRight(strings.TrimSpace(value), "/")
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func resolveDeltaExpireAfter(policy *Policy, expireAfter config.Expiration) config.Expiration {
	if policy == nil || policy.DeltaExpireAfter.IsUnset() {
		return expireAfter
	}
	return policy.DeltaExpireAfter
}

func validatePolicy(policy *Policy, expireAfter config.Expiration) error {
	if policy.CacheDeltas != nil && *policy.CacheDeltas {
		deltaExpireAfter := resolveDeltaExpireAfter(policy, expireAfter)
		if deltaExpireAfter.IsNever() {
			return errors.New("flatpak delta_expire_after must be finite when cache_deltas is enabled")
		}
	}
	return nil
}
