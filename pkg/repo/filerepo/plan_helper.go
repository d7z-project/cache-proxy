package filerepo

import (
	"context"
	"fmt"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

type RepoBlock struct {
	ExpireAfter     config.Expiration       `yaml:"expire_after"`
	Route           struct{ Path string }   `yaml:"route"`
	Transport       *config.TransportConfig `yaml:"transport,omitempty"`
	Upstreams       []string                `yaml:"upstreams"`
	RefreshInterval config.Duration         `yaml:"refresh_interval,omitempty"`
	CleanupInterval config.Duration         `yaml:"cleanup_interval,omitempty"`
	Policy          BasicPolicy             `yaml:",inline"`
}

func PlanRepoMode(
	plan *proxyruntime.InstancePlan,
	mode string,
	defaultFreshFor config.Freshness,
	defaultRefreshInterval time.Duration,
	inspector PathInspector,
	build SnapshotBuilder,
) error {
	var block RepoBlock
	if err := plan.Decode(&block); err != nil {
		return err
	}
	upstreams := CollectUpstreams(block.Upstreams, nil)
	if len(upstreams) == 0 {
		return fmt.Errorf("instance %s: %s mode requires at least one upstream", plan.Name(), mode)
	}
	policy := block.Policy.AsPolicy()
	ApplyDefaults(policy)
	if err := Validate(mode, policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	refreshInterval := ResolveMetadataRefreshInterval(block.RefreshInterval, defaultRefreshInterval)
	gcInterval := max(refreshInterval*3, 6*time.Hour)

	healthCfg := health.DefaultConfig()
	if block.Transport != nil {
		healthCfg = health.ApplyConfigPatch(healthCfg, block.Transport.Health)
	}
	probeUserAgent := httpcache.DefaultUserAgent
	if block.Transport != nil && block.Transport.UserAgent != "" {
		probeUserAgent = block.Transport.UserAgent
	}
	sh := health.New(plan.Name(), mode, healthCfg, upstreams, plan.Stats(), probeUserAgent)
	sh.SetBus(plan.Bus())
	handler := NewIndexedHandler(
		plan.Name(),
		mode,
		mode,
		inspector,
		upstreams,
		block.Transport,
		config.ExpirationNever,
		policy,
		build,
		plan.Store(),
		plan.Stats(),
		sh,
		plan.Downloads(),
	)
	handler.SetBus(plan.Bus())

	sched := plan.Scheduler()

	cleanupInterval := defaultCleanupInterval(block.CleanupInterval)
	sched.Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: cleanupInterval,
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			return nil, handler.Cleanup(ctx, plan.CleanupConfig())
		},
	})

	sched.RegisterFactory(scheduler.TaskFactory{
		Instance:        plan.Name(),
		RefreshInterval: refreshInterval,
		GCInterval:      gcInterval,
		NewRefresh: func(rootID string) scheduler.TaskHandler {
			return func(ctx context.Context) (*scheduler.TaskOutcome, error) {
				return handler.RefreshRootTask(ctx, rootID)
			}
		},
		NewGC: func(rootID string) scheduler.TaskHandler {
			return func(ctx context.Context) (*scheduler.TaskOutcome, error) {
				return nil, handler.CleanupRoot(ctx, rootID, plan.CleanupConfig())
			}
		},
	})

	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}

func defaultCleanupInterval(cfg config.Duration) time.Duration {
	if cfg > 0 {
		return cfg.Duration()
	}
	return 6 * time.Hour
}
