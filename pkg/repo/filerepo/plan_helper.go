package filerepo

import (
	"fmt"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type RepoBlock struct {
	ExpireAfter     config.Expiration       `yaml:"expire_after"`
	Route           struct{ Path string }   `yaml:"route"`
	Transport       *config.TransportConfig `yaml:"transport,omitempty"`
	Upstreams       []string                `yaml:"upstreams"`
	RefreshInterval config.Duration         `yaml:"refresh_interval,omitempty"`
	Policy          BasicPolicy             `yaml:",inline"`
}

func PlanRepoMode(plan *proxyruntime.InstancePlan, mode string, defaultFreshFor config.Freshness, defaultRefreshInterval time.Duration, classify func(string) ResourceClass, discover Discoverer, build SnapshotBuilder) error {
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
	handler := NewIndexedHandler(plan.Name(), mode, mode, defaultFreshFor, classify, upstreams, block.Transport, config.ExpirationNever, policy, RefreshPolicy{
		Interval: ResolveMetadataRefreshInterval(block.RefreshInterval, defaultRefreshInterval),
	}, discover, nil, build, plan.Store(), plan.Stats(), NewServiceHealth(block.Transport, upstreams, plan, mode), plan.Downloads())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}
