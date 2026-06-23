package deb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Config = filerepo.BasicPolicy
type Policy = Config

type Block struct {
	ExpireAfter     config.Expiration       `yaml:"expire_after"`
	Route           struct{ Path string }   `yaml:"route"`
	Transport       *config.TransportConfig `yaml:"transport,omitempty"`
	Upstreams       []string                `yaml:"upstreams"`
	RefreshInterval config.Duration         `yaml:"refresh_interval,omitempty"`
	RefreshTimeout  config.Duration         `yaml:"refresh_timeout,omitempty"`
	Policy          `yaml:",inline"`
}

type Driver struct{}

const defaultRefreshInterval = time.Hour

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeDEB }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	upstreams := filerepo.CollectUpstreams(block.Upstreams, nil)
	if len(upstreams) == 0 {
		return fmt.Errorf("instance %s: deb mode requires at least one upstream", plan.Name())
	}
	policy := block.Policy.AsPolicy()
	filerepo.ApplyDefaults(policy, config.Freshness(2*time.Minute))
	if err := filerepo.Validate(config.ModeDEB, policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := filerepo.NewIndexedHandler(plan.Name(), config.ModeDEB, config.ModeDEB, config.Freshness(2*time.Minute), classify, upstreams, block.Transport, expireAfter, policy, filerepo.RefreshPolicy{
		Interval: filerepo.ResolveMetadataRefreshInterval(block.RefreshInterval, defaultRefreshInterval),
		Timeout:  filerepo.ResolveMetadataRefreshTimeout(block.RefreshTimeout),
	}, discoverer{}, nil, buildSnapshot, plan.Store(), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasPrefix(cleanPath, "dists/"):
		return filerepo.ResourceMetadata
	case strings.HasPrefix(cleanPath, "pool/") && (strings.HasSuffix(cleanPath, ".deb") || strings.HasSuffix(cleanPath, ".udeb") || strings.HasSuffix(cleanPath, ".ddeb") || strings.HasSuffix(cleanPath, ".dsc") || strings.Contains(cleanPath, ".orig.tar.") || strings.Contains(cleanPath, ".debian.tar.") || strings.HasSuffix(cleanPath, ".diff.gz")):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".gpg"), strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5sum"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceUnknown
	}
}
