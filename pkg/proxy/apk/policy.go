package apk

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
func (Driver) Mode() string              { return config.ModeAPK }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	upstreams := filerepo.CollectUpstreams(block.Upstreams, nil)
	if len(upstreams) == 0 {
		return fmt.Errorf("instance %s: apk mode requires at least one upstream", plan.Name())
	}
	policy := block.Policy.AsPolicy()
	filerepo.ApplyDefaults(policy, config.Freshness(time.Minute))
	if err := filerepo.Validate(config.ModeAPK, policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := filerepo.NewIndexedHandler(plan.Name(), config.ModeAPK, config.ModeAPK, config.Freshness(time.Minute), classify, upstreams, block.Transport, expireAfter, policy, filerepo.RefreshPolicy{
		Interval: filerepo.ResolveMetadataRefreshInterval(block.RefreshInterval, defaultRefreshInterval),
		Timeout:  filerepo.ResolveMetadataRefreshTimeout(block.RefreshTimeout),
	}, discoverer{}, nil, buildSnapshot, plan.Store(), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasSuffix(cleanPath, "/APKINDEX.tar.gz"), strings.HasSuffix(cleanPath, "/APKINDEX.tar.gz.sig"), cleanPath == "APKINDEX.tar.gz", cleanPath == "APKINDEX.tar.gz.sig":
		return filerepo.ResourceMetadata
	case strings.HasSuffix(cleanPath, ".apk.sig"), strings.HasSuffix(cleanPath, ".apk.asc"), strings.HasSuffix(cleanPath, ".apk.sha256"), strings.HasSuffix(cleanPath, ".apk.sha512"):
		return filerepo.ResourceAuxiliary
	case strings.HasSuffix(cleanPath, ".apk"):
		return filerepo.ResourceArtifact
	default:
		return filerepo.ResourceAuxiliary
	}
}
