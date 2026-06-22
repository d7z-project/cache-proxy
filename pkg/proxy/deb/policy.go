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

type Config = filerepo.Policy
type Policy = Config
type Rule = filerepo.Rule

type Repository struct {
	URL           string   `yaml:"url"`
	Suite         string   `yaml:"suite,omitempty"`
	Suites        []string `yaml:"suites,omitempty"`
	Components    []string `yaml:"components"`
	Architectures []string `yaml:"architectures"`
	Source        bool     `yaml:"source,omitempty"`
}

type Block struct {
	ExpireAfter    config.Expiration       `yaml:"expire_after"`
	Route          struct{ Path string }   `yaml:"route"`
	Transport      *config.TransportConfig `yaml:"transport,omitempty"`
	Repositories   []Repository            `yaml:"repositories"`
	RefreshTimeout config.Duration         `yaml:"refresh_timeout,omitempty"`
	Policy         `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeDEB }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	targets, upstreams, err := metadataTargets(block.Repositories)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	filerepo.ApplyDefaults(&block.Policy, config.Freshness(2*time.Minute))
	if err := filerepo.Validate(config.ModeDEB, &block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := filerepo.NewIndexedHandler(plan.Name(), config.ModeDEB, config.ModeDEB, config.Freshness(2*time.Minute), classify, upstreams, block.Transport, expireAfter, &block.Policy, filerepo.RefreshPolicy{
		Interval: time.Hour,
		Timeout:  filerepo.ResolveMetadataRefreshTimeout(block.RefreshTimeout),
	}, targets, buildSnapshot, plan.Store(), plan.Stats())
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
		return filerepo.ResourceAuxiliary
	}
}
