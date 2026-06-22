package rpm

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
	URL   string   `yaml:"url"`
	Path  string   `yaml:"path,omitempty"`
	Paths []string `yaml:"paths,omitempty"`
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
func (Driver) Mode() string              { return config.ModeRPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	targets, upstreams, err := metadataTargets(block.Repositories)
	if err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	filerepo.ApplyDefaults(&block.Policy, config.Freshness(time.Minute))
	if err := filerepo.Validate(config.ModeRPM, &block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := filerepo.NewIndexedHandler(plan.Name(), config.ModeRPM, config.ModeRPM, config.Freshness(time.Minute), classify, upstreams, block.Transport, expireAfter, &block.Policy, filerepo.RefreshPolicy{
		Interval: time.Hour,
		Timeout:  filerepo.ResolveMetadataRefreshTimeout(block.RefreshTimeout),
	}, targets, buildSnapshot, plan.Store(), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, handler)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasPrefix(cleanPath, "repodata/"), strings.HasSuffix(cleanPath, "/repomd.xml"), cleanPath == "repomd.xml", strings.HasSuffix(cleanPath, "/mirrorlist"), cleanPath == "mirrorlist", strings.HasSuffix(cleanPath, "/metalink"), cleanPath == "metalink":
		return filerepo.ResourceMetadata
	case strings.HasSuffix(cleanPath, ".rpm"), strings.HasSuffix(cleanPath, ".drpm"):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceAuxiliary
	}
}
