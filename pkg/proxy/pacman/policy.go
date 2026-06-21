package pacman

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

type Block struct {
	ExpireAfter config.Expiration `yaml:"expire_after"`
	Route       struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
	Upstreams []string                `yaml:"upstreams"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Policy    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModePacman }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if len(block.Upstreams) == 0 {
		return fmt.Errorf("instance %s: pacman mode requires at least one upstream", plan.Name())
	}
	filerepo.ApplyDefaults(&block.Policy, config.Freshness(time.Minute))
	if err := filerepo.Validate(config.ModePacman, &block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := filerepo.NewHandler(plan.Name(), config.ModePacman, config.ModePacman, config.Freshness(time.Minute), classify, block.Upstreams, block.Transport, expireAfter, &block.Policy, plan.Store(), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler: handler,
		Close:   func() error { handler.Close(); return nil },
	})
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasSuffix(cleanPath, ".db"), strings.HasSuffix(cleanPath, ".db.sig"), strings.HasSuffix(cleanPath, ".files"), strings.HasSuffix(cleanPath, ".files.sig"):
		return filerepo.ResourceMetadata
	case strings.Contains(cleanPath, ".pkg.tar.") && strings.HasSuffix(cleanPath, ".sig"):
		return filerepo.ResourceAuxiliary
	case strings.Contains(cleanPath, ".pkg.tar."):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceAuxiliary
	}
}
