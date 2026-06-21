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
func (Driver) Mode() string              { return config.ModeAPK }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if len(block.Upstreams) == 0 {
		return fmt.Errorf("instance %s: apk mode requires at least one upstream", plan.Name())
	}
	filerepo.ApplyDefaults(&block.Policy, config.Freshness(time.Minute))
	if err := filerepo.Validate(config.ModeAPK, &block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := filerepo.NewHandler(plan.Name(), config.ModeAPK, config.ModeAPK, config.Freshness(time.Minute), classify, block.Upstreams, block.Transport, expireAfter, &block.Policy, plan.Store(), plan.Stats())
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:   handler,
		Close:     func() error { handler.Close(); return nil },
		CleanupFn: handler.Cleanup,
	})
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
