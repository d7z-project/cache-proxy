package pacman

import (
	"context"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Config = filerepo.BasicPolicy
type Policy = Config
type Block = filerepo.RepoBlock

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModePacman }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	return filerepo.PlanRepoMode(plan, config.ModePacman, config.Freshness(time.Minute), 2*time.Minute, classify, discoverer{}, buildSnapshot)
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
		return filerepo.ResourceUnknown
	}
}
