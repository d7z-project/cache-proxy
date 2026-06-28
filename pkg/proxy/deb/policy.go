package deb

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
func (Driver) Mode() string              { return config.ModeDEB }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	return filerepo.PlanRepoMode(plan, config.ModeDEB, config.Freshness(2*time.Minute), time.Hour, classify, discoverer{}, buildSnapshot)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.Count(cleanPath, "/") >= 2 && strings.HasPrefix(cleanPath, "dists/"):
		return filerepo.ResourceMetadata
	case strings.HasPrefix(cleanPath, "pool/") && (strings.HasSuffix(cleanPath, ".deb") || strings.HasSuffix(cleanPath, ".udeb") || strings.HasSuffix(cleanPath, ".ddeb") || strings.HasSuffix(cleanPath, ".dsc") || strings.Contains(cleanPath, ".orig.tar.") || strings.Contains(cleanPath, ".debian.tar.") || strings.HasSuffix(cleanPath, ".diff.gz")):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".gpg"), strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5sum"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceUnknown
	}
}
