package rpm

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
func (Driver) Mode() string              { return config.ModeRPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	return filerepo.PlanRepoMode(plan, config.ModeRPM, config.Freshness(time.Minute), time.Hour, classify, discoverer{}, buildSnapshot, rebuildCleanupIndex)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasPrefix(cleanPath, "repodata/"), strings.Contains(cleanPath, "/repodata/"), strings.HasSuffix(cleanPath, "/repomd.xml"), cleanPath == "repomd.xml", strings.HasSuffix(cleanPath, "/mirrorlist"), cleanPath == "mirrorlist", strings.HasSuffix(cleanPath, "/metalink"), cleanPath == "metalink":
		return filerepo.ResourceMetadata
	case strings.HasSuffix(cleanPath, ".rpm"), strings.HasSuffix(cleanPath, ".drpm"):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceUnknown
	}
}
