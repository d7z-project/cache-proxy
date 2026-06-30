package apk

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
func (Driver) Mode() string              { return config.ModeAPK }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	return filerepo.PlanRepoMode(plan, config.ModeAPK, config.Freshness(time.Minute), time.Hour, classify, discoverer{}, buildSnapshot, rebuildCleanupIndex)
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
		return filerepo.ResourceUnknown
	}
}
