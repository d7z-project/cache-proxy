package rpm

import (
	"context"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeRPM }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	return filerepo.PlanRepoMode(plan, config.ModeRPM, config.Freshness(time.Minute), time.Hour, inspector{}, buildSnapshot)
}
