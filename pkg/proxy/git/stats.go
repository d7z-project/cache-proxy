package git

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type gitStats struct {
	cloneSuccess prometheus.Counter
	cloneFailed  prometheus.Counter
	syncSuccess  prometheus.Counter
	syncFailed   prometheus.Counter
	lastSync     prometheus.Gauge
}

var (
	metricCloneSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_git_clone_success_total",
		Help: "Total successful git clone operations.",
	}, []string{"instance"})
	metricCloneFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_git_clone_failed_total",
		Help: "Total failed git clone operations.",
	}, []string{"instance"})
	metricSyncSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_git_sync_success_total",
		Help: "Total successful git sync operations.",
	}, []string{"instance"})
	metricSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_git_sync_failed_total",
		Help: "Total failed git sync operations.",
	}, []string{"instance"})
	metricLastSync = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cache_proxy_git_last_sync_timestamp_seconds",
		Help: "Unix timestamp of the last successful git sync.",
	}, []string{"instance"})
)

func newGitStats(instance string) *gitStats {
	return &gitStats{
		cloneSuccess: metricCloneSuccess.WithLabelValues(instance),
		cloneFailed:  metricCloneFailed.WithLabelValues(instance),
		syncSuccess:  metricSyncSuccess.WithLabelValues(instance),
		syncFailed:   metricSyncFailed.WithLabelValues(instance),
		lastSync:     metricLastSync.WithLabelValues(instance),
	}
}
