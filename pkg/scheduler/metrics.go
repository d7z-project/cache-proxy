package scheduler

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	registered         *prometheus.CounterVec
	unregistered       *prometheus.CounterVec
	active             *prometheus.GaugeVec
	status             *prometheus.GaugeVec
	runs               *prometheus.CounterVec
	duration           *prometheus.HistogramVec
	nextDelay          *prometheus.GaugeVec
	overdue            *prometheus.GaugeVec
	backoff            *prometheus.GaugeVec
	discoveryToRefresh *prometheus.HistogramVec
	discoveriesPending *prometheus.GaugeVec
	stateSaves         *prometheus.CounterVec
	stateRestore       *prometheus.CounterVec
	restoredTasks      *prometheus.CounterVec
	restoreSkipped     *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		return nil
	}
	m := &metrics{
		registered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_tasks_registered_total",
			Help: "Total scheduler task registrations by instance, task type, and source.",
		}, []string{"instance", "task_type", "source"}),
		unregistered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_tasks_unregistered_total",
			Help: "Total scheduler task unregistrations by instance, task type, and reason.",
		}, []string{"instance", "task_type", "reason"}),
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_scheduler_tasks_active",
			Help: "Current active scheduler tasks by instance and task type.",
		}, []string{"instance", "task_type"}),
		status: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_scheduler_task_status",
			Help: "Current scheduler task counts by instance, task type, and status.",
		}, []string{"instance", "task_type", "status"}),
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_task_runs_total",
			Help: "Total scheduler task runs by instance, task type, and result.",
		}, []string{"instance", "task_type", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_proxy_scheduler_task_duration_seconds",
			Help:    "Scheduler task execution duration by instance, task type, and result.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 3, 10, 30, 60, 300, 900},
		}, []string{"instance", "task_type", "result"}),
		nextDelay: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_scheduler_task_next_delay_seconds",
			Help: "Seconds until the next scheduled run by instance and task type.",
		}, []string{"instance", "task_type"}),
		overdue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_scheduler_task_overdue_seconds",
			Help: "Seconds that scheduled tasks are overdue by instance and task type.",
		}, []string{"instance", "task_type"}),
		backoff: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_scheduler_task_backoff_seconds",
			Help: "Current retry backoff seconds by instance and task type.",
		}, []string{"instance", "task_type"}),
		discoveryToRefresh: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_proxy_metadata_discovery_to_refresh_seconds",
			Help:    "Time from metadata discovery to first refresh completion by instance and result.",
			Buckets: []float64{0.05, 0.1, 0.5, 1, 3, 10, 30, 60, 300, 900},
		}, []string{"instance", "result"}),
		discoveriesPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_metadata_discoveries_pending",
			Help: "Current discovered metadata roots awaiting first refresh completion by instance.",
		}, []string{"instance"}),
		stateSaves: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_state_saves_total",
			Help: "Total scheduler state save attempts by result.",
		}, []string{"result"}),
		stateRestore: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_state_restore_total",
			Help: "Total scheduler state restore attempts by result.",
		}, []string{"result"}),
		restoredTasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_restored_tasks_total",
			Help: "Total scheduler tasks restored from persisted state by task type.",
		}, []string{"task_type"}),
		restoreSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_scheduler_restore_skipped_total",
			Help: "Total persisted scheduler tasks skipped during restore by task type and reason.",
		}, []string{"task_type", "reason"}),
	}
	reg.MustRegister(
		m.registered,
		m.unregistered,
		m.active,
		m.status,
		m.runs,
		m.duration,
		m.nextDelay,
		m.overdue,
		m.backoff,
		m.discoveryToRefresh,
		m.discoveriesPending,
		m.stateSaves,
		m.stateRestore,
		m.restoredTasks,
		m.restoreSkipped,
	)
	return m
}

func clampDurationSeconds(d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return d.Seconds()
}
