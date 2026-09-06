package app

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const statusAPIPath = "/-/status/"

type statusSummary struct {
	Healthy            bool   `json:"healthy"`
	DegradedObjects    int    `json:"degraded_objects"`
	LastSampleAt       string `json:"last_sample_at,omitempty"`
	DiskSampleInterval int64  `json:"disk_sample_interval_seconds"`
	DiskHistoryWindow  int64  `json:"disk_history_window_seconds"`
	EventLimit         int    `json:"event_limit"`
}

type diskSample struct {
	At         string `json:"at"`
	TotalBytes int64  `json:"total_bytes"`
}

type taskEvent struct {
	Validation      *validationEvent `json:"validation,omitempty"`
	Storage         string           `json:"storage"`
	TaskType        string           `json:"task_type"`
	Target          string           `json:"target"`
	StartedAt       string           `json:"started_at"`
	FinishedAt      string           `json:"finished_at"`
	DurationMS      int64            `json:"duration_ms"`
	QueueDurationMS int64            `json:"queue_duration_ms"`
	Result          string           `json:"result"`
	Reason          string           `json:"reason,omitempty"`
	Phase           string           `json:"phase,omitempty"`
	Message         string           `json:"message,omitempty"`
}

type validationEvent struct {
	Causes      []string `json:"causes"`
	AgeMS       int64    `json:"age_ms"`
	LifetimeMS  int64    `json:"lifetime_ms"`
	ValidatedAt string   `json:"validated_at"`
}

type appStatus struct {
	diskSampleInterval time.Duration
	diskHistoryWindow  time.Duration
	wg                 sync.WaitGroup

	diskMu     sync.RWMutex
	disk       []diskSample
	diskNext   int
	diskFilled bool

	eventMu     sync.RWMutex
	events      []taskEvent
	eventNext   int
	eventFilled bool
}

func newAppStatus(cfg config.ServerStatusConfig) *appStatus {
	diskInterval := cfg.DiskSampleInterval.Duration()
	diskWindow := cfg.DiskHistoryWindow.Duration()
	diskCapacity := int(diskWindow / diskInterval)
	if diskCapacity <= 0 {
		diskCapacity = 1
	}
	return &appStatus{
		disk:               make([]diskSample, diskCapacity),
		events:             make([]taskEvent, cfg.EventLimit),
		diskSampleInterval: diskInterval,
		diskHistoryWindow:  diskWindow,
	}
}

func (s *appStatus) start(ctx context.Context, app *App) {
	s.recordDiskUsage(ctx, app)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.diskSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.recordDiskUsage(ctx, app)
			}
		}
	}()
}

func (s *appStatus) stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return waitForGroup(ctx, &s.wg)
}

func (s *appStatus) observeTaskRun(run scheduler.TaskRun) {
	var validation *validationEvent
	if v := run.Validation; !v.ValidatedAt.IsZero() {
		validation = &validationEvent{AgeMS: v.Age.Milliseconds(), LifetimeMS: v.Lifetime.Milliseconds(), ValidatedAt: v.ValidatedAt.Format(time.RFC3339)}
		for _, cause := range []struct {
			name   string
			active bool
		}{
			{"client_no_cache", v.ClientNoCache},
			{"client_max_age_zero", v.ClientMaxAgeZero},
			{"response_no_cache", v.ResponseNoCache},
			{"freshness_expired", v.FreshnessExpired},
			{"protocol_expired", v.ProtocolExpired},
		} {
			if cause.active {
				validation.Causes = append(validation.Causes, cause.name)
			}
		}
	}
	target := run.Target
	if target == "" {
		target = run.Key.RootID()
	}
	if target == "" {
		target = "/"
	}
	s.appendEvent(taskEvent{
		Validation:      validation,
		Storage:         run.Key.Instance(),
		TaskType:        string(run.Key.Type()),
		Target:          target,
		StartedAt:       run.StartedAt.Format(time.RFC3339),
		FinishedAt:      run.FinishedAt.Format(time.RFC3339),
		DurationMS:      run.Duration.Milliseconds(),
		QueueDurationMS: run.QueueDuration.Milliseconds(),
		Result:          run.Result,
		Reason:          run.Reason,
		Phase:           run.Phase,
		Message:         run.Err,
	})
}

func (s *appStatus) appendEvent(event taskEvent) {
	if len(s.events) == 0 {
		return
	}
	s.eventMu.Lock()
	s.events[s.eventNext] = event
	s.eventNext = (s.eventNext + 1) % len(s.events)
	if s.eventNext == 0 {
		s.eventFilled = true
	}
	s.eventMu.Unlock()
}

func (s *appStatus) summary(app *App) statusSummary {
	summary := statusSummary{
		Healthy:            true,
		DiskSampleInterval: int64(s.diskSampleInterval / time.Second),
		DiskHistoryWindow:  int64(s.diskHistoryWindow / time.Second),
		EventLimit:         len(s.events),
	}
	s.diskMu.RLock()
	if s.diskFilled || s.diskNext > 0 {
		last := s.diskNext - 1
		if last < 0 {
			last = len(s.disk) - 1
		}
		summary.LastSampleAt = s.disk[last].At
	}
	s.diskMu.RUnlock()
	if len(app.stores) == 0 {
		return summary
	}
	baseCtx := app.lifecycleCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
	defer cancel()
	for _, store := range app.stores {
		stats, err := store.Stats(ctx)
		if err == nil && stats != nil {
			summary.DegradedObjects += stats.DegradedObjects
		}
	}
	summary.Healthy = summary.DegradedObjects == 0
	return summary
}

func (s *appStatus) diskSamples() []diskSample {
	s.diskMu.RLock()
	defer s.diskMu.RUnlock()
	return ringSnapshot(s.disk, s.diskNext, s.diskFilled, 0)
}

func (s *appStatus) taskEvents(limit int) []taskEvent {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return ringSnapshot(s.events, s.eventNext, s.eventFilled, limit)
}

func (s *appStatus) recordDiskUsage(ctx context.Context, app *App) {
	if len(app.stores) == 0 {
		return
	}
	sampleCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var total int64
	for _, store := range app.stores {
		stats, err := store.Stats(sampleCtx)
		if err == nil && stats != nil {
			total += stats.Bytes.LogicalObjectBytes
		}
	}
	s.diskMu.Lock()
	s.disk[s.diskNext] = diskSample{
		At:         time.Now().Format(time.RFC3339),
		TotalBytes: total,
	}
	s.diskNext = (s.diskNext + 1) % len(s.disk)
	if s.diskNext == 0 {
		s.diskFilled = true
	}
	s.diskMu.Unlock()
}

func ringSnapshot[T any](items []T, next int, filled bool, limit int) []T {
	count := len(items)
	if !filled {
		count = next
	}
	if limit > 0 {
		count = min(count, limit)
	}
	if count == 0 {
		return nil
	}
	start := next - count
	if start < 0 {
		start += len(items)
	}
	first := min(count, len(items)-start)
	snapshot := make([]T, 0, count)
	snapshot = append(snapshot, items[start:start+first]...)
	return append(snapshot, items[:count-first]...)
}

func (a *App) serveStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	if a.status == nil {
		http.Error(w, "status endpoint disabled", http.StatusNotFound)
		return
	}
	switch req.URL.Path {
	case "/-/status/summary":
		writeStatusJSON(w, req, a.status.summary(a))
	case "/-/status/disk":
		writeStatusJSON(w, req, map[string]any{"samples": a.status.diskSamples()})
	case "/-/status/events":
		limit, _ := strconv.Atoi(req.URL.Query().Get("limit"))
		writeStatusJSON(w, req, map[string]any{"events": a.status.taskEvents(limit)})
	case "/-/status/network":
		writeStatusJSON(w, req, a.status.network(a))
	default:
		http.NotFound(w, req)
	}
}

func writeStatusJSON(w http.ResponseWriter, req *http.Request, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		proxyruntime.WriteError(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if req.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(data)
}
