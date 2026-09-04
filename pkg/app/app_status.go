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
	Storage    string `json:"storage"`
	TaskType   string `json:"task_type"`
	Target     string `json:"target"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
	DurationMS int64  `json:"duration_ms"`
	Result     string `json:"result"`
	StateFrom  string `json:"state_from,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
	Detail     string `json:"detail,omitempty"`
	Message    string `json:"message,omitempty"`
}

type appStatus struct {
	store *statusStore

	diskSampleInterval time.Duration
	diskHistoryWindow  time.Duration
	eventLimit         int
	wg                 sync.WaitGroup
}

type statusStore struct {
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
		store: &statusStore{
			disk:   make([]diskSample, diskCapacity),
			events: make([]taskEvent, cfg.EventLimit),
		},
		diskSampleInterval: diskInterval,
		diskHistoryWindow:  diskWindow,
		eventLimit:         cfg.EventLimit,
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
	target := run.Key.RootID()
	if target == "" {
		target = "/"
	}
	message := run.Message
	if message == "" {
		message = run.Err
	}
	s.appendEvent(taskEvent{
		Storage:    run.Key.Instance(),
		TaskType:   string(run.Key.Type()),
		Target:     target,
		StartedAt:  run.StartedAt.Format(time.RFC3339),
		FinishedAt: run.FinishedAt.Format(time.RFC3339),
		DurationMS: run.Duration.Milliseconds(),
		Result:     run.Result,
		ReasonCode: run.ReasonCode,
		Detail:     run.Detail,
		Message:    message,
	})
}

func (s *appStatus) appendEvent(event taskEvent) {
	if len(s.store.events) == 0 {
		return
	}
	s.store.eventMu.Lock()
	s.store.events[s.store.eventNext] = event
	s.store.eventNext = (s.store.eventNext + 1) % len(s.store.events)
	if s.store.eventNext == 0 {
		s.store.eventFilled = true
	}
	s.store.eventMu.Unlock()
}

func (s *appStatus) summary(app *App) statusSummary {
	summary := statusSummary{
		Healthy:            true,
		DiskSampleInterval: int64(s.diskSampleInterval / time.Second),
		DiskHistoryWindow:  int64(s.diskHistoryWindow / time.Second),
		EventLimit:         s.eventLimit,
	}
	if samples := s.diskSamples(); len(samples) > 0 {
		summary.LastSampleAt = samples[len(samples)-1].At
	}
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
	s.store.diskMu.RLock()
	defer s.store.diskMu.RUnlock()
	return ringSnapshot(s.store.disk, s.store.diskNext, s.store.diskFilled)
}

func (s *appStatus) taskEvents(limit int) []taskEvent {
	if limit <= 0 || limit > s.eventLimit {
		limit = s.eventLimit
	}
	s.store.eventMu.RLock()
	defer s.store.eventMu.RUnlock()
	all := ringSnapshot(s.store.events, s.store.eventNext, s.store.eventFilled)
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all
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
	s.store.diskMu.Lock()
	s.store.disk[s.store.diskNext] = diskSample{
		At:         time.Now().Format(time.RFC3339),
		TotalBytes: total,
	}
	s.store.diskNext = (s.store.diskNext + 1) % len(s.store.disk)
	if s.store.diskNext == 0 {
		s.store.diskFilled = true
	}
	s.store.diskMu.Unlock()
}

func ringSnapshot[T any](items []T, next int, filled bool) []T {
	if len(items) == 0 {
		return nil
	}
	if !filled {
		return append([]T(nil), items[:next]...)
	}
	snapshot := make([]T, 0, len(items))
	snapshot = append(snapshot, items[next:]...)
	return append(snapshot, items[:next]...)
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
