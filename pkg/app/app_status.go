package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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
	Message    string `json:"message,omitempty"`
}

type appStatus struct {
	store *statusStore

	diskSampleInterval time.Duration
	diskHistoryWindow  time.Duration
	eventLimit         int
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
	if ctx == nil {
		ctx = context.Background()
	}
	s.recordDiskUsage(ctx, app)
	go func() {
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

func (s *appStatus) observeTaskRun(run scheduler.TaskRun) {
	if len(s.store.events) == 0 {
		return
	}
	target := run.Key.SubPath()
	if target == "" {
		target = "/"
	}
	event := taskEvent{
		Storage:    run.Key.Instance(),
		TaskType:   string(run.Key.Type()),
		Target:     target,
		StartedAt:  run.StartedAt.Format(time.RFC3339),
		FinishedAt: run.FinishedAt.Format(time.RFC3339),
		DurationMS: run.Duration.Milliseconds(),
		Result:     run.Result,
	}
	if run.Err != "" {
		event.Message = run.Err
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
		DiskSampleInterval: int64(s.diskSampleInterval / time.Second),
		DiskHistoryWindow:  int64(s.diskHistoryWindow / time.Second),
		EventLimit:         s.eventLimit,
	}
	if snapshot := s.latestDiskSample(); snapshot != nil {
		summary.LastSampleAt = snapshot.At
	}
	if app.store == nil {
		return summary
	}
	baseCtx := app.lifecycleCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
	defer cancel()
	stats, err := app.store.Stats(ctx)
	if err != nil || stats == nil {
		return summary
	}
	summary.DegradedObjects = stats.DegradedObjects
	summary.Healthy = stats.DegradedObjects == 0
	return summary
}

func (s *appStatus) diskSamples() []diskSample {
	s.store.diskMu.RLock()
	defer s.store.diskMu.RUnlock()
	return append([]diskSample(nil), s.store.diskSnapshotLocked()...)
}

func (s *appStatus) taskEvents(limit int) []taskEvent {
	if limit <= 0 || limit > s.eventLimit {
		limit = s.eventLimit
	}
	s.store.eventMu.RLock()
	defer s.store.eventMu.RUnlock()
	all := s.store.eventSnapshotLocked()
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return append([]taskEvent(nil), all...)
}

func (s *appStatus) latestDiskSample() *diskSample {
	s.store.diskMu.RLock()
	defer s.store.diskMu.RUnlock()
	items := s.store.diskSnapshotLocked()
	if len(items) == 0 {
		return nil
	}
	last := items[len(items)-1]
	return &last
}

func (s *appStatus) recordDiskUsage(ctx context.Context, app *App) {
	if app.store == nil {
		return
	}
	sampleCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stats, err := app.store.Stats(sampleCtx)
	if err != nil || stats == nil {
		return
	}
	s.store.diskMu.Lock()
	s.store.disk[s.store.diskNext] = diskSample{
		At:         time.Now().Format(time.RFC3339),
		TotalBytes: stats.Bytes.LogicalObjectBytes,
	}
	s.store.diskNext = (s.store.diskNext + 1) % len(s.store.disk)
	if s.store.diskNext == 0 {
		s.store.diskFilled = true
	}
	s.store.diskMu.Unlock()
}

func (s *statusStore) diskSnapshotLocked() []diskSample {
	if len(s.disk) == 0 {
		return nil
	}
	if !s.diskFilled {
		return append([]diskSample(nil), s.disk[:s.diskNext]...)
	}
	items := make([]diskSample, 0, len(s.disk))
	items = append(items, s.disk[s.diskNext:]...)
	items = append(items, s.disk[:s.diskNext]...)
	return items
}

func (s *statusStore) eventSnapshotLocked() []taskEvent {
	if len(s.events) == 0 {
		return nil
	}
	if !s.eventFilled {
		return append([]taskEvent(nil), s.events[:s.eventNext]...)
	}
	items := make([]taskEvent, 0, len(s.events))
	items = append(items, s.events[s.eventNext:]...)
	items = append(items, s.events[:s.eventNext]...)
	return items
}

func (a *App) serveStatus(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	if a.status == nil {
		writeStatusError(w, req, http.StatusNotFound, errors.New("status endpoint disabled"))
		return
	}
	switch req.URL.Path {
	case "/-/status/summary":
		writeStatusJSON(w, req, a.status.summary(a))
	case "/-/status/disk":
		writeStatusJSON(w, req, map[string]any{"samples": a.status.diskSamples()})
	case "/-/status/events":
		writeStatusJSON(w, req, map[string]any{"events": a.status.taskEvents(parseStatusLimit(req, a.status.eventLimit))})
	default:
		http.NotFound(w, req)
	}
}

func parseStatusLimit(req *http.Request, fallback int) int {
	value := req.URL.Query().Get("limit")
	if value == "" {
		return fallback
	}
	var limit int
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return fallback
		}
		limit = limit*10 + int(ch-'0')
		if limit > fallback {
			return fallback
		}
	}
	if limit <= 0 {
		return fallback
	}
	return limit
}

func writeStatusJSON(w http.ResponseWriter, req *http.Request, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		writeStatusError(w, req, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if req.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(data)
}

func writeStatusError(w http.ResponseWriter, req *http.Request, status int, err error) {
	resp := httpcache.ErrorResponse(status, err)
	_ = resp.FlushClose(req, w)
}
