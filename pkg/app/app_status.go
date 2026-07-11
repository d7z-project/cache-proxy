package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const statusAPIPath = "/-/status/"
const statusTenant = "_status"
const statusStatePath = "status/state.json"

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
	blobStore          *blobfs.Store
	persistCh          chan struct{}
	ctx                context.Context
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

type persistedStatus struct {
	Version int          `json:"version"`
	Disk    []diskSample `json:"disk"`
	Events  []taskEvent  `json:"events"`
}

func newAppStatus(cfg config.ServerStatusConfig, store *blobfs.Store) *appStatus {
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
		blobStore:          store,
		persistCh:          make(chan struct{}, 1),
	}
}

func (s *appStatus) start(ctx context.Context, app *App, b *bus.Bus) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx = ctx
	s.restore()
	go s.persistLoop()
	if b != nil {
		go s.busLoop(ctx, b.Subscribe(bus.EventUpstreamState))
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
	target := run.Key.RootID()
	if target == "" {
		target = "/"
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
		Message:    taskRunMessage(run),
	})
}

func taskRunMessage(run scheduler.TaskRun) string {
	if run.Message != "" {
		return run.Message
	}
	return run.Err
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
	s.markDirty()
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
	s.markDirty()
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

func (s *appStatus) busLoop(ctx context.Context, ch <-chan bus.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-ch:
			if evt.Type != bus.EventUpstreamState {
				continue
			}
			payload, ok := evt.Payload.(bus.UpstreamStatePayload)
			if !ok {
				continue
			}
			timestamp := evt.Timestamp
			if timestamp.IsZero() {
				timestamp = time.Now()
			}
			message := payload.Reason
			if payload.Detail != "" {
				message += ": " + payload.Detail
			}
			if payload.From != "" && payload.To != "" {
				if message != "" {
					message += " (" + payload.From + " -> " + payload.To + ")"
				} else {
					message = payload.From + " -> " + payload.To
				}
			}
			s.appendEvent(taskEvent{
				Storage:    payload.Instance,
				TaskType:   "upstream_state",
				Target:     payload.Upstream,
				StartedAt:  timestamp.Format(time.RFC3339),
				FinishedAt: timestamp.Format(time.RFC3339),
				DurationMS: 0,
				Result:     payload.To,
				StateFrom:  payload.From,
				ReasonCode: payload.Reason,
				Detail:     payload.Detail,
				Message:    message,
			})
		}
	}
}

func (s *appStatus) markDirty() {
	select {
	case s.persistCh <- struct{}{}:
	default:
	}
}

func (s *appStatus) persistLoop() {
	for {
		select {
		case <-s.ctx.Done():
			s.persist()
			return
		case <-s.persistCh:
			s.persist()
		}
	}
}

func (s *appStatus) persist() {
	if s.blobStore == nil {
		return
	}
	state := persistedStatus{
		Version: 1,
		Disk:    s.diskSamples(),
		Events:  s.taskEvents(s.eventLimit),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.blobStore.MkdirAll(path.Join(statusTenant, path.Dir(statusStatePath)), 0o755); err != nil {
		return
	}
	_, _ = s.blobStore.Put(ctx, statusTenant, statusStatePath, bytes.NewReader(data), map[string]string{
		"content-type": "application/json",
	})
}

func (s *appStatus) restore() {
	if s.blobStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reader, err := s.blobStore.OpenObject(ctx, statusTenant, statusStatePath)
	if err != nil {
		return
	}
	defer reader.Close()
	var state persistedStatus
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return
	}
	s.restoreDisk(state.Disk)
	s.restoreEvents(state.Events)
}

func (s *appStatus) restoreDisk(samples []diskSample) {
	if len(s.store.disk) == 0 || len(samples) == 0 {
		return
	}
	if len(samples) > len(s.store.disk) {
		samples = samples[len(samples)-len(s.store.disk):]
	}
	s.store.diskMu.Lock()
	copy(s.store.disk, samples)
	s.store.diskNext = len(samples)
	s.store.diskFilled = false
	if len(samples) == len(s.store.disk) {
		s.store.diskNext = 0
		s.store.diskFilled = true
	}
	s.store.diskMu.Unlock()
}

func (s *appStatus) restoreEvents(events []taskEvent) {
	if len(s.store.events) == 0 || len(events) == 0 {
		return
	}
	if len(events) > len(s.store.events) {
		events = events[len(events)-len(s.store.events):]
	}
	s.store.eventMu.Lock()
	copy(s.store.events, events)
	s.store.eventNext = len(events)
	s.store.eventFilled = false
	if len(events) == len(s.store.events) {
		s.store.eventNext = 0
		s.store.eventFilled = true
	}
	s.store.eventMu.Unlock()
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
