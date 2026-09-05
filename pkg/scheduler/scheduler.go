package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type TaskType string

const (
	TypeBlobGC          TaskType = "blob_gc"
	TypeExpireCleanup   TaskType = "expire_cleanup"
	TypeGitSync         TaskType = "git_sync"
	TypeMetadataRefresh TaskType = "metadata_refresh"
	TypeMetadataGC      TaskType = "metadata_gc"
)

type TaskStatus string

const (
	StatusIdle    TaskStatus = "idle"
	StatusRunning TaskStatus = "running"
	StatusDone    TaskStatus = "done"
	StatusFailed  TaskStatus = "failed"
)

type TaskHandler func(context.Context) (*TaskOutcome, error)

type TaskOutcome struct {
	Result        string
	ContinueAfter time.Duration
}

type TaskKey struct {
	instance string
	typ      TaskType
	rootID   string
}

func NewTaskKey(instance string, typ TaskType, rootID string) TaskKey {
	return TaskKey{instance: instance, typ: typ, rootID: rootID}
}

func (k TaskKey) String() string   { return k.instance + ":" + string(k.typ) + ":" + k.rootID }
func (k TaskKey) Instance() string { return k.instance }
func (k TaskKey) Type() TaskType   { return k.typ }
func (k TaskKey) RootID() string   { return k.rootID }

type TaskDef struct {
	Key            TaskKey
	Interval       time.Duration
	RunImmediately bool
	Handler        TaskHandler
}

type TaskInfo struct {
	Key       TaskKey
	Status    TaskStatus
	NextRun   time.Time
	LastRun   time.Time
	LastError string
	RunCount  uint64
	ErrCount  uint64
	Interval  time.Duration
}

type TaskRun struct {
	Key        TaskKey
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Result     string
	Err        string
}

type scheduledTask struct {
	info      TaskInfo
	handler   TaskHandler
	triggered bool
}

type Scheduler struct {
	mu        sync.Mutex
	tasks     map[TaskKey]*scheduledTask
	observer  func(TaskRun)
	wake      chan struct{}
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	started   bool
	stopped   bool
	statePath string
	persisted map[string]time.Time
}

func newScheduler() *Scheduler {
	return &Scheduler{tasks: make(map[TaskKey]*scheduledTask), wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func NewPersistent(statePath string) (*Scheduler, error) {
	s := newScheduler()
	s.statePath = statePath
	s.persisted = make(map[string]time.Time)
	data, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read scheduler state: %w", err)
	}
	var state struct {
		NextRun map[string]time.Time `json:"next_run"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode scheduler state: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("scheduler state contains trailing data")
	}
	s.persisted = state.NextRun
	return s, nil
}

func (s *Scheduler) Register(def TaskDef) {
	if def.Handler == nil || def.Interval <= 0 {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	next := time.Now().Add(def.Interval)
	if def.RunImmediately {
		next = time.Now()
	}
	if persistentTask(def.Key.typ) {
		if saved, ok := s.persisted[def.Key.String()]; ok && !saved.IsZero() {
			next = saved
		}
	}
	s.tasks[def.Key] = &scheduledTask{info: TaskInfo{Key: def.Key, Status: StatusIdle, NextRun: next, Interval: def.Interval}, handler: def.Handler}
	s.mu.Unlock()
	s.signal()
}

// TriggerNow makes a registered task eligible for the next scheduler
// iteration. A trigger received while the task runs schedules another run.
func (s *Scheduler) TriggerNow(key TaskKey) bool {
	s.mu.Lock()
	task := s.tasks[key]
	if task == nil || s.stopped {
		s.mu.Unlock()
		return false
	}
	if task.info.Status == StatusRunning {
		task.triggered = true
		s.mu.Unlock()
		return true
	}
	task.info.NextRun = time.Now()
	s.mu.Unlock()
	s.signal()
	return true
}

func (s *Scheduler) Snapshot() []TaskInfo {
	s.mu.Lock()
	infos := make([]TaskInfo, 0, len(s.tasks))
	for _, task := range s.tasks {
		infos = append(infos, task.info)
	}
	s.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key.String() < infos[j].Key.String() })
	return infos
}

func (s *Scheduler) Start(parent context.Context) {
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(parent)
	s.mu.Unlock()
	go s.loop()
}

func (s *Scheduler) SetRunObserver(observer func(TaskRun)) {
	s.mu.Lock()
	s.observer = observer
	s.mu.Unlock()
}

func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		if s.cancel != nil {
			s.cancel()
		} else {
			close(s.done)
		}
	}
	done := s.done
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) loop() {
	defer close(s.done)
	for {
		key, wait := s.nextTask()
		timer := time.NewTimer(wait)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-s.wake:
			timer.Stop()
			continue
		case <-timer.C:
		}
		if key != (TaskKey{}) {
			s.runTask(key)
		}
	}
}

func (s *Scheduler) nextTask() (TaskKey, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var selected TaskKey
	var next time.Time
	for key, task := range s.tasks {
		if next.IsZero() || task.info.NextRun.Before(next) {
			selected, next = key, task.info.NextRun
		}
	}
	if next.IsZero() {
		return TaskKey{}, time.Hour
	}
	return selected, max(time.Until(next), 0)
}

func (s *Scheduler) runTask(key TaskKey) {
	s.mu.Lock()
	task := s.tasks[key]
	if task == nil {
		s.mu.Unlock()
		return
	}
	task.info.Status = StatusRunning
	handler := task.handler
	interval := task.info.Interval
	s.mu.Unlock()

	started := time.Now()
	deadline := interval / 2
	if deadline < time.Minute {
		deadline = time.Minute
	}
	ctx, cancel := context.WithTimeout(s.ctx, deadline)
	outcome, err := handler(ctx)
	cancel()
	finished := time.Now()
	run := TaskRun{Key: key, StartedAt: started, FinishedAt: finished, Duration: finished.Sub(started)}
	var continueAfter time.Duration
	if outcome != nil {
		run.Result = outcome.Result
		continueAfter = outcome.ContinueAfter
	}
	if run.Result == "" {
		run.Result = "success"
	}
	if err != nil {
		run.Err = err.Error()
		run.Result = "failed"
	}

	s.mu.Lock()
	if current := s.tasks[key]; current != nil {
		current.info.LastRun = finished
		next := interval
		if continueAfter > 0 && continueAfter < next {
			next = continueAfter
		}
		if current.triggered {
			current.triggered = false
			current.info.NextRun = finished
		} else {
			current.info.NextRun = finished.Add(next)
		}
		current.info.RunCount++
		current.info.LastError = run.Err
		if err != nil {
			current.info.Status = StatusFailed
			current.info.ErrCount++
		} else {
			current.info.Status = StatusDone
		}
	}
	observer := s.observer
	s.mu.Unlock()
	if persistentTask(key.typ) {
		_ = s.persist()
	}
	if observer != nil {
		observer(run)
	}
}

func persistentTask(taskType TaskType) bool {
	return taskType == TypeMetadataRefresh || taskType == TypeMetadataGC
}

// persist runs only on the scheduler loop; mu protects the snapshot from triggers.
func (s *Scheduler) persist() error {
	if s.statePath == "" {
		return nil
	}
	s.mu.Lock()
	nextRun := make(map[string]time.Time)
	for key, task := range s.tasks {
		if persistentTask(key.typ) {
			nextRun[key.String()] = task.info.NextRun
		}
	}
	s.mu.Unlock()
	data, err := json.Marshal(struct {
		NextRun map[string]time.Time `json:"next_run"`
	}{NextRun: nextRun})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.statePath), ".scheduler-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, s.statePath); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(s.statePath))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
