package scheduler

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.d7z.net/blobfs"
)

type TaskType string

const (
	TypeBlobGC        TaskType = "blob_gc"
	TypeExpireCleanup TaskType = "expire_cleanup"
	TypeGitSync       TaskType = "git_sync"
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
	Result     string
	ReasonCode string
	Detail     string
	Message    string
}

var ErrTaskSkipped = errors.New("task skipped")

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
	ReasonCode string
	Detail     string
	Message    string
	Err        string
}

type scheduledTask struct {
	info    TaskInfo
	handler TaskHandler
}

type Scheduler struct {
	mu       sync.Mutex
	tasks    map[TaskKey]*scheduledTask
	observer func(TaskRun)
	wake     chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	stopped  bool
}

func New(_ *blobfs.Store, _ prometheus.Registerer) *Scheduler {
	return &Scheduler{tasks: make(map[TaskKey]*scheduledTask), wake: make(chan struct{}, 1), done: make(chan struct{})}
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
	s.tasks[def.Key] = &scheduledTask{info: TaskInfo{Key: def.Key, Status: StatusIdle, NextRun: next, Interval: def.Interval}, handler: def.Handler}
	s.mu.Unlock()
	s.signal()
}

func (s *Scheduler) Info(key TaskKey) (TaskInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[key]
	if !ok {
		return TaskInfo{}, false
	}
	return task.info, true
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
			s.run(key)
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

func (s *Scheduler) run(key TaskKey) {
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
	if outcome != nil {
		run.Result, run.ReasonCode, run.Detail, run.Message = outcome.Result, outcome.ReasonCode, outcome.Detail, outcome.Message
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
		current.info.NextRun = finished.Add(interval)
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
	if observer != nil {
		observer(run)
	}
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
