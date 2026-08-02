package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

type TaskType string

const (
	TypeBlobGC          TaskType = "blob_gc"
	TypeExpireCleanup   TaskType = "expire_cleanup"
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

// TaskHandler runs a scheduled task and may report a structured successful outcome.
type TaskHandler func(ctx context.Context) (*TaskOutcome, error)

// TaskOutcome describes the semantic result of a completed task.
type TaskOutcome struct {
	Result     string
	ReasonCode string
	Detail     string
	Message    string
}

var ErrTaskSkipped = errors.New("task skipped")

var errHandlerPanic = errors.New("handler panic")

const maxTaskErrorBytes = 4096

type RetryAtError struct {
	At time.Time
}

func (e RetryAtError) Error() string {
	return "retry at " + e.At.UTC().Format(time.RFC3339Nano)
}

func RetryAt(at time.Time) error {
	return RetryAtError{At: at}
}

type TaskKey struct {
	instance string
	typ      TaskType
	rootID   string
	str      string
}

func NewTaskKey(instance string, typ TaskType, rootID string) TaskKey {
	s := instance + ":" + string(typ) + ":" + rootID
	return TaskKey{instance: instance, typ: typ, rootID: rootID, str: s}
}

func (k TaskKey) String() string   { return k.str }
func (k TaskKey) Instance() string { return k.instance }
func (k TaskKey) Type() TaskType   { return k.typ }
func (k TaskKey) RootID() string   { return k.rootID }

type TaskDef struct {
	Key      TaskKey
	Interval time.Duration
	Handler  TaskHandler
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

type TaskFactory struct {
	Instance        string
	RefreshInterval time.Duration
	GCInterval      time.Duration
	NewRefresh      func(rootID string) TaskHandler
	NewGC           func(rootID string) TaskHandler
	CurrentRoots    func() []string
}

type taskState struct {
	TaskInfo
	handler      TaskHandler
	index        int
	discoveredAt time.Time
	firstRunDone bool
}

type cmdKind int

const (
	cmdRegister cmdKind = iota
	cmdRegisterFactory
	cmdUnregister
	cmdInfo
	cmdSnapshot
)

type cmd struct {
	kind    cmdKind
	def     TaskDef
	factory TaskFactory
	key     TaskKey
	respCh  chan any
}

type Scheduler struct {
	cmdCh           chan cmd
	bus             *bus.Bus
	busSub          <-chan bus.Event
	startGate       chan struct{}
	done            chan struct{}
	doneOnce        sync.Once
	stopped         atomic.Bool
	factories       map[string]*TaskFactory
	metricInstances map[string]struct{}
	tasks           map[TaskKey]*taskState
	heap            taskHeap

	store   *blobfs.Store
	tenant  string
	storeMu sync.Mutex
	startMu sync.Mutex
	started bool
	m       *metrics

	preStartTasks map[TaskKey]TaskDef
	runObserver   func(TaskRun)
	observerMu    sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

func New(b *bus.Bus, store *blobfs.Store, reg prometheus.Registerer) *Scheduler {
	return &Scheduler{
		cmdCh:           make(chan cmd, 16),
		bus:             b,
		busSub:          b.Subscribe(bus.EventMetadataDiscovered, bus.EventMetadataRemoved),
		startGate:       make(chan struct{}),
		done:            make(chan struct{}),
		factories:       map[string]*TaskFactory{},
		metricInstances: map[string]struct{}{},
		tasks:           map[TaskKey]*taskState{},
		heap:            taskHeap{},
		store:           store,
		tenant:          "_scheduler",
		m:               newMetrics(reg),
		preStartTasks:   map[TaskKey]TaskDef{},
	}
}

func (s *Scheduler) Register(def TaskDef) {
	if s.stopped.Load() {
		return
	}
	if s.withPreStart(func() {
		s.preStartTasks[def.Key] = def
	}) {
		return
	}
	respCh := make(chan any, 1)
	s.submit(cmd{kind: cmdRegister, def: def, respCh: respCh})
}

func (s *Scheduler) RegisterFactory(factory TaskFactory) {
	if s.stopped.Load() {
		return
	}
	if s.withPreStart(func() {
		s.factories[factory.Instance] = &factory
	}) {
		return
	}
	respCh := make(chan any, 1)
	s.submit(cmd{kind: cmdRegisterFactory, factory: factory, respCh: respCh})
}

func (s *Scheduler) Unregister(key TaskKey) {
	if s.stopped.Load() {
		return
	}
	if s.withPreStart(func() {
		delete(s.preStartTasks, key)
	}) {
		return
	}
	respCh := make(chan any, 1)
	s.submit(cmd{kind: cmdUnregister, key: key, respCh: respCh})
}

func (s *Scheduler) Info(key TaskKey) (TaskInfo, bool) {
	if s.stopped.Load() {
		return TaskInfo{}, false
	}
	s.startMu.Lock()
	if !s.started {
		if def, ok := s.preStartTasks[key]; ok {
			s.startMu.Unlock()
			return TaskInfo{
				Key:      key,
				Status:   StatusIdle,
				Interval: def.Interval,
			}, true
		}
		s.startMu.Unlock()
		return TaskInfo{}, false
	}
	s.startMu.Unlock()
	respCh := make(chan any, 1)
	value, ok := s.submit(cmd{kind: cmdInfo, key: key, respCh: respCh})
	if !ok {
		return TaskInfo{}, false
	}
	result := value.(TaskInfo)
	return result, result.Key.instance != ""
}

func (s *Scheduler) Snapshot() []TaskInfo {
	if s.stopped.Load() {
		return nil
	}
	s.startMu.Lock()
	if !s.started {
		infos := make([]TaskInfo, 0, len(s.preStartTasks))
		for _, def := range s.preStartTasks {
			infos = append(infos, TaskInfo{
				Key:      def.Key,
				Status:   StatusIdle,
				Interval: def.Interval,
			})
		}
		s.startMu.Unlock()
		return infos
	}
	s.startMu.Unlock()
	respCh := make(chan any, 1)
	value, ok := s.submit(cmd{kind: cmdSnapshot, respCh: respCh})
	if !ok {
		return nil
	}
	return value.([]TaskInfo)
}

func (s *Scheduler) Start(ctx context.Context) {
	if s.stopped.Load() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.startMu.Lock()
	if s.stopped.Load() {
		s.startMu.Unlock()
		return
	}
	if s.started {
		s.startMu.Unlock()
		return
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.startMu.Unlock()
	go s.loop()
}

func (s *Scheduler) SetRunObserver(fn func(TaskRun)) {
	s.observerMu.Lock()
	s.runObserver = fn
	s.observerMu.Unlock()
}

func (s *Scheduler) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.stopped.CompareAndSwap(false, true) {
		s.startMu.Lock()
		started := s.started
		cancel := s.cancel
		s.startMu.Unlock()
		if cancel != nil {
			cancel()
		}
		if !started {
			s.finish()
		}
		s.bus.Unsubscribe(s.busSub)
	}
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.saveState()
	return nil
}

func (s *Scheduler) submit(c cmd) (any, bool) {
	select {
	case <-s.startGate:
	case <-s.done:
		return nil, false
	}
	select {
	case s.cmdCh <- c:
	case <-s.done:
		return nil, false
	}
	select {
	case value := <-c.respCh:
		return value, true
	case <-s.done:
		return nil, false
	}
}

func (s *Scheduler) finish() {
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *Scheduler) withPreStart(fn func()) bool {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.stopped.Load() {
		return true
	}
	if s.started {
		return false
	}
	fn()
	return true
}
