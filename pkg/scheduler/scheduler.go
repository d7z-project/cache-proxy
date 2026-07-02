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

type TaskHandler func(ctx context.Context) error

var ErrTaskSkipped = errors.New("task skipped")

var errHandlerPanic = errors.New("handler panic")

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
	Err        string
}

type TaskFactory struct {
	Instance        string
	RefreshInterval time.Duration
	GCInterval      time.Duration
	NewRefresh      func(rootID string) TaskHandler
	NewGC           func(rootID string) TaskHandler
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
	wg     sync.WaitGroup
}

func New(b *bus.Bus, store *blobfs.Store, reg prometheus.Registerer) *Scheduler {
	return &Scheduler{
		cmdCh:           make(chan cmd, 16),
		bus:             b,
		busSub:          b.Subscribe(bus.EventMetadataDiscovered, bus.EventMetadataRemoved),
		startGate:       make(chan struct{}),
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
	if s.withPreStart(func() {
		s.preStartTasks[def.Key] = def
	}) {
		return
	}
	<-s.startGate
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdRegister, def: def, respCh: respCh}
	<-respCh
}

func (s *Scheduler) RegisterFactory(factory TaskFactory) {
	if s.withPreStart(func() {
		s.factories[factory.Instance] = &factory
	}) {
		return
	}
	<-s.startGate
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdRegisterFactory, factory: factory, respCh: respCh}
	<-respCh
}

func (s *Scheduler) Unregister(key TaskKey) {
	if s.withPreStart(func() {
		delete(s.preStartTasks, key)
	}) {
		return
	}
	<-s.startGate
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdUnregister, key: key, respCh: respCh}
	<-respCh
}

func (s *Scheduler) Info(key TaskKey) (TaskInfo, bool) {
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
	<-s.startGate
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdInfo, key: key, respCh: respCh}
	result := (<-respCh).(TaskInfo)
	return result, result.Key.instance != ""
}

func (s *Scheduler) Snapshot() []TaskInfo {
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
	<-s.startGate
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdSnapshot, respCh: respCh}
	return (<-respCh).([]TaskInfo)
}

func (s *Scheduler) Start(ctx context.Context) {
	s.startMu.Lock()
	if s.started {
		s.startMu.Unlock()
		return
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.startMu.Unlock()
	s.wg.Add(1)
	go s.loop()
}

func (s *Scheduler) SetRunObserver(fn func(TaskRun)) {
	s.observerMu.Lock()
	s.runObserver = fn
	s.observerMu.Unlock()
}

func (s *Scheduler) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.stopped.Store(true)
		s.bus.Unsubscribe(s.busSub)
		return ctx.Err()
	}
	s.bus.Unsubscribe(s.busSub)
	s.saveState()
	return nil
}

func (s *Scheduler) withPreStart(fn func()) bool {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.started {
		return false
	}
	fn()
	return true
}
