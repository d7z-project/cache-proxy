package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	subPath  string
	str      string
}

func NewTaskKey(instance string, typ TaskType, subPath string) TaskKey {
	s := instance + ":" + string(typ) + ":" + subPath
	return TaskKey{instance: instance, typ: typ, subPath: subPath, str: s}
}

func (k TaskKey) String() string   { return k.str }
func (k TaskKey) Instance() string { return k.instance }
func (k TaskKey) Type() TaskType   { return k.typ }
func (k TaskKey) SubPath() string  { return k.subPath }

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

type TaskFactory struct {
	Instance        string
	RefreshInterval time.Duration
	GCInterval      time.Duration
	NewRefresh      func(subPath string) TaskHandler
	NewGC           func(subPath string) TaskHandler
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
	cmdUnregister
	cmdInfo
	cmdSnapshot
)

type cmd struct {
	kind   cmdKind
	def    TaskDef
	key    TaskKey
	respCh chan any
}

type Scheduler struct {
	cmdCh           chan cmd
	busSub          <-chan bus.Event
	factories       map[string]*TaskFactory
	metricInstances map[string]struct{}
	tasks           map[TaskKey]*taskState
	heap            taskHeap

	store   *blobfs.Store
	tenant  string
	storeMu sync.Mutex
	m       *metrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(b *bus.Bus, store *blobfs.Store, reg prometheus.Registerer) *Scheduler {
	return &Scheduler{
		cmdCh:           make(chan cmd),
		busSub:          b.Subscribe(bus.EventMetadataDiscovered, bus.EventMetadataRemoved),
		factories:       map[string]*TaskFactory{},
		metricInstances: map[string]struct{}{},
		tasks:           map[TaskKey]*taskState{},
		heap:            taskHeap{},
		store:           store,
		tenant:          "_scheduler",
		m:               newMetrics(reg),
	}
}

func (s *Scheduler) Register(def TaskDef) {
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdRegister, def: def, respCh: respCh}
	<-respCh
}

func (s *Scheduler) Unregister(key TaskKey) {
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdUnregister, key: key, respCh: respCh}
	<-respCh
}

func (s *Scheduler) RegisterFactory(factory TaskFactory) {
	// Synchronous: stored immediately so it's available for restoreFromStore
	s.factories[factory.Instance] = &factory
}

func (s *Scheduler) Info(key TaskKey) (TaskInfo, bool) {
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdInfo, key: key, respCh: respCh}
	result := (<-respCh).(TaskInfo)
	return result, result.Key.instance != ""
}

func (s *Scheduler) Snapshot() []TaskInfo {
	respCh := make(chan any, 1)
	s.cmdCh <- cmd{kind: cmdSnapshot, respCh: respCh}
	return (<-respCh).([]TaskInfo)
}

func (s *Scheduler) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go s.loop()
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
		return ctx.Err()
	}
	s.saveState()
	return nil
}

func (s *Scheduler) loop() {
	defer s.wg.Done()

	s.restoreFromStore()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case c := <-s.cmdCh:
			s.handleCmd(c)
		case evt := <-s.busSub:
			s.handleBusEvent(evt)
		case <-ticker.C:
			s.processDue()
		}
	}
}

func (s *Scheduler) handleCmd(c cmd) {
	switch c.kind {
	case cmdRegister:
		s.registerLocked(c.def, "plan", time.Time{})
		s.updateHeap(c.def.Key)
		s.refreshMetrics()
		c.respCh <- struct{}{}
	case cmdUnregister:
		s.unregisterLocked(c.key, "manual")
		s.refreshMetrics()
		s.saveState()
		c.respCh <- struct{}{}
	case cmdInfo:
		ts, ok := s.tasks[c.key]
		if ok {
			c.respCh <- ts.TaskInfo
		} else {
			c.respCh <- TaskInfo{}
		}
	case cmdSnapshot:
		infos := make([]TaskInfo, 0, len(s.tasks))
		for _, ts := range s.tasks {
			infos = append(infos, ts.TaskInfo)
		}
		c.respCh <- infos
	}
}

func (s *Scheduler) handleBusEvent(evt bus.Event) {
	switch evt.Type {
	case bus.EventMetadataDiscovered:
		p := evt.Payload.(bus.MetadataDiscoveredPayload)
		factory := s.factories[p.Instance]
		if factory == nil {
			return
		}
		refreshKey := NewTaskKey(p.Instance, TypeMetadataRefresh, p.SubPath)
		if _, exists := s.tasks[refreshKey]; exists {
			return
		}
		now := time.Now()
		s.registerLocked(TaskDef{
			Key:      refreshKey,
			Interval: factory.RefreshInterval,
			Handler:  factory.NewRefresh(p.SubPath),
		}, "discovery", now)
		s.registerLocked(TaskDef{
			Key:      NewTaskKey(p.Instance, TypeMetadataGC, p.SubPath),
			Interval: factory.GCInterval,
			Handler:  factory.NewGC(p.SubPath),
		}, "discovery", time.Time{})
		s.triggerLocked(refreshKey)
		// push gc task to heap too
		s.updateHeap(NewTaskKey(p.Instance, TypeMetadataGC, p.SubPath))
		s.refreshMetrics()
		s.saveState()
		slog.Debug("scheduler registered metadata tasks", "instance", p.Instance, "sub_path", p.SubPath)

	case bus.EventMetadataRemoved:
		p := evt.Payload.(bus.MetadataRemovedPayload)
		s.unregisterLocked(NewTaskKey(p.Instance, TypeMetadataRefresh, p.SubPath), "removed")
		s.unregisterLocked(NewTaskKey(p.Instance, TypeMetadataGC, p.SubPath), "removed")
		s.refreshMetrics()
		s.saveState()
		slog.Debug("scheduler removed metadata tasks", "instance", p.Instance, "sub_path", p.SubPath)
	}
}

func (s *Scheduler) processDue() {
	now := time.Now()
	for {
		ts := s.heapPeek()
		if ts == nil || ts.NextRun.After(now) {
			return
		}
		heap.Pop(&s.heap)
		s.execute(ts)
	}
}

func (s *Scheduler) execute(ts *taskState) {
	deadline := ts.Interval / 2
	if deadline < time.Minute {
		deadline = time.Minute
	}
	if deadline > 30*time.Minute {
		deadline = 30 * time.Minute
	}

	ctx, cancel := context.WithTimeout(s.ctx, deadline)
	defer cancel()

	ts.Status = StatusRunning
	start := time.Now()
	err := safeCall(ctx, ts.handler)
	dur := time.Since(start)
	result := "success"

	ts.LastRun = start
	ts.RunCount++
	if err != nil && !errors.Is(err, ErrTaskSkipped) {
		ts.LastError = err.Error()
		ts.ErrCount++
		ts.Status = StatusFailed
		retryAfter := backoff(ts.ErrCount, ts.Interval)
		ts.NextRun = time.Now().Add(retryAfter)
		if ctx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
			result = "timeout"
		} else if errors.Is(err, errHandlerPanic) {
			result = "panic"
		} else {
			result = "failed"
		}
		slog.Info("scheduler task failed", "key", ts.Key.String(), "err", err, "duration", dur)
	} else {
		ts.LastError = ""
		ts.Status = StatusDone
		ts.NextRun = time.Now().Add(ts.Interval)
		if errors.Is(err, ErrTaskSkipped) {
			result = "skipped"
		}
	}
	if ts.Key.Type() == TypeMetadataRefresh && !ts.firstRunDone {
		refreshResult := result
		if !ts.discoveredAt.IsZero() && s.m != nil {
			s.m.discoveryToRefresh.WithLabelValues(ts.Key.Instance(), refreshResult).Observe(time.Since(ts.discoveredAt).Seconds())
		}
		ts.firstRunDone = true
		ts.discoveredAt = time.Time{}
	}
	if s.m != nil {
		s.m.runs.WithLabelValues(ts.Key.Instance(), string(ts.Key.Type()), result).Inc()
		s.m.duration.WithLabelValues(ts.Key.Instance(), string(ts.Key.Type()), result).Observe(dur.Seconds())
	}

	if ts.Interval > 0 {
		if ts.index >= 0 {
			heap.Fix(&s.heap, ts.index)
		} else {
			heap.Push(&s.heap, ts)
		}
	}
	s.refreshMetrics()
	s.saveState()
}

func (s *Scheduler) registerLocked(def TaskDef, source string, discoveredAt time.Time) {
	if _, exists := s.tasks[def.Key]; exists {
		s.unregisterLocked(def.Key, "replaced")
	}
	ts := &taskState{
		TaskInfo: TaskInfo{
			Key:      def.Key,
			Status:   StatusIdle,
			NextRun:  time.Now().Add(def.Interval),
			Interval: def.Interval,
		},
		handler:      def.Handler,
		index:        -1,
		discoveredAt: discoveredAt,
	}
	s.tasks[def.Key] = ts
	s.metricInstances[def.Key.Instance()] = struct{}{}
	if s.m != nil {
		s.m.registered.WithLabelValues(def.Key.Instance(), string(def.Key.Type()), source).Inc()
	}
}

func (s *Scheduler) unregisterLocked(key TaskKey, reason string) {
	ts, ok := s.tasks[key]
	if !ok {
		return
	}
	if ts.index >= 0 {
		heap.Remove(&s.heap, ts.index)
	}
	if ts.Key.Type() == TypeMetadataRefresh && !ts.firstRunDone && !ts.discoveredAt.IsZero() && s.m != nil {
		s.m.discoveryToRefresh.WithLabelValues(ts.Key.Instance(), "removed_before_refresh").Observe(time.Since(ts.discoveredAt).Seconds())
	}
	if s.m != nil {
		s.m.unregistered.WithLabelValues(key.Instance(), string(key.Type()), reason).Inc()
	}
	delete(s.tasks, key)
}

func (s *Scheduler) triggerLocked(key TaskKey) {
	ts, ok := s.tasks[key]
	if !ok {
		return
	}
	ts.NextRun = time.Now()
	s.updateHeap(key)
}

func (s *Scheduler) updateHeap(key TaskKey) {
	ts, ok := s.tasks[key]
	if !ok {
		return
	}
	if ts.index >= 0 {
		heap.Fix(&s.heap, ts.index)
	} else {
		heap.Push(&s.heap, ts)
	}
}

func (s *Scheduler) heapPeek() *taskState {
	if s.heap.Len() == 0 {
		return nil
	}
	return s.heap[0]
}

func (s *Scheduler) refreshMetrics() {
	if s.m == nil {
		return
	}
	now := time.Now()
	active := map[[2]string]float64{}
	statuses := map[[3]string]float64{}
	pending := map[string]float64{}
	nextDelay := map[[2]string]float64{}
	overdue := map[[2]string]float64{}
	backoffVals := map[[2]string]float64{}
	for _, ts := range s.tasks {
		key := [2]string{ts.Key.Instance(), string(ts.Key.Type())}
		active[key]++
		statuses[[3]string{ts.Key.Instance(), string(ts.Key.Type()), string(ts.Status)}]++
		if ts.Key.Type() == TypeMetadataRefresh && !ts.firstRunDone && !ts.discoveredAt.IsZero() {
			pending[ts.Key.Instance()]++
		}
		delay := ts.NextRun.Sub(now)
		delaySeconds := clampDurationSeconds(delay)
		if current, ok := nextDelay[key]; !ok || delaySeconds < current {
			nextDelay[key] = delaySeconds
		}
		if now.After(ts.NextRun) {
			overdue[key] = max(overdue[key], now.Sub(ts.NextRun).Seconds())
			nextDelay[key] = 0
		}
		if ts.Status == StatusFailed && !ts.LastRun.IsZero() && ts.NextRun.After(ts.LastRun) {
			backoffVals[key] = max(backoffVals[key], ts.NextRun.Sub(ts.LastRun).Seconds())
		}
	}
	for inst := range s.metricInstances {
		for _, typ := range []TaskType{TypeBlobGC, TypeExpireCleanup, TypeMetadataRefresh, TypeMetadataGC} {
			k := [2]string{inst, string(typ)}
			s.m.active.WithLabelValues(inst, string(typ)).Set(active[k])
			s.m.nextDelay.WithLabelValues(inst, string(typ)).Set(nextDelay[k])
			s.m.overdue.WithLabelValues(inst, string(typ)).Set(overdue[k])
			s.m.backoff.WithLabelValues(inst, string(typ)).Set(backoffVals[k])
			for _, status := range []TaskStatus{StatusIdle, StatusRunning, StatusDone, StatusFailed} {
				s.m.status.WithLabelValues(inst, string(typ), string(status)).Set(statuses[[3]string{inst, string(typ), string(status)}])
			}
		}
		s.m.discoveriesPending.WithLabelValues(inst).Set(pending[inst])
	}
}

func backoff(failures uint64, interval time.Duration) time.Duration {
	d := time.Duration(failures) * interval / 8
	if d < time.Minute {
		d = time.Minute
	}
	if d > interval/2 {
		d = interval / 2
	}
	return d
}

func safeCall(ctx context.Context, handler TaskHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errHandlerPanic, r)
			slog.Error("scheduler handler panicked", "panic", r)
		}
	}()
	return handler(ctx)
}
