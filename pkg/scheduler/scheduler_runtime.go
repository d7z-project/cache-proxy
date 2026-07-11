package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
)

func (s *Scheduler) loop() {
	defer s.wg.Done()
	close(s.startGate)

	for _, def := range s.preStartTasks {
		s.registerLocked(def, "plan", time.Time{})
		s.updateHeap(def.Key)
	}
	s.restoreFromStore()
	s.refreshMetrics()

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
	case cmdRegisterFactory:
		s.factories[c.factory.Instance] = &c.factory
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
		refreshKey := NewTaskKey(p.Instance, TypeMetadataRefresh, p.RootID)
		if _, exists := s.tasks[refreshKey]; exists {
			return
		}
		now := time.Now()
		s.registerLocked(TaskDef{
			Key:      refreshKey,
			Interval: factory.RefreshInterval,
			Handler:  factory.NewRefresh(p.RootID),
		}, "discovery", now)
		s.registerLocked(TaskDef{
			Key:      NewTaskKey(p.Instance, TypeMetadataGC, p.RootID),
			Interval: factory.GCInterval,
			Handler:  factory.NewGC(p.RootID),
		}, "discovery", time.Time{})
		s.triggerLocked(refreshKey)
		s.updateHeap(NewTaskKey(p.Instance, TypeMetadataGC, p.RootID))
		s.refreshMetrics()
		s.saveState()
		slog.Debug("scheduler registered metadata tasks", "instance", p.Instance, "root_id", p.RootID)
	case bus.EventMetadataRemoved:
		p := evt.Payload.(bus.MetadataRemovedPayload)
		s.unregisterLocked(NewTaskKey(p.Instance, TypeMetadataRefresh, p.RootID), "removed")
		s.unregisterLocked(NewTaskKey(p.Instance, TypeMetadataGC, p.RootID), "removed")
		s.refreshMetrics()
		s.saveState()
		slog.Debug("scheduler removed metadata tasks", "instance", p.Instance, "root_id", p.RootID)
	}
}

func (s *Scheduler) processDue() {
	now := time.Now()
	for {
		if s.stopped.Load() {
			return
		}
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
	outcome, err := safeCall(ctx, ts.handler)
	dur := time.Since(start)

	if s.stopped.Load() {
		return
	}

	result := "success"
	var reasonCode, detail, message string
	if outcome != nil {
		result = outcome.Result
		reasonCode = outcome.ReasonCode
		detail = outcome.Detail
		message = outcome.Message
	}
	if result == "" {
		result = "success"
	}

	ts.LastRun = start
	ts.RunCount++
	if err != nil && !errors.Is(err, ErrTaskSkipped) {
		var retryAt RetryAtError
		if errors.As(err, &retryAt) {
			ts.LastError = ""
			ts.Status = StatusDone
			ts.NextRun = retryAt.At
			result = "delayed"
		} else {
			ts.LastError = taskErrorString(err)
			ts.ErrCount++
			ts.Status = StatusFailed
			retryAfter := backoff(ts.ErrCount, ts.Interval)
			ts.NextRun = time.Now().Add(retryAfter)
			switch {
			case ctx.Err() == context.DeadlineExceeded, errors.Is(err, context.DeadlineExceeded):
				result = "timeout"
			case errors.Is(err, errHandlerPanic):
				result = "panic"
			default:
				result = "failed"
			}
			slog.Info("scheduler task failed", "key", ts.Key.String(), "err", err, "duration", dur)
		}
	} else {
		ts.LastError = ""
		ts.Status = StatusDone
		ts.NextRun = time.Now().Add(ts.Interval)
		if errors.Is(err, ErrTaskSkipped) {
			result = "skipped"
		}
	}

	if ts.Key.Type() == TypeMetadataRefresh && !ts.firstRunDone {
		if !ts.discoveredAt.IsZero() && s.m != nil {
			s.m.discoveryToRefresh.WithLabelValues(ts.Key.Instance(), result).
				Observe(time.Since(ts.discoveredAt).Seconds())
		}
		ts.firstRunDone = true
		ts.discoveredAt = time.Time{}
	}
	if s.m != nil {
		s.m.runs.WithLabelValues(ts.Key.Instance(), string(ts.Key.Type()), result).Inc()
		s.m.duration.WithLabelValues(ts.Key.Instance(), string(ts.Key.Type()), result).Observe(dur.Seconds())
	}
	s.observerMu.RLock()
	runObserver := s.runObserver
	s.observerMu.RUnlock()
	if runObserver != nil {
		runObserver(TaskRun{
			Key:        ts.Key,
			StartedAt:  start,
			FinishedAt: start.Add(dur),
			Duration:   dur,
			Result:     result,
			ReasonCode: reasonCode,
			Detail:     detail,
			Message:    message,
			Err:        ts.LastError,
		})
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
	debug.FreeOSMemory()
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
		s.m.discoveryToRefresh.WithLabelValues(ts.Key.Instance(), "removed_before_refresh").
			Observe(time.Since(ts.discoveredAt).Seconds())
	}
	if s.m != nil {
		s.m.unregistered.WithLabelValues(key.Instance(), string(key.Type()), reason).Inc()
	}
	delete(s.tasks, key)

	instance := key.Instance()
	for k := range s.tasks {
		if k.Instance() == instance {
			return
		}
	}
	delete(s.metricInstances, instance)
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
		delaySeconds := clampDurationSeconds(ts.NextRun.Sub(now))
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
			key := [2]string{inst, string(typ)}
			s.m.active.WithLabelValues(inst, string(typ)).Set(active[key])
			s.m.nextDelay.WithLabelValues(inst, string(typ)).Set(nextDelay[key])
			s.m.overdue.WithLabelValues(inst, string(typ)).Set(overdue[key])
			s.m.backoff.WithLabelValues(inst, string(typ)).Set(backoffVals[key])
			for _, status := range []TaskStatus{StatusIdle, StatusRunning, StatusDone, StatusFailed} {
				s.m.status.WithLabelValues(inst, string(typ), string(status)).
					Set(statuses[[3]string{inst, string(typ), string(status)}])
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

func taskErrorString(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) <= maxTaskErrorBytes {
		return msg
	}
	return msg[:maxTaskErrorBytes] + "...(truncated)"
}

func safeCall(ctx context.Context, handler TaskHandler) (outcome *TaskOutcome, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errHandlerPanic, r)
			slog.Error("scheduler handler panicked", "panic", r)
		}
	}()
	return handler(ctx)
}
