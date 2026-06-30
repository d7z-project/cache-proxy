package scheduler

import (
	"bytes"
	"container/heap"
	"context"
	"log/slog"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"
)

type persistedTask struct {
	Instance  string        `yaml:"instance"`
	Type      string        `yaml:"type"`
	SubPath   string        `yaml:"sub_path"`
	Interval  time.Duration `yaml:"interval_ns"`
	NextRun   time.Time     `yaml:"next_run,omitempty"`
	LastRun   time.Time     `yaml:"last_run,omitempty"`
	RunCount  uint64        `yaml:"run_count"`
	ErrCount  uint64        `yaml:"err_count"`
	LastError string        `yaml:"last_error,omitempty"`
}

type persistedState struct {
	Version int             `yaml:"version"`
	Tasks   []persistedTask `yaml:"tasks"`
}

func (s *Scheduler) saveState() {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	s.saveStateLocked()
}

func (s *Scheduler) saveAllState() {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	s.saveStateLocked()
}

func (s *Scheduler) saveStateLocked() {
	if s.store == nil {
		return
	}
	state := persistedState{Version: 1}
	for _, ts := range s.tasks {
		if ts.Key.Type() != TypeMetadataRefresh && ts.Key.Type() != TypeMetadataGC {
			continue
		}
		state.Tasks = append(state.Tasks, persistedTask{
			Instance:  ts.Key.Instance(),
			Type:      string(ts.Key.Type()),
			SubPath:   ts.Key.SubPath(),
			Interval:  ts.Interval,
			NextRun:   ts.NextRun,
			LastRun:   ts.LastRun,
			RunCount:  ts.RunCount,
			ErrCount:  ts.ErrCount,
			LastError: ts.LastError,
		})
	}

	var buf bytes.Buffer
	if err := yaml.NewEncoder(&buf).Encode(state); err != nil {
		slog.Warn("scheduler state marshal failed", "err", err)
		return
	}
	if err := s.store.MkdirAll(s.tenant+"/", 0o755); err != nil {
		slog.Warn("scheduler state mkdir failed", "err", err)
		return
	}
	if _, err := s.store.Put(context.Background(), s.tenant, "tasks.yaml", bytes.NewReader(buf.Bytes()), nil); err != nil {
		slog.Warn("scheduler state write failed", "err", err)
	}
}

func (s *Scheduler) restoreFromStore() {
	data := loadTaskState(s.store, s.tenant)
	for _, pt := range data {
		factory := s.factories[pt.Instance]
		if factory == nil {
			continue
		}
		var handler TaskHandler
		switch TaskType(pt.Type) {
		case TypeMetadataRefresh:
			handler = factory.NewRefresh(pt.SubPath)
		case TypeMetadataGC:
			handler = factory.NewGC(pt.SubPath)
		default:
			continue
		}

		key := NewTaskKey(pt.Instance, TaskType(pt.Type), pt.SubPath)
		ts := &taskState{
			TaskInfo: TaskInfo{
				Key:       key,
				Status:    StatusIdle,
				NextRun:   pt.NextRun,
				LastRun:   pt.LastRun,
				LastError: pt.LastError,
				RunCount:  pt.RunCount,
				ErrCount:  pt.ErrCount,
				Interval:  pt.Interval,
			},
			handler: handler,
		}
		s.tasks[key] = ts
		heap.Push(&s.heap, ts)
	}
	if len(data) > 0 {
		slog.Debug("scheduler restored tasks", "count", len(data))
	}
}

func loadTaskState(store *blobfs.Store, tenant string) []persistedTask {
	if store == nil {
		return nil
	}
	reader, err := store.OpenObject(context.Background(), tenant, "tasks.yaml")
	if err != nil {
		return nil
	}
	defer reader.Close()

	var state persistedState
	if err := yaml.NewDecoder(reader).Decode(&state); err != nil {
		return nil
	}
	return state.Tasks
}
