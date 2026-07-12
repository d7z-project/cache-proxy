package scheduler

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"
)

type persistedTask struct {
	Instance  string        `yaml:"instance"`
	Type      string        `yaml:"type"`
	RootID    string        `yaml:"root_id"`
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

func (s *Scheduler) saveStateLocked() {
	if s.store == nil {
		if s.m != nil {
			s.m.stateSaves.WithLabelValues("success").Inc()
		}
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
			RootID:    ts.Key.RootID(),
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
		if s.m != nil {
			s.m.stateSaves.WithLabelValues("failed").Inc()
		}
		return
	}
	if err := s.store.MkdirAll(s.tenant+"/", 0o755); err != nil {
		slog.Warn("scheduler state mkdir failed", "err", err)
		if s.m != nil {
			s.m.stateSaves.WithLabelValues("failed").Inc()
		}
		return
	}
	tmpPath := fmt.Sprintf("tasks.yaml.tmp.%d", time.Now().UnixNano())
	if _, err := s.store.Put(context.Background(), s.tenant, tmpPath, bytes.NewReader(buf.Bytes()), nil); err != nil {
		slog.Warn("scheduler state write failed", "err", err)
		if s.m != nil {
			s.m.stateSaves.WithLabelValues("failed").Inc()
		}
		return
	}
	if err := s.store.Rename(path.Join(s.tenant, tmpPath), path.Join(s.tenant, "tasks.yaml")); err != nil {
		slog.Warn("scheduler state publish failed", "err", err)
		if cleanupErr := s.store.DeleteObject(context.Background(), s.tenant, tmpPath); cleanupErr != nil {
			slog.Debug("scheduler state temp cleanup failed", "path", tmpPath, "err", cleanupErr)
		}
		if s.m != nil {
			s.m.stateSaves.WithLabelValues("failed").Inc()
		}
		return
	}
	s.cleanStateTemps()
	if s.m != nil {
		s.m.stateSaves.WithLabelValues("success").Inc()
	}
}

func (s *Scheduler) restoreFromStore() {
	data, err := loadTaskState(s.store, s.tenant)
	if err != nil {
		slog.Warn("scheduler state restore failed", "err", err)
	}
	if s.m != nil {
		switch {
		case err != nil:
			s.m.stateRestore.WithLabelValues("failed").Inc()
		case len(data) == 0:
			s.m.stateRestore.WithLabelValues("empty").Inc()
		default:
			s.m.stateRestore.WithLabelValues("success").Inc()
		}
	}
	for _, pt := range data {
		factory := s.factories[pt.Instance]
		if factory == nil {
			if s.m != nil {
				s.m.restoreSkipped.WithLabelValues(pt.Type, "factory_missing").Inc()
			}
			continue
		}
		var handler TaskHandler
		switch TaskType(pt.Type) {
		case TypeMetadataRefresh:
			handler = factory.NewRefresh(pt.RootID)
		case TypeMetadataGC:
			handler = factory.NewGC(pt.RootID)
		default:
			if s.m != nil {
				s.m.restoreSkipped.WithLabelValues(pt.Type, "unknown_type").Inc()
			}
			continue
		}

		key := NewTaskKey(pt.Instance, TaskType(pt.Type), pt.RootID)
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
		s.metricInstances[key.Instance()] = struct{}{}
		if s.m != nil {
			s.m.restoredTasks.WithLabelValues(pt.Type).Inc()
		}
		heap.Push(&s.heap, ts)
	}
	s.refreshMetrics()
	if len(data) > 0 {
		slog.Debug("scheduler restored tasks", "count", len(data))
	}
	s.cleanStateTemps()
}

func loadTaskState(store *blobfs.Store, tenant string) ([]persistedTask, error) {
	if store == nil {
		return nil, nil
	}
	reader, err := store.OpenObject(context.Background(), tenant, "tasks.yaml")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open scheduler state: %w", err)
	}
	defer reader.Close()

	var state persistedState
	if err := yaml.NewDecoder(reader).Decode(&state); err != nil {
		return nil, fmt.Errorf("decode scheduler state: %w", err)
	}
	return state.Tasks, nil
}

func (s *Scheduler) cleanStateTemps() {
	if s.store == nil {
		return
	}
	err := fs.WalkDir(s.store.TenantFS(s.tenant), ".", func(objectPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasPrefix(path.Base(objectPath), "tasks.yaml.tmp.") {
			return nil
		}
		if err := s.store.DeleteObject(context.Background(), s.tenant, objectPath); err != nil {
			slog.Debug("scheduler state temp cleanup failed", "path", objectPath, "err", err)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Debug("scheduler state temp scan failed", "err", err)
	}
}
