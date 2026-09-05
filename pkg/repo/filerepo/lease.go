package filerepo

import (
	"context"
	"errors"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"
)

// SnapshotLease pins the nearest current snapshot and its retained predecessors.
// Snapshots are immutable; callers must close the lease after reading them.
type SnapshotLease struct {
	Snapshots []*Snapshot
	manager   *GenerationManager
	once      sync.Once
}

func (h *GenerationManager) AcquireSnapshots(requestPath string) *SnapshotLease {
	h.mu.Lock()
	var selected *liveSnapshot
	for rootID, current := range h.current {
		if !h.retiring[rootID] && containsPath(current.snapshot.Root, requestPath) &&
			(selected == nil || len(current.snapshot.Root) > len(selected.snapshot.Root)) {
			selected = current
		}
	}
	if selected == nil {
		h.mu.Unlock()
		return nil
	}
	lease := &SnapshotLease{manager: h, Snapshots: []*Snapshot{selected.snapshot}}
	for _, previous := range h.retained[selected.snapshot.RootID] {
		lease.Snapshots = append(lease.Snapshots, previous.snapshot)
	}
	for _, snapshot := range lease.Snapshots {
		h.readers[snapshot.RootID+"\x00"+snapshot.CandidateID]++
	}
	h.mu.Unlock()
	h.markLastSeen(selected.snapshot.RootID, time.Now().UTC())
	return lease
}

func (l *SnapshotLease) OpenAnchor(ctx context.Context, snapshot *Snapshot) (*blobfs.ObjectReader, error) {
	for _, pinned := range l.Snapshots {
		if pinned == snapshot {
			anchor, ok := snapshot.byPath[snapshot.Anchor]
			if !ok {
				return nil, errors.New("snapshot anchor is missing")
			}
			return l.manager.config.Store.OpenObject(ctx, l.manager.config.Tenant, anchor.Key)
		}
	}
	return nil, errors.New("snapshot is not covered by lease")
}

func (l *SnapshotLease) Close() {
	l.once.Do(func() {
		l.manager.mu.Lock()
		defer l.manager.mu.Unlock()
		for _, snapshot := range l.Snapshots {
			key := snapshot.RootID + "\x00" + snapshot.CandidateID
			l.manager.readers[key]--
			if l.manager.readers[key] == 0 {
				delete(l.manager.readers, key)
			}
		}
	})
}
