package filerepo

import (
	"sort"

	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func (h *IndexedHandler) reportMetadataState() {
	ready := h.hasAnyRootSnapshot()
	stateStr := "booting"
	if h.sh != nil {
		resources := h.sh.SnapshotResources()
		refreshing := false
		for _, item := range resources {
			if item.Refreshing {
				refreshing = true
				break
			}
		}
		switch {
		case refreshing && ready:
			stateStr = "refreshing"
		case refreshing:
			stateStr = "bootstrapping"
		case !ready:
			stateStr = "booting"
		case h.sh.AggregateState() == health.StateHealthy:
			stateStr = "ready"
		default:
			stateStr = "degraded"
		}
	} else if ready {
		stateStr = "ready"
	}
	h.stats.SetMetadataState(h.name, h.mode, stateStr, ready)
}

func (h *IndexedHandler) RepositoryStatuses() []runtime.RepositoryStatus {
	h.mu.RLock()
	statusesByID := make(map[string]runtime.RepositoryStatus, len(h.roots)+len(h.rootSnapshots))
	for rootID, entry := range h.roots {
		statusesByID[rootID] = runtime.RepositoryStatus{
			ID:              rootID,
			Path:            entry.root.Path,
			DisplayName:     entry.root.DisplayName,
			Layout:          entry.root.Layout,
			PrimaryMetadata: append([]string(nil), entry.root.PrimaryMetadata...),
			Attributes:      toRuntimeAttributes(entry.root.Attributes),
		}
	}
	for rootID, snapshot := range h.rootSnapshots {
		status := statusesByID[rootID]
		status.ID = rootID
		if status.DisplayName == "" {
			status.DisplayName = rootID
		}
		status.Generation = snapshot.Generation
		status.HasCurrent = true
		status.Published = snapshot.Published
		status.Upstream = snapshot.Upstream
		status.ArtifactCount = snapshot.ArtifactCount
		status.MetadataCount = len(snapshot.Metadata)
		if status.Path == "" {
			status.Path = snapshot.RootPath
		}
		statusesByID[rootID] = status
	}
	h.mu.RUnlock()

	if h.sh != nil {
		for _, resource := range h.sh.SnapshotResources() {
			status := statusesByID[resource.Path]
			status.ID = resource.Path
			if status.Path == "" {
				status.Path = resource.Path
			}
			if status.DisplayName == "" {
				status.DisplayName = status.Path
			}
			status.State = resource.State
			status.Refreshing = resource.Refreshing
			status.LastError = resource.LastError
			status.LastSuccessAt = resource.LastSuccessAt
			status.LastRefreshAt = resource.LastRefreshAt
			statusesByID[resource.Path] = status
		}
	}

	statuses := make([]runtime.RepositoryStatus, 0, len(statusesByID))
	for _, item := range statusesByID {
		statuses = append(statuses, item)
	}
	stateOrder := map[string]int{"active": 0, "suspect": 1, "blocked": 2, "pending": 3, "removed": 4, "": 5}
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].Refreshing != statuses[j].Refreshing {
			return statuses[i].Refreshing
		}
		if stateOrder[statuses[i].State] != stateOrder[statuses[j].State] {
			return stateOrder[statuses[i].State] < stateOrder[statuses[j].State]
		}
		if statuses[i].Path != statuses[j].Path {
			return statuses[i].Path < statuses[j].Path
		}
		return statuses[i].ID < statuses[j].ID
	})
	return statuses
}

func toRuntimeAttributes(attrs []RepositoryAttribute) []runtime.RepositoryAttribute {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]runtime.RepositoryAttribute, len(attrs))
	for i, attr := range attrs {
		out[i] = runtime.RepositoryAttribute{LabelKey: attr.LabelKey, Value: attr.Value}
	}
	return out
}
