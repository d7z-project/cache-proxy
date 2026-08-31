package filerepo

import (
	"sort"

	"gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func (h *IndexedHandler) reportMetadataState() {
	ready := h.hasAnyRootSnapshot()
	h.mu.RLock()
	refreshing := false
	hasErrors := len(h.refreshErrors) > 0
	for _, active := range h.refreshing {
		refreshing = refreshing || active
	}
	h.mu.RUnlock()
	stateStr := "booting"
	switch {
	case refreshing && ready:
		stateStr = "refreshing"
	case refreshing:
		stateStr = "bootstrapping"
	case ready && hasErrors:
		stateStr = "degraded"
	case ready:
		stateStr = "ready"
	}
	h.stats.SetMetadataState(h.name, h.mode, stateStr, ready)
}

func (h *IndexedHandler) RepositoryStatuses() []runtime.RepositoryStatus {
	h.mu.RLock()
	statusesByID := make(map[string]runtime.RepositoryStatus, len(h.roots)+len(h.rootSnapshots))
	for rootID, entry := range h.roots {
		attributes := make([]runtime.RepositoryAttribute, len(entry.root.Attributes))
		for i, attribute := range entry.root.Attributes {
			attributes[i] = runtime.RepositoryAttribute{LabelKey: attribute.LabelKey, Value: attribute.Value}
		}
		statusesByID[rootID] = runtime.RepositoryStatus{
			ID:              rootID,
			Path:            entry.root.Path,
			DisplayName:     entry.root.DisplayName,
			Layout:          entry.root.Layout,
			PrimaryMetadata: append([]string(nil), entry.root.PrimaryMetadata...),
			Attributes:      attributes,
			State:           "pending",
			Refreshing:      h.refreshing[rootID],
			LastError:       h.refreshErrors[rootID],
			LastSuccessAt:   entry.lastValidatedAt,
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
		status.State = "active"
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

	statuses := make([]runtime.RepositoryStatus, 0, len(statusesByID))
	for _, item := range statusesByID {
		statuses = append(statuses, item)
	}
	stateOrder := map[string]int{"active": 0, "pending": 1, "retired": 2, "": 3}
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
