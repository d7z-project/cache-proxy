package filerepo

import (
	"context"
	"errors"
	"log/slog"
	"path"
	"slices"
	"sort"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func (h *IndexedHandler) weightedUpstreams() []string {
	if h.sh == nil {
		return append([]string(nil), h.upstreams...)
	}
	weighted := h.sh.WeightedUpstreams(h.upstreams)
	upstreams := make([]string, 0, len(weighted))
	for _, item := range weighted {
		upstreams = append(upstreams, item.URL)
	}
	return upstreams
}

func (h *IndexedHandler) discoverRoot(cleanPath string) (string, bool) {
	if h.discover == nil {
		return "", false
	}
	result := h.discover.Discover(cleanPath)
	if !result.Matched || result.Role == DiscoveryIgnore || result.Root.ID == "" {
		return "", false
	}

	rootID := result.Root.ID
	created := false
	changed := false

	h.mu.Lock()
	entry, exists := h.roots[rootID]
	switch {
	case exists:
		changed = mergeRepositoryRoot(&entry.root, result.Root)
	case result.Role == DiscoveryCreateRoot:
		h.roots[rootID] = &rootEntry{root: result.Root}
		created = true
		changed = true
	}
	h.mu.Unlock()

	if !created && !changed {
		return rootID, false
	}
	if created {
		slog.Debug("discovered new repository root", "instance", h.name, "mode", h.mode, "root_id", rootID, "path", result.Root.Path)
		if h.sh != nil {
			h.sh.AddResource(rootID, targetsToProbe(result.Root.Targets), h.upstreams)
		}
	}
	h.saveState(context.Background())
	return rootID, created
}

func mergeRepositoryRoot(current *RepositoryRoot, next RepositoryRoot) bool {
	if current == nil || current.ID == "" || current.ID != next.ID {
		return false
	}
	changed := false
	if current.Path == "" && next.Path != "" {
		current.Path = next.Path
		changed = true
	}
	if current.DisplayName == "" && next.DisplayName != "" {
		current.DisplayName = next.DisplayName
		changed = true
	}
	if current.Kind == "" && next.Kind != "" {
		current.Kind = next.Kind
		changed = true
	}
	if len(next.PrimaryMetadata) > 0 && !equalStringSlices(current.PrimaryMetadata, next.PrimaryMetadata) {
		current.PrimaryMetadata = append([]string(nil), next.PrimaryMetadata...)
		changed = true
	}
	if len(next.Targets) > 0 && !equalTargets(current.Targets, next.Targets) {
		current.Targets = append([]MetadataTarget(nil), next.Targets...)
		changed = true
	}
	if current.Suite == "" && next.Suite != "" {
		current.Suite = next.Suite
		changed = true
	}
	if current.Branch == "" && next.Branch != "" {
		current.Branch = next.Branch
		changed = true
	}
	if current.Repo == "" && next.Repo != "" {
		current.Repo = next.Repo
		changed = true
	}
	if current.Arch == "" && next.Arch != "" {
		current.Arch = next.Arch
		changed = true
	}
	if mergeStringSet(&current.Components, next.Components) {
		changed = true
	}
	if mergeStringSet(&current.Architectures, next.Architectures) {
		changed = true
	}
	if next.Source && !current.Source {
		current.Source = true
		changed = true
	}
	if len(next.Attributes) > 0 && !equalAttributes(current.Attributes, next.Attributes) {
		current.Attributes = append([]RepositoryAttribute(nil), next.Attributes...)
		changed = true
	}
	return changed
}

func mergeStringSet(dst *[]string, src []string) bool {
	changed := false
	for _, item := range src {
		if item == "" || slices.Contains(*dst, item) {
			continue
		}
		*dst = append(*dst, item)
		changed = true
	}
	if changed {
		sort.Strings(*dst)
	}
	return changed
}

func equalStringSlices(left, right []string) bool {
	return slices.Equal(left, right)
}

func equalTargets(left, right []MetadataTarget) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].URL != right[i].URL || !equalStringSlices(left[i].Candidates, right[i].Candidates) || left[i].Kind != right[i].Kind || left[i].Repo != right[i].Repo || left[i].Arch != right[i].Arch {
			return false
		}
	}
	return true
}

func equalAttributes(left, right []RepositoryAttribute) bool {
	return slices.Equal(left, right)
}

func (h *IndexedHandler) AddRepository(root RepositoryRoot) {
	if root.ID == "" {
		return
	}
	h.mu.Lock()
	if entry, ok := h.roots[root.ID]; ok {
		mergeRepositoryRoot(&entry.root, root)
		h.mu.Unlock()
		return
	}
	h.roots[root.ID] = &rootEntry{root: root}
	h.mu.Unlock()
	if h.sh != nil {
		h.sh.AddResource(root.ID, targetsToProbe(root.Targets), h.upstreams)
	}
}

func (h *IndexedHandler) rebuildAggregateLocked() {
	if len(h.rootSnapshots) == 0 {
		h.snapshot = nil
		return
	}
	aggregate := &LiveSnapshot{Metadata: map[string]MetadataObject{}}
	for _, snapshot := range h.rootSnapshots {
		for itemPath, item := range snapshot.Metadata {
			aggregate.Metadata[itemPath] = item
		}
	}
	h.snapshot = aggregate
}

func (h *IndexedHandler) currentSnapshot() *LiveSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.snapshot
}

func (h *IndexedHandler) hasAnyRootSnapshot() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rootSnapshots) > 0
}

func (h *IndexedHandler) currentPreferredUpstream() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, snapshot := range h.rootSnapshots {
		if snapshot.Upstream != "" {
			return snapshot.Upstream
		}
	}
	return ""
}

func (h *IndexedHandler) currentGeneration(rootID string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if snapshot, ok := h.rootSnapshots[rootID]; ok {
		return snapshot.Generation
	}
	return ""
}

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

func targetsToProbe(targets []MetadataTarget) []health.ProbeTarget {
	probes := make([]health.ProbeTarget, 0, len(targets))
	for _, target := range targets {
		probes = append(probes, health.ProbeTarget{Path: target.URL})
	}
	return probes
}

func refreshHealthError(err error) error {
	var fetchErr MetadataFetchError
	switch {
	case errors.Is(err, errMetadataNotFound):
		return health.ErrResourceNotFound
	case errors.As(err, &fetchErr):
		return fetchErr.Err
	default:
		return err
	}
}

func cleanRequestPath(target string) string {
	cleanPath := strings.TrimPrefix(path.Clean("/"+target), "/")
	if cleanPath == "." {
		return ""
	}
	return cleanPath
}

func (h *IndexedHandler) generationMetadataPath(rootID, generation, cleanPath string) string {
	if rootID == "" {
		rootID = "unknown"
	}
	return metadataStorePath(h.objectRoot, rootID, generation, cleanPath)
}

func (h *IndexedHandler) currentPath(rootID string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "current.yaml")
}

func (h *IndexedHandler) snapshotPath(rootID, generation string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "snapshots", generation+".yaml")
}
