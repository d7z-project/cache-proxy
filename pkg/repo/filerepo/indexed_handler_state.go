package filerepo

import (
	"errors"
	"log/slog"
	"path"
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

func (h *IndexedHandler) discoverSubPath(cleanPath string) (subPath string, discovered bool) {
	if h.discover == nil {
		return "", false
	}
	spec, ok := h.discover.Discover(cleanPath)
	if !ok {
		return "", false
	}
	key := spec.Key()
	subPath = spec.SubPath()
	newTargets := spec.Targets()

	h.mu.Lock()
	entry, exists := h.roots[subPath]
	if exists {
		changed := false
		if entry.spec != nil {
			changed = entry.spec.Merge(spec)
		} else {
			entry.spec = spec
			changed = true
		}
		if changed {
			entry.targets = entry.spec.Targets()
		}
		h.mu.Unlock()
		return subPath, true
	}
	h.roots[subPath] = &rootEntry{spec: spec, targets: newTargets}
	h.mu.Unlock()

	slog.Debug("discovered new root", "instance", h.name, "mode", h.mode, "root", key, "subPath", subPath)
	if h.sh != nil {
		h.sh.AddResource(subPath, targetsToProbe(newTargets), h.upstreams)
	}
	return subPath, true
}

func (h *IndexedHandler) AddRoot(rootKey string, targets []MetadataTarget) {
	h.mu.Lock()
	if entry, ok := h.roots[rootKey]; ok {
		if len(targets) > 0 {
			entry.targets = targets
		}
		h.mu.Unlock()
		return
	}
	h.roots[rootKey] = &rootEntry{targets: targets}
	h.mu.Unlock()
	if h.sh != nil {
		h.sh.AddResource(rootKey, targetsToProbe(targets), h.upstreams)
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

func (h *IndexedHandler) currentGeneration(subPath string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if snapshot, ok := h.rootSnapshots[subPath]; ok {
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

func (h *IndexedHandler) RootReleases() []runtime.RootRelease {
	h.mu.RLock()
	releasesByKey := make(map[string]runtime.RootRelease, len(h.roots)+len(h.rootSnapshots))
	for rootKey := range h.roots {
		releasesByKey[rootKey] = runtime.RootRelease{Key: rootKey}
	}
	for rootKey, snapshot := range h.rootSnapshots {
		releasesByKey[rootKey] = runtime.RootRelease{
			Key:           rootKey,
			Generation:    snapshot.Generation,
			HasCurrent:    true,
			Published:     snapshot.Published,
			Upstream:      snapshot.Upstream,
			ArtifactCount: snapshot.ArtifactCount,
			MetadataCount: len(snapshot.Metadata),
		}
	}
	h.mu.RUnlock()

	if h.sh != nil {
		for _, resource := range h.sh.SnapshotResources() {
			release := releasesByKey[resource.Path]
			release.Key = resource.Path
			release.State = resource.State
			release.Refreshing = resource.Refreshing
			release.LastError = resource.LastError
			release.LastSuccessAt = resource.LastSuccessAt
			release.LastRefreshAt = resource.LastRefreshAt
			releasesByKey[resource.Path] = release
		}
	}

	releases := make([]runtime.RootRelease, 0, len(releasesByKey))
	for _, item := range releasesByKey {
		releases = append(releases, item)
	}
	stateOrder := map[string]int{"active": 0, "suspect": 1, "blocked": 2, "pending": 3, "removed": 4, "": 5}
	sort.Slice(releases, func(i, j int) bool {
		if releases[i].Refreshing != releases[j].Refreshing {
			return releases[i].Refreshing
		}
		if stateOrder[releases[i].State] != stateOrder[releases[j].State] {
			return stateOrder[releases[i].State] < stateOrder[releases[j].State]
		}
		return releases[i].Key < releases[j].Key
	})
	return releases
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

func (h *IndexedHandler) generationMetadataPath(rootKey, generation, cleanPath string) string {
	if rootKey == "" {
		rootKey = "unknown"
	}
	return metadataStorePath(h.objectRoot, rootKey, generation, cleanPath)
}

func (h *IndexedHandler) currentPath(rootKey string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootKey), "current.yaml")
}

func (h *IndexedHandler) snapshotPath(rootKey, generation string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootKey), "snapshots", generation+".yaml")
}
