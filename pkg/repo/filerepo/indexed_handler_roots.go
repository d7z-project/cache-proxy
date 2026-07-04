package filerepo

import (
	"context"
	"log/slog"
	"slices"
	"sort"
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

func (h *IndexedHandler) registerRoot(result DiscoveryResult) (string, bool, bool) {
	if result.Class != ResourceMetadata || result.Role == DiscoveryIgnore || result.Root.ID == "" {
		return "", false, false
	}

	rootID := result.Root.ID
	root := h.finalizeRoot(result.Root)
	created := false
	changed := false

	h.mu.Lock()
	entry, exists := h.roots[rootID]
	switch {
	case exists:
		changed = h.mergeAndFinalizeRoot(&entry.root, root)
	case result.Role == DiscoveryCreateRoot:
		h.roots[rootID] = &rootEntry{root: root}
		created = true
		changed = true
	}
	h.mu.Unlock()

	if !created && !changed {
		return rootID, false, false
	}
	if created {
		slog.Debug("discovered new repository root", "instance", h.name, "mode", h.mode, "root_id", rootID, "path", result.Root.Path)
		if h.sh != nil {
			h.sh.AddResource(rootID, targetsToProbe(result.Root.Targets), h.upstreams)
		}
	}
	h.saveState(context.Background())
	return rootID, created, changed
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
	if current.Layout == "" && next.Layout != "" {
		current.Layout = next.Layout
		changed = true
	}
	if mergeOrderedStrings(&current.PrimaryMetadata, next.PrimaryMetadata) {
		changed = true
	}
	if mergeTargets(&current.Targets, next.Targets) {
		changed = true
	}
	if current.Suite == "" && next.Suite != "" {
		current.Suite = next.Suite
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

func mergeOrderedStrings(dst *[]string, src []string) bool {
	changed := false
	for _, item := range src {
		if item == "" || slices.Contains(*dst, item) {
			continue
		}
		*dst = append(*dst, item)
		changed = true
	}
	return changed
}

func mergeTargets(dst *[]MetadataTarget, src []MetadataTarget) bool {
	changed := false
	for _, item := range src {
		if item.URL == "" {
			continue
		}
		matched := false
		for i := range *dst {
			current := &(*dst)[i]
			if current.URL != item.URL {
				continue
			}
			matched = true
			if mergeOrderedStrings(&current.Candidates, item.Candidates) {
				changed = true
			}
			if current.Repo == "" && item.Repo != "" {
				current.Repo = item.Repo
				changed = true
			}
			if current.Arch == "" && item.Arch != "" {
				current.Arch = item.Arch
				changed = true
			}
			break
		}
		if matched {
			continue
		}
		*dst = append(*dst, item)
		changed = true
	}
	return changed
}

func equalTargets(left, right []MetadataTarget) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].URL != right[i].URL ||
			!slices.Equal(left[i].Candidates, right[i].Candidates) ||
			left[i].Kind != right[i].Kind ||
			left[i].Repo != right[i].Repo ||
			left[i].Arch != right[i].Arch {
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
	root = h.finalizeRoot(root)
	h.mu.Lock()
	if entry, ok := h.roots[root.ID]; ok {
		h.mergeAndFinalizeRoot(&entry.root, root)
		h.mu.Unlock()
		return
	}
	h.roots[root.ID] = &rootEntry{root: root}
	h.mu.Unlock()
	if h.sh != nil {
		h.sh.AddResource(root.ID, targetsToProbe(root.Targets), h.upstreams)
	}
}

func (h *IndexedHandler) finalizeRoot(root RepositoryRoot) RepositoryRoot {
	if h == nil || h.finalizer == nil {
		return root
	}
	return h.finalizer.FinalizeRoot(root)
}

func (h *IndexedHandler) mergeAndFinalizeRoot(current *RepositoryRoot, next RepositoryRoot) bool {
	changed := mergeRepositoryRoot(current, next)
	finalized := h.finalizeRoot(*current)
	if equalRoots(*current, finalized) {
		return changed
	}
	*current = finalized
	return true
}

func equalRoots(left, right RepositoryRoot) bool {
	return left.ID == right.ID &&
		left.Path == right.Path &&
		left.DisplayName == right.DisplayName &&
		left.Layout == right.Layout &&
		left.Suite == right.Suite &&
		left.Source == right.Source &&
		left.Repo == right.Repo &&
		left.Arch == right.Arch &&
		slices.Equal(left.PrimaryMetadata, right.PrimaryMetadata) &&
		slices.Equal(left.Components, right.Components) &&
		slices.Equal(left.Architectures, right.Architectures) &&
		equalTargets(left.Targets, right.Targets) &&
		equalAttributes(left.Attributes, right.Attributes)
}
