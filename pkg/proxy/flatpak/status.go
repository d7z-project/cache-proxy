package flatpak

import (
	"io/fs"
	"path"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

// DashboardStatus reports aggregate Flatpak upstream health.
func (h *Handler) DashboardStatus() (color, label, extra string) {
	if h.sh == nil {
		return "", "", ""
	}
	return h.sh.DashboardStatus()
}

// RepositoryStatuses reports the published Flatpak metadata generation for the home page.
func (h *Handler) RepositoryStatuses() []proxyruntime.RepositoryStatus {
	current := h.currentSnapshot()
	status := proxyruntime.RepositoryStatus{
		ID:              "/",
		Path:            "/",
		DisplayName:     h.name,
		Layout:          "flatpak",
		PrimaryMetadata: []string{"summary"},
		HasCurrent:      current.Generation != "",
		Generation:      current.Generation,
		Published:       current.Published,
		Upstream:        current.Upstream,
		LastSuccessAt:   current.Published,
		LastRefreshAt:   current.Published,
	}
	if status.HasCurrent {
		status.State = "active"
		status.MetadataCount = h.currentMetadataCount(current.Generation)
	} else {
		status.State = "pending"
	}
	if h.sh != nil {
		if resource, ok := h.sh.ResourceHealth("/"); ok {
			status.State = resource.State.String()
			status.Refreshing = resource.Refreshing
			status.LastError = resource.LastError
			status.LastSuccessAt = resource.LastSuccessAt
			status.LastRefreshAt = resource.LastRefreshAt
		}
	}
	return []proxyruntime.RepositoryStatus{status}
}

func (h *Handler) currentMetadataCount(generation string) int {
	if generation == "" || h.store == nil {
		return 0
	}
	entries, err := fs.ReadDir(h.store.TenantFS(h.name), path.Join(metadataRoot, generation))
	if err != nil {
		return 0
	}
	var count int
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
		}
	}
	return count
}
