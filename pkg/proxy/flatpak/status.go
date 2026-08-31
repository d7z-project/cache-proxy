package flatpak

import proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"

// DashboardStatus reports aggregate Flatpak upstream health.
func (h *Handler) DashboardStatus() (color, label, extra string) {
	if h.serviceHealth == nil {
		return "", "", ""
	}
	return h.serviceHealth.DashboardStatus()
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
		status.MetadataCount = len(current.Manifest.Objects)
	} else {
		status.State = "pending"
	}
	h.mu.RLock()
	status.Refreshing = h.refreshing || h.refreshQueued
	status.LastError = h.lastError
	h.mu.RUnlock()
	if !status.HasCurrent && status.LastError != "" {
		status.State = "suspect"
	}
	return []proxyruntime.RepositoryStatus{status}
}
