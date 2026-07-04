package filerepo

import (
	"errors"
	"net/http"
	"path"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type currentViewEntry struct {
	RootID            string
	Generation        string
	Class             ResourceClass
	StorePath         string
	PreferredUpstream string
}

func (h *IndexedHandler) hasAnyRootSnapshot() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rootSnapshots) > 0
}

func (h *IndexedHandler) lookupCurrent(cleanPath string) (currentViewEntry, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	current, ok := h.currentView[cleanPath]
	return current, ok
}

func (h *IndexedHandler) currentGeneration(rootID string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if snapshot, ok := h.rootSnapshots[rootID]; ok {
		return snapshot.Generation
	}
	return ""
}

func (h *IndexedHandler) rebuildCurrentViewLocked() {
	view := make(map[string]currentViewEntry)
	for rootID, snapshot := range h.rootSnapshots {
		if snapshot == nil {
			continue
		}
		for cleanPath, item := range snapshot.Metadata {
			resolved := item
			if item.Path != "" && item.Path != cleanPath {
				actual, ok := snapshot.Metadata[item.Path]
				if !ok {
					continue
				}
				resolved = actual
			}
			storePath := resolved.StorePath
			if storePath == "" {
				storePath = h.generationMetadataPath(snapshot.RootID, snapshot.Generation, resolved.Path)
			}
			view[cleanPath] = currentViewEntry{
				RootID:            rootID,
				Generation:        snapshot.Generation,
				Class:             ResourceMetadata,
				StorePath:         storePath,
				PreferredUpstream: snapshot.Upstream,
			}
		}
		for _, cleanPath := range h.rootManagedPaths[rootID] {
			class := h.inspect(cleanPath).Class
			if class != ResourceArtifact && class != ResourceAuxiliary {
				class = ResourceAuxiliary
			}
			view[cleanPath] = currentViewEntry{
				RootID:            rootID,
				Generation:        snapshot.Generation,
				Class:             class,
				PreferredUpstream: snapshot.Upstream,
			}
		}
	}
	h.currentView = view
}

func (h *IndexedHandler) setRootSnapshot(rootID string, snapshot *LiveSnapshot, managedPaths []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rootManagedPaths == nil {
		h.rootManagedPaths = map[string][]string{}
	}
	if h.currentView == nil {
		h.currentView = map[string]currentViewEntry{}
	}
	if snapshot == nil {
		delete(h.rootSnapshots, rootID)
		delete(h.rootManagedPaths, rootID)
		h.rebuildCurrentViewLocked()
		return
	}
	h.rootSnapshots[rootID] = snapshot
	if managedPaths == nil {
		delete(h.rootManagedPaths, rootID)
	} else {
		h.rootManagedPaths[rootID] = append([]string(nil), managedPaths...)
	}
	if entry, ok := h.roots[rootID]; ok && len(entry.root.Targets) == 0 && len(snapshot.Targets) > 0 {
		entry.root.Targets = append([]MetadataTarget(nil), snapshot.Targets...)
	}
	h.rebuildCurrentViewLocked()
}

func (h *IndexedHandler) serveCurrentMetadata(w http.ResponseWriter, req *http.Request, current currentViewEntry) {
	reader, err := h.store.OpenObject(req.Context(), h.name, current.StorePath)
	if err != nil {
		httpcache.ErrorResponse(http.StatusInternalServerError, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusInternalServerError, 0)
		return
	}
	size := reader.Info().Size
	headers := map[string]string{
		"Content-Length": strconv.FormatInt(size, 10),
		"X-Cache":        "GENERATION",
	}
	for key, value := range reader.Info().Options {
		headers[httpcache.HeaderName(key)] = value
	}
	httpcache.StripInternal(headers)
	result := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}
	result.FlushClose(req, w)
	h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusOK, uint64(size))
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

func (h *IndexedHandler) generationContentPath(rootID, generation string, class ResourceClass, cleanPath string) string {
	kind := "auxiliary"
	if class == ResourceArtifact {
		kind = "artifacts"
	}
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "generations", generation, kind, cleanPath)
}

func (h *IndexedHandler) currentPath(rootID string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "current.yaml")
}

func (h *IndexedHandler) snapshotPath(rootID, generation string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "snapshots", generation+".yaml")
}
