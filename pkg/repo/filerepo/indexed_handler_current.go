package filerepo

import (
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

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

type generationReadCloser struct {
	io.ReadCloser
	release func()
}

func (r *generationReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.release()
	return err
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

func (h *IndexedHandler) lookupCurrentRequest(cleanPath string) (currentViewEntry, func(), bool) {
	h.mu.Lock()
	current, ok := h.currentView[cleanPath]
	if !ok || current.Class != ResourceMetadata {
		h.mu.Unlock()
		return current, func() {}, ok
	}
	key := current.RootID + "\x00" + current.Generation
	h.metadataReaders[key]++
	h.mu.Unlock()
	return current, h.metadataReaderRelease(key), true
}

func (h *IndexedHandler) lookupCurrentContent(cleanPath string, class ResourceClass) (currentViewEntry, bool) {
	if class != ResourceArtifact && class != ResourceSidecar {
		return currentViewEntry{}, false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	var (
		selectedRoot string
		selected     *LiveSnapshot
		selectedRank int
	)
	for rootID, snapshot := range h.rootSnapshots {
		if snapshot == nil {
			continue
		}
		rank := 0
		rootPath := strings.Trim(strings.TrimSpace(snapshot.RootPath), "/")
		if rootPath != "" && (cleanPath == rootPath || strings.HasPrefix(cleanPath, rootPath+"/")) {
			rank = len(rootPath)
		}
		if selected == nil ||
			rank > selectedRank ||
			(rank == selectedRank && snapshot.Published.After(selected.Published)) ||
			(rank == selectedRank && snapshot.Published.Equal(selected.Published) && rootID < selectedRoot) {
			selectedRoot = rootID
			selected = snapshot
			selectedRank = rank
		}
	}
	if selected == nil {
		return currentViewEntry{}, false
	}
	return currentViewEntry{
		RootID:            selectedRoot,
		Generation:        selected.Generation,
		Class:             class,
		PreferredUpstream: selected.Upstream,
	}, true
}

func (h *IndexedHandler) currentGeneration(rootID string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if snapshot, ok := h.rootSnapshots[rootID]; ok {
		return snapshot.Generation
	}
	return ""
}

func (h *IndexedHandler) matchRepository(cleanPath string) (string, *LiveSnapshot, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	selectedID := ""
	selectedPathLength := -1
	consider := func(rootID, rootPath string) {
		rootPath = strings.Trim(strings.TrimSpace(rootPath), "/")
		if rootPath != "" && cleanPath != rootPath && !strings.HasPrefix(cleanPath, rootPath+"/") {
			return
		}
		if len(rootPath) > selectedPathLength || len(rootPath) == selectedPathLength && (selectedID == "" || rootID < selectedID) {
			selectedID = rootID
			selectedPathLength = len(rootPath)
		}
	}
	for rootID, entry := range h.roots {
		if entry == nil {
			continue
		}
		consider(rootID, entry.root.Path)
	}
	for rootID, snapshot := range h.rootSnapshots {
		if snapshot != nil {
			consider(rootID, snapshot.RootPath)
		}
	}
	if selectedID == "" {
		return "", nil, false
	}
	return selectedID, h.rootSnapshots[selectedID], true
}

func (h *IndexedHandler) metadataReaderRelease(key string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			h.metadataReaders[key]--
			if h.metadataReaders[key] == 0 {
				delete(h.metadataReaders, key)
			}
			h.mu.Unlock()
		})
	}
}

func (h *IndexedHandler) metadataGenerationInUse(rootID, generation string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.metadataReaders[rootID+"\x00"+generation] > 0
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
	}
	h.currentView = view
}

func (h *IndexedHandler) setRootSnapshot(rootID string, snapshot *LiveSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.currentView == nil {
		h.currentView = map[string]currentViewEntry{}
	}
	if snapshot == nil {
		delete(h.rootSnapshots, rootID)
		h.rebuildCurrentViewLocked()
		return
	}
	h.rootSnapshots[rootID] = snapshot
	if entry, ok := h.roots[rootID]; ok && len(entry.root.Targets) == 0 && len(snapshot.Targets) > 0 {
		entry.root.Targets = append([]MetadataTarget(nil), snapshot.Targets...)
	}
	h.rebuildCurrentViewLocked()
}

func (h *IndexedHandler) removeRoot(rootID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.roots, rootID)
	delete(h.rootSnapshots, rootID)
	h.rebuildCurrentViewLocked()
}

func (h *IndexedHandler) serveCurrentMetadata(w http.ResponseWriter, req *http.Request, current currentViewEntry, release func()) {
	reader, err := h.store.OpenObject(req.Context(), h.name, current.StorePath)
	if err != nil {
		release()
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
	result := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: &generationReadCloser{ReadCloser: reader, release: release}}
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

func (h *IndexedHandler) contentPath(class ResourceClass, cleanPath string) string {
	kind := "sidecars"
	if class == ResourceArtifact {
		kind = "artifacts"
	}
	return path.Join(h.objectRoot, ".content", kind, cleanPath)
}

func (h *IndexedHandler) cleanupIndexPath(rootID, generation string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "generations", generation, "cleanup", "paths.txt")
}

func (h *IndexedHandler) currentPath(rootID string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "current.yaml")
}

func (h *IndexedHandler) snapshotPath(rootID, generation string) string {
	return path.Join(h.objectRoot, ".roots", pathEscapeKey(rootID), "snapshots", generation+".yaml")
}
