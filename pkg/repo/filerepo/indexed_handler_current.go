package filerepo

import (
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type currentViewEntry struct {
	RootID            string
	Generation        string
	Class             ResourceClass
	StorePath         string
	StatusCode        int
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
		rootPath := strings.Trim(strings.TrimSpace(snapshot.RootPath), "/")
		if rootPath != "" && cleanPath != rootPath && !strings.HasPrefix(cleanPath, rootPath+"/") {
			continue
		}
		rank := len(rootPath)
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
	type rankedEntry struct {
		currentViewEntry
		rootPathLength int
		published      time.Time
	}
	ranked := make(map[string]rankedEntry)
	for rootID, snapshot := range h.rootSnapshots {
		if snapshot == nil {
			continue
		}
		for cleanPath, item := range snapshot.Metadata {
			rootPathLength := len(strings.Trim(strings.TrimSpace(snapshot.RootPath), "/"))
			if current, ok := ranked[cleanPath]; ok {
				if rootPathLength < current.rootPathLength ||
					rootPathLength == current.rootPathLength && snapshot.Published.Before(current.published) ||
					rootPathLength == current.rootPathLength && snapshot.Published.Equal(current.published) && rootID > current.RootID {
					continue
				}
			}
			resolved := item
			if item.Path != "" && item.Path != cleanPath {
				actual, ok := snapshot.Metadata[item.Path]
				if !ok {
					continue
				}
				resolved = actual
			}
			storePath := ""
			if resolved.State == MetadataPresent {
				storePath = h.generationMetadataPath(snapshot.RootID, snapshot.Generation, resolved.Path)
			}
			ranked[cleanPath] = rankedEntry{
				currentViewEntry: currentViewEntry{
					RootID:            rootID,
					Generation:        snapshot.Generation,
					Class:             ResourceMetadata,
					StorePath:         storePath,
					StatusCode:        metadataStateStatus(item.State),
					PreferredUpstream: snapshot.Upstream,
				},
				rootPathLength: rootPathLength,
				published:      snapshot.Published,
			}
		}
	}
	view := make(map[string]currentViewEntry, len(ranked))
	for cleanPath, entry := range ranked {
		view[cleanPath] = entry.currentViewEntry
	}
	h.currentView = view
}

func (h *IndexedHandler) setRootSnapshot(rootID string, snapshot *LiveSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.setRootSnapshotLocked(rootID, snapshot)
}

func (h *IndexedHandler) setRootSnapshotLocked(rootID string, snapshot *LiveSnapshot) {
	if h.currentView == nil {
		h.currentView = map[string]currentViewEntry{}
	}
	if snapshot == nil {
		delete(h.rootSnapshots, rootID)
		h.rebuildCurrentViewLocked()
		return
	}
	h.rootSnapshots[rootID] = snapshot
	if entry, ok := h.roots[rootID]; ok {
		entry.retirementCleanupGeneration = ""
		entry.retirementCleanupDigest = ""
		entry.lastValidatedAt = snapshot.Published
		if len(entry.root.Targets) == 0 && len(snapshot.Targets) > 0 {
			entry.root.Targets = append([]MetadataTarget(nil), snapshot.Targets...)
			entry.closureRevision++
		}
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
	if current.StatusCode != 0 {
		release()
		w.Header().Set("X-Cache", "GENERATION")
		w.Header().Set("X-Cache-Generation", current.Generation)
		w.WriteHeader(current.StatusCode)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", current.StatusCode, 0)
		return
	}
	reader, err := h.store.OpenObject(req.Context(), h.name, current.StorePath)
	if err != nil {
		release()
		_ = httpcache.ErrorResponse(http.StatusInternalServerError, err).FlushClose(req, w)
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
	headers["X-Cache-Generation"] = current.Generation
	result := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: &generationReadCloser{ReadCloser: reader, release: release}}
	_ = result.FlushClose(req, w)
	h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusOK, uint64(size))
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
