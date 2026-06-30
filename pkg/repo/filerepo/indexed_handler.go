package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxMetadataObjectSize = 512 << 20

type rootEntry struct {
	spec    RootSpec
	targets []MetadataTarget
}

type IndexedHandler struct {
	name       string
	mode       string
	objectRoot string
	store      *blobfs.Store
	stats      *httpcache.Stats
	classifier func(string) ResourceClass
	base       *httpcache.Handler
	client     *utils.HttpClientWrapper
	upstreams  []string
	discover   Discoverer
	build      SnapshotBuilder
	sh         *health.ServiceHealth
	bus        *bus.Bus

	mu            sync.RWMutex
	snapshot      *LiveSnapshot
	roots         map[string]*rootEntry
	rootSnapshots map[string]*LiveSnapshot
	lifecycleCtx  context.Context
	wait          sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, discover Discoverer, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats, svcHealth *health.ServiceHealth, downloads *httpcache.DownloadLimiter) *IndexedHandler {
	ApplyDefaults(policy)
	handler := &IndexedHandler{
		name:          name,
		mode:          mode,
		objectRoot:    objectRoot,
		store:         store,
		stats:         stats,
		classifier:    classifier,
		upstreams:     append([]string(nil), upstreams...),
		discover:      discover,
		build:         builder,
		sh:            svcHealth,
		roots:         map[string]*rootEntry{},
		rootSnapshots: map[string]*LiveSnapshot{},
	}
	handler.base = httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:            mode,
		ExpireAfter:     expireAfter,
		Upstreams:       append([]string(nil), upstreams...),
		Transport:       transport,
		PassHeaders:     append([]string(nil), policy.PassHeaders...),
		BusyPolicy:      policy.AuxiliaryBusyPolicy,
		MetadataFunc:    handler.extraObjectMetadata,
		VerifyFunc:      handler.verifyObject,
		DownloadLimiter: downloads,
	}, store, &generationResolver{handler: handler, policy: policy}, stats, svcHealth)
	handler.client = utils.DefaultHttpClientWrapper()
	httpcache.ConfigureClientTransport(handler.client, name, mode, transport)
	handler.reportMetadataState()
	return handler
}

func (h *IndexedHandler) SetBus(b *bus.Bus) { h.bus = b }

func (h *IndexedHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := cleanRequestPath(req.URL.Path)
	if cleanPath == "" {
		h.base.ProxyPassthrough(w, req, "", "")
		return
	}
	class := h.classify(cleanPath)
	if class == ResourceUnknown {
		h.base.ProxyPassthrough(w, req, cleanPath, h.currentPreferredUpstream())
		return
	}
	if class == ResourceMetadata {
		subPath, discovered := h.discoverSubPath(cleanPath)
		if h.tryServeMetadata(w, req, cleanPath) {
			return
		}
		if discovered && h.bus != nil {
			h.bus.Publish(bus.Event{
				Type: bus.EventMetadataDiscovered,
				Payload: bus.MetadataDiscoveredPayload{
					Instance: h.name,
					SubPath:  subPath,
				},
			})
		}
		h.base.ProxyPassthrough(w, req, cleanPath, h.currentPreferredUpstream())
		return
	}
	h.prepareRequest(req.Context(), cleanPath, class)
	h.base.ServeHTTP(w, req)
}

func (h *IndexedHandler) Start(ctx context.Context) error {
	h.lifecycleCtx = ctx
	if h.sh != nil {
		h.sh.Start(ctx)
	}
	h.restoreRoots(ctx)
	h.restoreGenerations(ctx)
	return nil
}

func (h *IndexedHandler) Stop(ctx context.Context) error {
	if h.sh != nil {
		if err := h.sh.Stop(ctx); err != nil {
			return err
		}
	}
	if err := utils.WaitGroupContext(ctx, &h.wait); err != nil {
		return err
	}
	return h.base.CloseContext(ctx)
}

func (h *IndexedHandler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	if !h.hasAnyRootSnapshot() {
		return nil
	}
	deleted := 0
	return fs.WalkDir(h.store.TenantFS(h.name), h.objectRoot, func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil || entry.IsDir() || strings.Contains(objectPath, "/.roots/") {
			return nil
		}
		objectInfo, statErr := h.store.StatObject(ctx, h.name, objectPath)
		if statErr != nil || objectInfo.Options["indexed"] != "true" {
			return nil
		}
		cleanPath := strings.TrimPrefix(objectPath, h.objectRoot+"/")
		switch h.classify(cleanPath) {
		case ResourceArtifact:
			if _, keep := h.currentRepoObject(cleanPath, ResourceArtifact); keep {
				return nil
			}
		case ResourceAuxiliary:
			if _, keep := h.currentRepoObject(cleanPath, ResourceAuxiliary); keep {
				return nil
			}
		default:
			return nil
		}
		if opts.DryRun {
			deleted++
			slog.Info("indexed cleanup dry-run delete", "instance", h.name, "path", objectPath)
			return nil
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("indexed cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		} else {
			deleted++
		}
		return nil
	})
}

func (h *IndexedHandler) RefreshSubPath(ctx context.Context, subPath string) error {
	h.mu.RLock()
	entry, ok := h.roots[subPath]
	h.mu.RUnlock()
	if !ok || entry == nil || len(entry.targets) == 0 {
		return fmt.Errorf("root %s not found or has no targets", subPath)
	}
	var (
		refreshGen uint64
		release    func()
	)
	if h.sh != nil {
		rh, done, err := h.sh.TryStartRefresh(subPath, time.Now())
		if err != nil {
			reason := "rejected"
			switch {
			case errors.Is(err, health.ErrRefreshAlreadyRunning):
				reason = "already_refreshing"
			case errors.Is(err, health.ErrRefreshBlockedUntil):
				reason = "blocked"
			case errors.Is(err, health.ErrRefreshResourceRemoved):
				reason = "removed"
			}
			slog.Debug("subpath refresh skipped", "instance", h.name, "mode", h.mode, "subPath", subPath, "reason", reason)
			return scheduler.ErrTaskSkipped
		}
		refreshGen = rh.Generation
		release = done
		defer release()
	}
	upstreams := h.weightedUpstreams()
	if len(upstreams) == 0 {
		return errors.New("no upstreams available")
	}
	generation := strconv.FormatInt(time.Now().UnixNano(), 36)
	var firstErr error
	for _, upstream := range upstreams {
		snapshot, err := h.buildSnapshot(ctx, subPath, generation, upstream, entry.targets)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			slog.Debug("subpath refresh failed on upstream", "instance", h.name, "subPath", subPath, "upstream", upstream, "err", err)
			continue
		}
		if err := h.publishSnapshot(ctx, snapshot); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if h.sh != nil {
			h.sh.FinishRefresh(subPath, refreshGen, nil, targetsToProbe(snapshot.Targets))
		}
		h.mu.Lock()
		h.rootSnapshots[subPath] = snapshot
		h.rebuildAggregateLocked()
		h.mu.Unlock()
		h.reportMetadataState()
		slog.Debug("subpath refresh succeeded", "instance", h.name, "mode", h.mode, "subPath", subPath, "upstream", upstream)
		return nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	if h.sh != nil {
		h.sh.FinishRefresh(subPath, refreshGen, refreshHealthError(firstErr), nil)
	}
	return firstErr
}

func (h *IndexedHandler) CleanupSubPath(ctx context.Context, subPath string) error {
	rootDir := path.Join(h.objectRoot, ".roots", pathEscapeKey(subPath), "generations")
	currentGen := h.currentGeneration(subPath)
	if currentGen == "" {
		return nil
	}
	var toDelete []string
	if err := fs.WalkDir(h.store.TenantFS(h.name), rootDir, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		parts := strings.Split(strings.TrimPrefix(objectPath, rootDir+"/"), "/")
		if len(parts) > 0 && parts[0] != currentGen {
			toDelete = append(toDelete, objectPath)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, p := range toDelete {
		if err := h.store.DeleteObject(ctx, h.name, p); err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("metadata gc failed", "path", p, "err", err)
		}
	}
	return nil
}

func (h *IndexedHandler) tryServeMetadata(w http.ResponseWriter, req *http.Request, cleanPath string) bool {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		return false
	}
	obj, ok := snapshot.Metadata[cleanPath]
	if !ok {
		return false
	}
	if obj.Path != cleanPath {
		http.Redirect(w, req, req.Header.Get("X-Cache-Proxy-Prefix")+"/"+obj.Path, http.StatusFound)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusFound, 0)
		return true
	}
	objectPath := obj.StorePath
	reader, err := h.store.OpenObject(req.Context(), h.name, objectPath)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusInternalServerError, 0)
		return true
	}
	size := reader.Info().Size
	headers := map[string]string{
		"Content-Length": strconv.FormatInt(size, 10),
		"X-Cache":        "GENERATION",
	}
	for key, value := range reader.Info().Options {
		headers[headerName(key)] = value
	}
	httpcache.StripInternal(headers)
	result := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}
	result.FlushClose(req, w)
	h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusOK, uint64(size))
	return true
}

func (h *IndexedHandler) buildSnapshot(ctx context.Context, rootKey, generation, upstream string, targets []MetadataTarget) (*LiveSnapshot, error) {
	session := &RefreshSession{
		handler:    h,
		rootKey:    rootKey,
		upstream:   upstream,
		generation: generation,
		blobs:      map[string]*MetadataBlob{},
		targets:    append([]MetadataTarget(nil), targets...),
	}
	defer session.Close()
	snapshot, err := h.build(ctx, session)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, errors.New("metadata refresh produced no snapshot")
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]MetadataObject{}
	}
	if snapshot.Artifacts == nil {
		snapshot.Artifacts = map[string]RepoObject{}
	}
	snapshot.RootKey = rootKey
	snapshot.Generation = generation
	snapshot.Upstream = upstream
	snapshot.Published = time.Now().UTC()
	snapshot.Targets = targets
	for path, obj := range snapshot.Artifacts {
		obj.Path = path
		obj.Upstream = upstream
		obj.RootKey = rootKey
		obj.Generation = generation
		snapshot.Artifacts[path] = obj
	}
	for pathKey, obj := range snapshot.Metadata {
		if obj.Path == "" {
			obj.Path = pathKey
		}
		obj.StorePath = h.generationMetadataPath(rootKey, generation, obj.Path)
		snapshot.Metadata[pathKey] = obj
		if obj.Required {
			if _, err := h.store.StatObject(ctx, h.name, obj.StorePath); err != nil {
				return nil, fmt.Errorf("%s: required metadata missing", obj.Path)
			}
		}
	}
	return snapshot, nil
}

func (h *IndexedHandler) fetchMetadataObject(ctx context.Context, rootKey, generation, upstream, cleanPath string) (MetadataBlob, error) {
	targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(cleanPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return MetadataBlob{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)

	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, 0)
		if h.sh != nil {
			h.sh.RecordFailure(upstream, err)
		}
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: fmt.Errorf("fetch %s: %w", targetURL, err)}
	}
	defer response.Body.Close()
	response.Body = utils.NewContextReadCloser(ctx, h.client.WrapBody(response.Body))
	h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, response.StatusCode)
	if h.sh != nil {
		h.sh.RecordResult(upstream, response.StatusCode, latency)
	}
	if response.StatusCode != http.StatusOK {
		switch response.StatusCode {
		case http.StatusNotFound, http.StatusGone:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataNotFound}
		case http.StatusUnauthorized, http.StatusForbidden:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataForbidden}
		default:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: fmt.Errorf("HTTP %d from upstream: %w", response.StatusCode, errMetadataTransient)}
		}
	}
	tempFile, size, err := utils.TempFileFromReader(io.LimitReader(utils.NewRateLimitReader(response.Body), maxMetadataObjectSize+1))
	if err != nil {
		return MetadataBlob{}, err
	}
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name())
		}
	}()
	if size > maxMetadataObjectSize {
		return MetadataBlob{}, fmt.Errorf("%s: metadata object exceeds %d bytes", cleanPath, maxMetadataObjectSize)
	}
	headers := map[string]string{}
	for key, value := range response.Header {
		if len(value) > 0 {
			headers[http.CanonicalHeaderKey(key)] = value[0]
		}
	}
	if err := h.putMetadataObject(ctx, rootKey, generation, cleanPath, tempFile, size, headers); err != nil {
		return MetadataBlob{}, err
	}
	cleanupTemp = false
	return MetadataBlob{Path: cleanPath, file: tempFile, temp: tempFile.Name(), Headers: headers}, nil
}

func (h *IndexedHandler) putMetadataObject(ctx context.Context, rootKey, generation, cleanPath string, body io.ReadSeeker, size int64, headers map[string]string) error {
	objectPath := h.generationMetadataPath(rootKey, generation, cleanPath)
	meta := map[string]string{
		"content-type":   headers["Content-Type"],
		"content-length": headers["Content-Length"],
		"last-modified":  headers["Last-Modified"],
		"etag":           headers["ETag"],
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           h.mode,
		"cache":          "GENERATION",
	}
	if meta["content-length"] == "" {
		meta["content-length"] = strconv.FormatInt(size, 10)
	}
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(path.Join(h.name, parent), 0o755); err != nil {
			return err
		}
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := h.store.Put(ctx, h.name, objectPath, body, meta)
	if _, seekErr := body.Seek(0, io.SeekStart); seekErr != nil && err == nil {
		err = seekErr
	}
	return err
}

func (h *IndexedHandler) publishSnapshot(ctx context.Context, snapshot *LiveSnapshot) error {
	data, err := yaml.Marshal(snapshot)
	if err != nil {
		return err
	}
	snapshotPath := h.snapshotPath(snapshot.RootKey, snapshot.Generation)
	currentPath := h.currentPath(snapshot.RootKey)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(snapshotPath)), 0o755); err != nil {
		return err
	}
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(currentPath)), 0o755); err != nil {
		return err
	}
	if _, err = h.store.Put(ctx, h.name, snapshotPath, bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		return err
	}
	refData, err := yaml.Marshal(struct {
		RootKey    string `yaml:"root_key"`
		Generation string `yaml:"generation"`
	}{RootKey: snapshot.RootKey, Generation: snapshot.Generation})
	if err != nil {
		return err
	}
	tmpPath := currentPath + ".tmp." + snapshot.Generation
	if _, err = h.store.Put(ctx, h.name, tmpPath, bytes.NewReader(refData), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		return err
	}
	return h.store.Rename(path.Join(h.name, tmpPath), path.Join(h.name, currentPath))
}

func (h *IndexedHandler) prepareRequest(ctx context.Context, cleanPath string, class ResourceClass) {
	obj, ok := h.currentRepoObject(cleanPath, class)
	if !ok {
		return
	}
	if obj.Identity == "" {
		return
	}
	info, err := h.store.StatObject(ctx, h.name, path.Join(h.objectRoot, cleanPath))
	if err != nil {
		return
	}
	if info.Options["indexed"] == "true" && info.Options["indexed-identity"] != obj.Identity {
		h.deleteObject(ctx, cleanPath)
	}
}

func (h *IndexedHandler) currentRepoObject(cleanPath string, class ResourceClass) (RepoObject, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	switch class {
	case ResourceArtifact:
		for _, snapshot := range h.rootSnapshots {
			if obj, ok := snapshot.Artifacts[cleanPath]; ok {
				return obj, true
			}
		}
		return RepoObject{}, false
	case ResourceAuxiliary:
		for _, suffix := range []string{".sig", ".asc", ".gpg", ".sha256", ".sha512", ".md5", ".md5sum"} {
			if base := strings.TrimSuffix(cleanPath, suffix); base != cleanPath {
				for _, snapshot := range h.rootSnapshots {
					if artifact, ok := snapshot.Artifacts[base]; ok {
						return RepoObject{
							Path:       cleanPath,
							Identity:   artifact.Identity,
							Upstream:   artifact.Upstream,
							RootKey:    artifact.RootKey,
							Generation: artifact.Generation,
						}, true
					}
				}
			}
		}
		return RepoObject{}, false
	default:
		return RepoObject{}, false
	}
}

func (h *IndexedHandler) deleteObject(ctx context.Context, cleanPath string) {
	if err := h.store.DeleteObject(ctx, h.name, path.Join(h.objectRoot, cleanPath)); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, fs.ErrNotExist) {
		slog.Debug("indexed object delete failed", "instance", h.name, "path", cleanPath, "err", err)
	}
}

func (h *IndexedHandler) extraObjectMetadata(_ *http.Request, route httpcache.Route, _ map[string]string, _ string) map[string]string {
	cleanPath := strings.TrimPrefix(route.ObjectPath, h.objectRoot+"/")
	obj, ok := h.currentRepoObject(cleanPath, h.classify(cleanPath))
	if !ok || obj.Identity == "" {
		return nil
	}
	meta := map[string]string{
		"indexed":            "true",
		"indexed-identity":   obj.Identity,
		"indexed-root":       obj.RootKey,
		"indexed-generation": obj.Generation,
		"indexed-upstream":   obj.Upstream,
	}
	if obj.Digest.Algorithm != "" && obj.Digest.Value != "" {
		meta["indexed-digest-algorithm"] = obj.Digest.Algorithm
		meta["indexed-digest"] = obj.Digest.Value
		if obj.Digest.Verifiable {
			meta["indexed-digest-verifiable"] = "true"
		}
	}
	return meta
}

func (h *IndexedHandler) verifyObject(_ *http.Request, route httpcache.Route, reader io.ReadSeeker) error {
	cleanPath := strings.TrimPrefix(route.ObjectPath, h.objectRoot+"/")
	obj, ok := h.currentRepoObject(cleanPath, h.classify(cleanPath))
	if !ok || !obj.Digest.Verifiable || obj.Digest.Algorithm != "sha256" || obj.Digest.Value == "" {
		return nil
	}
	expected := strings.TrimPrefix(obj.Digest.Value, "sha256:")
	if len(expected) != 64 {
		return nil
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return err
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("sha256 mismatch: expected %s got %s", expected, actual)
	}
	return nil
}

func (h *IndexedHandler) weightedUpstreams() []string {
	if h.sh == nil {
		return append([]string(nil), h.upstreams...)
	}
	weighted := h.sh.WeightedUpstreams(h.upstreams)
	upstreams := make([]string, 0, len(weighted))
	for _, wu := range weighted {
		upstreams = append(upstreams, wu.URL)
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
	aggregate := &LiveSnapshot{
		Metadata:  map[string]MetadataObject{},
		Artifacts: map[string]RepoObject{},
	}
	for _, snap := range h.rootSnapshots {
		for p, obj := range snap.Metadata {
			aggregate.Metadata[p] = obj
		}
	}
	h.snapshot = aggregate
}

func (h *IndexedHandler) currentSnapshot() *LiveSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.snapshot
}

func (h *IndexedHandler) hasRootSnapshot(rootKey string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.rootSnapshots[rootKey]
	return ok
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
	if snap, ok := h.rootSnapshots[subPath]; ok {
		return snap.Generation
	}
	return ""
}

func (h *IndexedHandler) reportMetadataState() {
	ready := h.hasAnyRootSnapshot()
	stateStr := "booting"
	if ready {
		if h.sh != nil {
			switch h.sh.AggregateState() {
			case health.StateHealthy:
				stateStr = "ready"
			case health.StateDegraded:
				stateStr = "degraded"
			case health.StateUnhealthy:
				stateStr = "degraded"
			}
		} else {
			stateStr = "ready"
		}
	}
	h.stats.SetMetadataState(h.name, h.mode, stateStr, ready)
}

func (h *IndexedHandler) RootReleases() []runtime.RootRelease {
	h.mu.RLock()
	releases := make([]runtime.RootRelease, 0, len(h.rootSnapshots))
	for rootKey, snap := range h.rootSnapshots {
		releases = append(releases, runtime.RootRelease{
			Key:           rootKey,
			Generation:    snap.Generation,
			Published:     snap.Published,
			Upstream:      snap.Upstream,
			ArtifactCount: len(snap.Artifacts),
			MetadataCount: len(snap.Metadata),
		})
	}
	h.mu.RUnlock()

	if h.sh != nil {
		resources := h.sh.SnapshotResources()
		for i, rr := range releases {
			for _, res := range resources {
				if res.Path == rr.Key {
					releases[i].State = res.State
					releases[i].LastSuccessAt = res.LastSuccessAt
					releases[i].LastRefreshAt = res.LastRefreshAt
					break
				}
			}
		}
	}

	stateOrder := map[string]int{"active": 0, "suspect": 1, "blocked": 2, "pending": 3, "removed": 4}
	sort.Slice(releases, func(i, j int) bool {
		oi := stateOrder[releases[i].State]
		oj := stateOrder[releases[j].State]
		if oi != oj {
			return oi < oj
		}
		return releases[i].Key < releases[j].Key
	})
	return releases
}

func targetsToProbe(targets []MetadataTarget) []health.ProbeTarget {
	pt := make([]health.ProbeTarget, 0, len(targets))
	for _, t := range targets {
		pt = append(pt, health.ProbeTarget{Path: t.URL})
	}
	return pt
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

func (h *IndexedHandler) classify(cleanPath string) ResourceClass {
	if h.classifier == nil {
		return ResourceAuxiliary
	}
	return h.classifier(cleanPath)
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

func headerName(key string) string {
	switch key {
	case "content-type":
		return "Content-Type"
	case "content-length":
		return "Content-Length"
	case "last-modified":
		return "Last-Modified"
	case "etag":
		return "ETag"
	default:
		return key
	}
}
