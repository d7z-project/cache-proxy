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
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

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
	policy     RefreshPolicy
	build      SnapshotBuilder
	sh         *health.ServiceHealth

	mu            sync.RWMutex
	snapshot      *LiveSnapshot
	targets       map[string][]MetadataTarget
	rootSnapshots map[string]*LiveSnapshot
	firstRefreshReady map[string]chan struct{}
	lifecycleCtx  context.Context
	wait          sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, _ config.Freshness, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, refreshPolicy RefreshPolicy, discover Discoverer, seeds []RootSpec, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats, svcHealth *health.ServiceHealth) *IndexedHandler {
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
		policy:        refreshPolicy,
		build:         builder,
		sh:            svcHealth,
		targets:       map[string][]MetadataTarget{},
		rootSnapshots: map[string]*LiveSnapshot{},
		firstRefreshReady: map[string]chan struct{}{},
	}
	handler.base = httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:         mode,
		ExpireAfter:  expireAfter,
		Upstreams:    append([]string(nil), upstreams...),
		Transport:    transport,
		PassHeaders:  append([]string(nil), policy.PassHeaders...),
		BusyPolicy:   policy.AuxiliaryBusyPolicy,
		MetadataFunc: handler.extraObjectMetadata,
		VerifyFunc:   handler.verifyObject,
	}, store, &generationResolver{handler: handler, policy: policy}, stats, svcHealth)
	handler.client = utils.DefaultHttpClientWrapper()
	httpcache.ConfigureClientTransport(handler.client, name, mode, transport)
	for _, seed := range seeds {
		handler.addRoot(seed.Key(), seed.Targets())
	}
	handler.reportMetadataState()
	return handler
}

func (h *IndexedHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := cleanRequestPath(req.URL.Path)
	if cleanPath == "" {
		h.base.ProxyPassthrough(w, req, "", "")
		return
	}
	class := h.classify(cleanPath)
	if class == ResourceUnknown {
		snap := h.currentSnapshot()
		preferred := ""
		if snap != nil {
			preferred = snap.Upstream
		}
		h.base.ProxyPassthrough(w, req, cleanPath, preferred)
		return
	}
	if class == ResourceMetadata {
		h.discoverRoot(cleanPath)
		h.serveMetadata(w, req, cleanPath)
		return
	}
	h.prepareRequest(req.Context(), cleanPath, class)
	h.base.ServeHTTP(w, req)
}

func (h *IndexedHandler) Start(ctx context.Context) error {
	h.lifecycleCtx = ctx
	if h.sh != nil {
		h.sh.Start()
	}
	h.restoreRoots(ctx)
	h.wait.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("refresh cycle panic", "instance", h.name, "panic", r)
			}
		}()
		defer h.wait.Done()
		h.restoreGenerations(ctx)
		if h.policy.Interval <= 0 || h.build == nil {
			return
		}
		ticker := time.NewTicker(h.policy.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.runRefreshCycle(ctx)
			}
		}
	}()
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

func (h *IndexedHandler) Refresh(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return h.doRefresh(ctx, true)
}

func (h *IndexedHandler) Cleanup(ctx context.Context) error {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		return nil
	}
	return fs.WalkDir(h.store.TenantFS(h.name), h.objectRoot, func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || entry.IsDir() || strings.Contains(objectPath, "/.roots/") {
			return nil
		}
		cleanPath := strings.TrimPrefix(objectPath, h.objectRoot+"/")
		switch h.classify(cleanPath) {
		case ResourceArtifact:
			if _, keep := snapshot.Artifacts[cleanPath]; keep {
				return nil
			}
		case ResourceAuxiliary:
			if _, keep := snapshot.Auxiliary[cleanPath]; keep {
				return nil
			}
		default:
			return nil
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("indexed cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		}
		return nil
	})
}

func (h *IndexedHandler) serveMetadata(w http.ResponseWriter, req *http.Request, cleanPath string) {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		if spec, ok := h.discover.Discover(cleanPath); ok {
			rootKey := spec.Key()
			if !h.rootHasSnapshot(rootKey) {
				ready := h.waitForFirstRefresh(req.Context(), rootKey)
				if ready {
					snapshot = h.currentSnapshot()
				}
			} else {
				snapshot = h.currentSnapshot()
			}
		}
	}
	if snapshot == nil {
		w.Header().Set("Retry-After", "5")
		httpcache.ErrorResponse(http.StatusServiceUnavailable, errors.New("metadata generation is not ready")).FlushClose(req, w)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusServiceUnavailable, 0)
		return
	}
	obj, ok := snapshot.Metadata[cleanPath]
	if !ok {
		if spec, disc := h.discover.Discover(cleanPath); disc {
			rootKey := spec.Key()
			if !h.rootHasSnapshot(rootKey) {
				ready := h.waitForFirstRefresh(req.Context(), rootKey)
				if ready {
					obj, ok = h.currentSnapshot().Metadata[cleanPath]
				}
			}
		}
	}
	if !ok {
		http.NotFound(w, req)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "MISS", http.StatusNotFound, 0)
		return
	}
	objectPath := obj.StorePath
	reader, err := h.store.OpenObject(req.Context(), h.name, objectPath)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusInternalServerError, 0)
		return
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
	slog.Debug("metadata served", "instance", h.name, "mode", h.mode, "path", cleanPath, "size", size)
	h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusOK, uint64(size))
}

func (h *IndexedHandler) ensureFirstRefresh(rootKey string) {
	h.mu.Lock()
	if _, exists := h.firstRefreshReady[rootKey]; !exists {
		h.firstRefreshReady[rootKey] = make(chan struct{})
	}
	h.mu.Unlock()
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()
		_, _ = h.refreshRoot(h.lifecycleCtx, rootKey, time.Now())
	}()
}

func (h *IndexedHandler) waitForFirstRefresh(ctx context.Context, rootKey string) bool {
	if h.rootHasSnapshot(rootKey) {
		return true
	}
	h.mu.Lock()
	ch, exists := h.firstRefreshReady[rootKey]
	if !exists {
		ch = make(chan struct{})
		h.firstRefreshReady[rootKey] = ch
	}
	h.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (h *IndexedHandler) signalFirstRefresh(rootKey string) {
	h.mu.Lock()
	if ch, ok := h.firstRefreshReady[rootKey]; ok {
		close(ch)
		delete(h.firstRefreshReady, rootKey)
	}
	h.mu.Unlock()
}

func (h *IndexedHandler) rootHasSnapshot(rootKey string) bool {
	h.mu.RLock()
	_, ok := h.rootSnapshots[rootKey]
	h.mu.RUnlock()
	return ok
}

func (h *IndexedHandler) runRefreshCycle(ctx context.Context) {
	if h.build == nil || ctx.Err() != nil {
		return
	}
	slog.Debug("metadata refresh cycle starting", "instance", h.name, "mode", h.mode, "interval", h.policy.Interval)
	h.doRefresh(ctx, false)
}

func (h *IndexedHandler) doRefresh(ctx context.Context, allRoots bool) error {
	startedAt := time.Now()
	refreshed, err := h.refreshRoots(ctx, startedAt, !allRoots)

	snapshotReady := h.currentSnapshot() != nil

	if refreshed {
		if cleanupErr := h.Cleanup(ctx); cleanupErr != nil && !errors.Is(cleanupErr, context.Canceled) {
			slog.Info("indexed cleanup failed after refresh", "instance", h.name, "mode", h.mode, "err", cleanupErr)
		}
	}
	h.reportMetadataState()
	h.stats.RecordMetadataRefresh(h.name, h.mode, refreshResult(err), time.Since(startedAt), snapshotReady)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Info("metadata refresh failed", "instance", h.name, "mode", h.mode, "err", err)
	}
	h.saveState(ctx)
	return err
}

func (h *IndexedHandler) refreshRoot(ctx context.Context, rootKey string, now time.Time) (bool, error) {
	if h.sh == nil {
		return false, nil
	}
	rhs, unlock, ok := h.sh.TryStartRefresh(rootKey)
	if !ok {
		return false, nil
	}
	defer unlock()

	h.mu.RLock()
	targets, ok := h.targets[rootKey]
	h.mu.RUnlock()
	if !ok || len(targets) == 0 {
		h.sh.FinishRefresh(rootKey, rhs.Generation, nil, nil)
		h.signalFirstRefresh(rootKey)
		return false, nil
	}

	upstreams := h.refreshUpstreams()
	generation := strconv.FormatUint(rhs.Generation, 10)
	var firstErr error
	for _, upstream := range upstreams {
		snapshot, err := h.buildSnapshot(ctx, rootKey, generation, upstream, targets)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			slog.Debug("root refresh failed on upstream", "instance", h.name, "mode", h.mode, "root", rootKey, "upstream", upstream, "err", err)
			continue
		}
		if err := h.publishSnapshot(ctx, snapshot); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		h.mu.Lock()
		h.rootSnapshots[rootKey] = snapshot
		h.rebuildAggregateLocked()
		h.mu.Unlock()
		h.sh.FinishRefresh(rootKey, rhs.Generation, nil, targetsToProbe(targets))
		h.sh.ScheduleRefresh(rootKey, h.policy.Interval)
		h.signalFirstRefresh(rootKey)
		slog.Debug("root refresh succeeded", "instance", h.name, "mode", h.mode, "root", rootKey, "generation", generation, "upstream", upstream)
		return true, nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	h.sh.FinishRefresh(rootKey, rhs.Generation, refreshHealthError(firstErr), nil)
	h.sh.ScheduleRefresh(rootKey, h.policy.Interval)
	h.signalFirstRefresh(rootKey)
	return false, firstErr
}

func (h *IndexedHandler) buildSnapshot(ctx context.Context, rootKey, generation, upstream string, targets []MetadataTarget) (*LiveSnapshot, error) {
	session := &RefreshSession{
		handler:    h,
		rootKey:    rootKey,
		upstream:   upstream,
		generation: generation,
		blobs:      map[string]MetadataBlob{},
		targets:    append([]MetadataTarget(nil), targets...),
	}
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
	if snapshot.Auxiliary == nil {
		snapshot.Auxiliary = map[string]RepoObject{}
	}
	snapshot.RootKey = rootKey
	snapshot.Generation = generation
	snapshot.Upstream = upstream
	snapshot.Published = time.Now().UTC()
	for path, obj := range snapshot.Artifacts {
		obj.Path = path
		obj.Upstream = upstream
		snapshot.Artifacts[path] = obj
	}
	for path, obj := range snapshot.Auxiliary {
		obj.Path = path
		obj.Upstream = upstream
		snapshot.Auxiliary[path] = obj
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
	response.Body = utils.NewContextReadCloser(ctx, response.Body)
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
	body, err := io.ReadAll(io.LimitReader(utils.NewRateLimitReader(response.Body), 50<<20))
	if err != nil {
		return MetadataBlob{}, err
	}
	headers := map[string]string{}
	for key, value := range response.Header {
		if len(value) > 0 {
			headers[http.CanonicalHeaderKey(key)] = value[0]
		}
	}
	if err := h.putMetadataObject(ctx, rootKey, generation, cleanPath, body, headers); err != nil {
		return MetadataBlob{}, err
	}
	return MetadataBlob{Path: cleanPath, Body: body, Headers: headers}, nil
}

func (h *IndexedHandler) putMetadataObject(ctx context.Context, rootKey, generation, cleanPath string, body []byte, headers map[string]string) error {
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
		meta["content-length"] = strconv.Itoa(len(body))
	}
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(path.Join(h.name, parent), 0o755); err != nil {
			return err
		}
	}
	_, err := h.store.Put(ctx, h.name, objectPath, bytes.NewReader(body), meta)
	return err
}

func (h *IndexedHandler) publishSnapshot(ctx context.Context, snapshot *LiveSnapshot) error {
	data, err := yaml.Marshal(snapshot)
	if err != nil {
		return err
	}
	current := h.currentPath(snapshot.RootKey)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(current)), 0o755); err != nil {
		return err
	}
	_, err = h.store.Put(ctx, h.name, current, bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	})
	return err
}

func (h *IndexedHandler) refreshRoots(ctx context.Context, now time.Time, dueOnly bool) (bool, error) {
	if h.sh == nil {
		return false, nil
	}
	resources := h.sh.ActiveResources()
	refreshed := false
	var firstErr error
	for _, rh := range resources {
		if dueOnly {
			nextRefresh, ok := h.sh.ResourceNextRefresh(rh.Path)
			if ok && !nextRefresh.IsZero() && nextRefresh.After(now) {
				slog.Debug("root refresh skipped (not due)", "instance", h.name, "mode", h.mode, "root", rh.Path, "next_refresh", nextRefresh)
				continue
			}
		}
		slog.Debug("root refresh due", "instance", h.name, "mode", h.mode, "root", rh.Path)
		changed, err := h.refreshRoot(ctx, rh.Path, now)
		if changed {
			refreshed = true
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return refreshed, firstErr
}

func (h *IndexedHandler) refreshUpstreams() []string {
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

func (h *IndexedHandler) prepareRequest(ctx context.Context, cleanPath string, class ResourceClass) {
	obj, ok := h.currentRepoObject(cleanPath, class)
	if !ok {
		h.deleteObject(ctx, cleanPath)
		return
	}
	if obj.Identity == "" {
		return
	}
	info, err := h.store.StatObject(ctx, h.name, path.Join(h.objectRoot, cleanPath))
	if err != nil {
		return
	}
	if info.Options["indexed-identity"] != obj.Identity {
		slog.Debug("object identity changed, evicting", "instance", h.name, "path", cleanPath, "old", info.Options["indexed-identity"], "new", obj.Identity)
		h.deleteObject(ctx, cleanPath)
	}
}

func (h *IndexedHandler) currentRepoObject(cleanPath string, class ResourceClass) (RepoObject, bool) {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		return RepoObject{}, false
	}
	switch class {
	case ResourceArtifact:
		obj, ok := snapshot.Artifacts[cleanPath]
		return obj, ok
	case ResourceAuxiliary:
		obj, ok := snapshot.Auxiliary[cleanPath]
		return obj, ok
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
	return map[string]string{"indexed-identity": obj.Identity}
}

func (h *IndexedHandler) verifyObject(_ *http.Request, route httpcache.Route, reader io.ReadSeeker) error {
	cleanPath := strings.TrimPrefix(route.ObjectPath, h.objectRoot+"/")
	obj, ok := h.currentRepoObject(cleanPath, h.classify(cleanPath))
	if !ok || obj.ContentHash == "" {
		return nil
	}
	expected := strings.TrimPrefix(obj.ContentHash, "sha256:")
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

func targetsToProbe(targets []MetadataTarget) []health.ProbeTarget {
	pt := make([]health.ProbeTarget, 0, len(targets))
	for _, t := range targets {
		pt = append(pt, health.ProbeTarget{Path: t.URL})
	}
	return pt
}

func (h *IndexedHandler) addRoot(rootKey string, targets []MetadataTarget) {
	h.mu.Lock()
	h.targets[rootKey] = targets
	h.mu.Unlock()
	if h.sh != nil {
		h.sh.AddResource(rootKey, targetsToProbe(targets), h.upstreams)
	}
}

func (h *IndexedHandler) discoverRoot(cleanPath string) {
	if h.discover == nil {
		return
	}
	spec, ok := h.discover.Discover(cleanPath)
	if !ok {
		return
	}
	key := spec.Key()
	targets := spec.Targets()
	h.mu.Lock()
	existingTargets, existing := h.targets[key]
	if existing && len(existingTargets) > 0 {
		h.mu.Unlock()
		if !h.rootHasSnapshot(key) {
			slog.Debug("discovered root (needs refresh)", "instance", h.name, "mode", h.mode, "root", key)
			h.ensureFirstRefresh(key)
		}
		return
	}
	h.targets[key] = targets
	h.mu.Unlock()
	slog.Debug("discovered new root", "instance", h.name, "mode", h.mode, "root", key)
	if h.sh != nil {
		h.sh.AddResource(key, targetsToProbe(targets), h.upstreams)
	}
	if !h.rootHasSnapshot(key) {
		h.ensureFirstRefresh(key)
	}
}

func (h *IndexedHandler) rebuildAggregateLocked() {
	aggregate := &LiveSnapshot{
		Metadata:  map[string]MetadataObject{},
		Artifacts: map[string]RepoObject{},
		Auxiliary: map[string]RepoObject{},
	}
	for _, snap := range h.rootSnapshots {
		for p, obj := range snap.Metadata {
			aggregate.Metadata[p] = obj
		}
		for p, obj := range snap.Artifacts {
			aggregate.Artifacts[p] = obj
		}
		for p, obj := range snap.Auxiliary {
			aggregate.Auxiliary[p] = obj
		}
	}
	h.snapshot = aggregate
}

func (h *IndexedHandler) currentSnapshot() *LiveSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.snapshot
}

func (h *IndexedHandler) reportMetadataState() {
	ready := h.currentSnapshot() != nil
	stateStr := "booting"
	if ready {
		if h.sh != nil {
			switch h.sh.AggregateState() {
			case health.StateHealthy:
				stateStr = "ready"
			case health.StateDegraded:
				stateStr = "degraded"
			case health.StateUnhealthy:
				stateStr = "booting"
			}
		} else {
			stateStr = "ready"
		}
	}
	h.stats.SetMetadataState(h.name, h.mode, stateStr, ready)
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

func refreshResult(err error) string {
	if err == nil {
		return "success"
	}
	var fetchErr MetadataFetchError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, errMetadataNotFound):
		return "not_found"
	case errors.As(err, &fetchErr):
		return "fetch_error"
	default:
		return "parse_error"
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
