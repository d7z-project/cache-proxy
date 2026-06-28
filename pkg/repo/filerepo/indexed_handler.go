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
	health     *health.ServiceHealth

	mu             sync.RWMutex
	refreshing     bool
	snapshot       *LiveSnapshot
	targets        map[string][]MetadataTarget
	rootSnapshots  map[string]*LiveSnapshot
	refreshTrigger chan struct{}
	wait           sync.WaitGroup
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
		health:        svcHealth,
		targets:       map[string][]MetadataTarget{},
		rootSnapshots: map[string]*LiveSnapshot{},
	}
	if builder != nil && refreshPolicy.Interval > 0 {
		handler.refreshTrigger = make(chan struct{}, 1)
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
		h.base.ProxyPassthrough(w, req, "")
		return
	}
	class := h.classify(cleanPath)
	if class == ResourceUnknown {
		h.base.ProxyPassthrough(w, req, cleanPath)
		return
	}
	if class == ResourceMetadata {
		h.discoverRoot(req.Context(), cleanPath)
		h.serveMetadata(w, req, cleanPath)
		return
	}
	h.prepareRequest(req.Context(), cleanPath, class)
	h.base.ServeHTTP(w, req)
}

func (h *IndexedHandler) Start(ctx context.Context) error {
	if h.health != nil {
		h.health.Start()
	}
	h.restoreRoots(ctx)
	h.restoreGenerations(ctx)
	go h.runRefreshCycle(ctx)
	if h.policy.Interval <= 0 || h.build == nil {
		return nil
	}
	h.wait.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("refresh cycle panic", "instance", h.name, "panic", r)
			}
		}()
		defer h.wait.Done()
		ticker := time.NewTicker(h.policy.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.runRefreshCycle(ctx)
			case <-h.refreshTrigger:
				h.runRefreshCycle(ctx)
			}
		}
	}()
	return nil
}

func (h *IndexedHandler) Stop(ctx context.Context) error {
	if h.health != nil {
		if err := h.health.Stop(ctx); err != nil {
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
		if err := h.Refresh(req.Context()); err != nil {
			httpcache.ErrorResponse(http.StatusBadGateway, err).FlushClose(req, w)
			h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusBadGateway, 0)
			return
		}
		snapshot = h.currentSnapshot()
	}
	if snapshot == nil {
		httpcache.ErrorResponse(http.StatusBadGateway, errors.New("metadata generation is not ready")).FlushClose(req, w)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	obj, ok := snapshot.Metadata[cleanPath]
	if !ok {
		http.NotFound(w, req)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "MISS", http.StatusNotFound, 0)
		return
	}
	objectPath := obj.StorePath
	reader, err := h.store.OpenObject(req.Context(), h.name, objectPath)
	if err != nil {
		httpcache.ErrorResponse(http.StatusBadGateway, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusBadGateway, 0)
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
	h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusOK, uint64(size))
}

func (h *IndexedHandler) runRefreshCycle(ctx context.Context) {
	if h.build == nil || ctx.Err() != nil {
		return
	}
	h.doRefresh(ctx, false)
}

func (h *IndexedHandler) doRefresh(ctx context.Context, allRoots bool) error {
	h.mu.Lock()
	if h.refreshing {
		h.mu.Unlock()
		return nil
	}
	h.refreshing = true
	h.mu.Unlock()

	startedAt := time.Now()
	refreshed, err := h.refreshRoots(ctx, startedAt, !allRoots)

	h.mu.Lock()
	h.refreshing = false
	snapshotReady := h.snapshot != nil
	h.mu.Unlock()

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
	if h.health == nil {
		return false, nil
	}
	rhs, unlock, ok := h.health.TryStartRefresh(rootKey)
	if !ok {
		return false, nil
	}
	defer unlock()

	h.mu.RLock()
	targets, ok := h.targets[rootKey]
	h.mu.RUnlock()
	if !ok || len(targets) == 0 {
		h.health.FinishRefresh(rootKey, rhs.Generation, nil, nil)
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
		h.health.FinishRefresh(rootKey, rhs.Generation, nil, targetsToProbe(targets))
		return true, nil
	}
	if firstErr == nil {
		firstErr = errMetadataTransient
	}
	h.health.FinishRefresh(rootKey, rhs.Generation, refreshHealthError(firstErr), nil)
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
		if h.health != nil {
			h.health.RecordFailure(upstream, err)
		}
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataTransient}
	}
	defer response.Body.Close()
	h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, response.StatusCode)
	if h.health != nil {
		h.health.RecordResult(upstream, response.StatusCode, latency)
	}
	if response.StatusCode != http.StatusOK {
		switch response.StatusCode {
		case http.StatusNotFound, http.StatusGone:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataNotFound}
		case http.StatusUnauthorized, http.StatusForbidden:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataForbidden}
		default:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataTransient}
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
	if h.health == nil {
		return false, nil
	}
	resources := h.health.ActiveResources()
	refreshed := false
	var firstErr error
	for _, rh := range resources {
		if dueOnly {
			nextRefresh, ok := h.health.ResourceNextRefresh(rh.Path)
			if ok && !nextRefresh.IsZero() && nextRefresh.After(now) {
				continue
			}
		}
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
	if h.health == nil {
		return append([]string(nil), h.upstreams...)
	}
	weighted := h.health.WeightedUpstreams(h.upstreams)
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
	if !ok || obj.Identity == "" {
		return nil
	}
	expected := strings.TrimPrefix(obj.Identity, "sha256:")
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
	if h.health != nil {
		h.health.AddResource(rootKey, targetsToProbe(targets), h.upstreams)
	}
}

func (h *IndexedHandler) discoverRoot(ctx context.Context, cleanPath string) {
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
		return
	}
	h.targets[key] = targets
	h.mu.Unlock()
	if h.health != nil {
		h.health.AddResource(key, targetsToProbe(targets), h.upstreams)
	}
	if h.refreshTrigger != nil {
		select {
		case h.refreshTrigger <- struct{}{}:
		default:
		}
		return
	}
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()
		_, _ = h.refreshRoot(ctx, key, time.Now())
	}()
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
		if h.health != nil {
			switch h.health.AggregateState() {
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
