package filerepo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

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

	metadataFreshFor time.Duration

	mu             sync.RWMutex
	refreshing     bool
	snapshot       *LiveSnapshot
	targets        map[string][]MetadataTarget
	rootSnapshots  map[string]*LiveSnapshot
	refreshTrigger chan struct{}
	wait           sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, metadataFreshFor config.Freshness, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, refreshPolicy RefreshPolicy, discover Discoverer, seeds []RootSpec, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats, svcHealth *health.ServiceHealth) *IndexedHandler {
	ApplyDefaults(policy, metadataFreshFor)
	handler := &IndexedHandler{
		name:             name,
		mode:             mode,
		objectRoot:       objectRoot,
		store:            store,
		stats:            stats,
		classifier:       classifier,
		upstreams:        append([]string(nil), upstreams...),
		discover:         discover,
		policy:           refreshPolicy,
		build:            builder,
		health:           svcHealth,
		metadataFreshFor: metadataFreshFor.Duration(),
		targets:          map[string][]MetadataTarget{},
		rootSnapshots:    map[string]*LiveSnapshot{},
	}
	if builder != nil && refreshPolicy.Interval > 0 {
		handler.refreshTrigger = make(chan struct{}, 1)
	}
	handler.base = httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:            mode,
		ExpireAfter:     expireAfter,
		Upstreams:       append([]string(nil), upstreams...),
		Transport:       transport,
		PassHeaders:     append([]string(nil), policy.PassHeaders...),
		DefaultFreshFor: policy.AuxiliaryFreshFor,
		BusyPolicy:      policy.AuxiliaryBusyPolicy,
		MetadataFunc:    handler.extraObjectMetadata,
	}, store, NewResolver(Config{
		ObjectRoot: objectRoot,
		Defaults: Defaults{
			Metadata: CacheProfile{
				Policy:      policy.MetadataPolicy,
				FreshFor:    policy.MetadataFreshFor,
				BusyPolicy:  policy.MetadataBusyPolicy,
				ExpireAfter: policy.MetadataExpireAfter,
			},
			Artifact: CacheProfile{
				Policy:      policy.ArtifactPolicy,
				FreshFor:    policy.ArtifactFreshFor,
				BusyPolicy:  policy.ArtifactBusyPolicy,
				ExpireAfter: policy.ArtifactExpireAfter,
			},
			Auxiliary: CacheProfile{
				Policy:      policy.AuxiliaryPolicy,
				FreshFor:    policy.AuxiliaryFreshFor,
				BusyPolicy:  policy.AuxiliaryBusyPolicy,
				ExpireAfter: policy.AuxiliaryExpireAfter,
			},
		},
		Rules:      append([]Rule(nil), policy.Rules...),
		Classifier: classifier,
	}), stats, svcHealth)
	handler.client = utils.DefaultHttpClientWrapper()
	handler.client.UserAgent = httpcache.ModeUserAgent(mode)
	if transport != nil {
		if transport.UserAgent != "" {
			handler.client.UserAgent = transport.UserAgent
		}
		if baseTransport, ok := handler.client.Transport.(*http.Transport); ok {
			if transport.Proxy != "" {
				if proxyURL, err := url.Parse(transport.Proxy); err == nil {
					baseTransport.Proxy = http.ProxyURL(proxyURL)
				}
			}
			if transport.Timeout > 0 {
				baseTransport.DialContext = utils.DefaultDialContext(transport.Timeout.Duration())
			}
		}
	}
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
	if h.classify(cleanPath) == ResourceUnknown {
		h.base.ProxyPassthrough(w, req, cleanPath)
		return
	}
	if cleanPath != "" {
		h.discoverRoot(req.Context(), cleanPath)
		h.prepareRequest(req.Context(), cleanPath)
	}
	h.base.ServeHTTP(w, req)
}

func (h *IndexedHandler) Start(ctx context.Context) error {
	if h.health != nil {
		h.health.Start()
	}
	h.restoreRoots(ctx)
	h.runRefreshCycle(ctx)
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
	return fs.WalkDir(h.store.TenantFS(h.name), ".", func(objectPath string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || entry.IsDir() || !strings.HasPrefix(objectPath, h.objectRoot+"/") {
			return nil
		}
		cleanPath := strings.TrimPrefix(objectPath, h.objectRoot+"/")
		switch h.classify(cleanPath) {
		case ResourceMetadata:
			if _, keep := snapshot.Metadata[cleanPath]; keep {
				return nil
			}
		case ResourceArtifact:
			if _, keep := snapshot.Artifacts[cleanPath]; keep {
				return nil
			}
		default:
			if _, keep := snapshot.Auxiliary[cleanPath]; keep {
				return nil
			}
		}
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			slog.Info("indexed cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		}
		return nil
	})
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
	var refreshed bool
	var err error
	if allRoots {
		refreshed, err = h.refreshAllRoots(ctx, startedAt)
	} else {
		refreshed, err = h.refreshDueRoots(ctx, startedAt)
	}

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

func (h *IndexedHandler) buildSnapshot(ctx context.Context, targets []MetadataTarget) (*LiveSnapshot, error) {
	session := &RefreshSession{
		handler: h,
		blobs:   map[string]MetadataBlob{},
		targets: append([]MetadataTarget(nil), targets...),
	}
	snapshot, err := h.build(ctx, session)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, errors.New("metadata refresh produced no snapshot")
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]struct{}{}
	}
	if snapshot.Artifacts == nil {
		snapshot.Artifacts = map[string]string{}
	}
	if snapshot.Auxiliary == nil {
		snapshot.Auxiliary = map[string]string{}
	}
	if snapshot.Companions == nil {
		snapshot.Companions = map[string][]string{}
	}
	for _, companions := range snapshot.Companions {
		for _, companion := range companions {
			h.deleteObject(ctx, companion)
		}
	}
	return snapshot, nil
}

func (h *IndexedHandler) refreshMetadataObject(ctx context.Context, cleanPath string) (MetadataBlob, error) {
	cached := h.readCachedMetadata(ctx, cleanPath)

	if cached != nil && time.Since(cached.FetchedAt) < h.metadataFreshFor {
		return *cached, nil
	}

	notFound, transient, forbidden := 0, 0, 0
	upstreams := h.upstreams
	if h.health != nil {
		weighted := h.health.WeightedUpstreams(h.upstreams)
		upstreams = make([]string, 0, len(weighted))
		for _, wu := range weighted {
			upstreams = append(upstreams, wu.URL)
		}
	}
	for _, upstream := range upstreams {
		targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(cleanPath)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return MetadataBlob{}, err
		}
		request.Header.Set("User-Agent", h.client.UserAgent)
		if cached != nil {
			if etag := cached.Headers["ETag"]; etag != "" {
				request.Header.Set("If-None-Match", etag)
			}
			if lastMod := cached.Headers["Last-Modified"]; lastMod != "" {
				request.Header.Set("If-Modified-Since", lastMod)
			}
		}

		response, err := h.client.Do(request)
		if err != nil {
			h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, 0)
			continue
		}
		h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, response.StatusCode)
		response.Body = utils.NewRateLimitReader(response.Body)

		if response.StatusCode == http.StatusNotModified && cached != nil {
			_ = response.Body.Close()
			h.touchMetadataObject(ctx, cleanPath)
			return *cached, nil
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			switch response.StatusCode {
			case http.StatusNotFound, http.StatusGone:
				notFound++
			case http.StatusUnauthorized, http.StatusForbidden:
				forbidden++
			default:
				transient++
			}
			continue
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			return MetadataBlob{}, err
		}
		headers := map[string]string{}
		for key, value := range response.Header {
			if len(value) > 0 {
				headers[http.CanonicalHeaderKey(key)] = value[0]
			}
		}
		if err := h.putObject(ctx, cleanPath, body, headers, nil); err != nil {
			return MetadataBlob{}, err
		}
		return MetadataBlob{Path: cleanPath, Body: body, Headers: headers}, nil
	}

	if cached != nil {
		if transient > 0 || (notFound == 0 && forbidden == 0) {
			return *cached, nil
		}
	}
	switch {
	case forbidden > 0:
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataForbidden}
	case transient > 0:
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataTransient}
	case notFound > 0:
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataNotFound}
	default:
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataTransient}
	}
}

func (h *IndexedHandler) prepareRequest(ctx context.Context, cleanPath string) {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		return
	}
	switch h.classify(cleanPath) {
	case ResourceArtifact:
		liveIdentity, live := snapshot.Artifacts[cleanPath]
		if !live {
			h.deleteObject(ctx, cleanPath)
			h.deleteAuxiliaryCompanions(ctx, snapshot, cleanPath)
			return
		}
		h.invalidateObjectByIdentity(ctx, cleanPath, liveIdentity)
	case ResourceAuxiliary:
		liveIdentity, live := snapshot.Auxiliary[cleanPath]
		if !live {
			h.deleteObject(ctx, cleanPath)
			return
		}
		if liveIdentity != "" {
			h.invalidateObjectByIdentity(ctx, cleanPath, liveIdentity)
		}
	}
}

func (h *IndexedHandler) invalidateObjectByIdentity(ctx context.Context, cleanPath, liveIdentity string) {
	if liveIdentity == "" {
		return
	}
	info, err := h.store.StatObject(ctx, h.name, path.Join(h.objectRoot, cleanPath))
	if err != nil {
		return
	}
	if info.Options["indexed-identity"] == liveIdentity {
		return
	}
	h.deleteObject(ctx, cleanPath)
}

func (h *IndexedHandler) deleteAuxiliaryCompanions(ctx context.Context, snapshot *LiveSnapshot, artifactPath string) {
	for auxPath := range snapshot.Auxiliary {
		if snapshot.Auxiliary[auxPath] == snapshot.Artifacts[artifactPath] {
			h.deleteObject(ctx, auxPath)
		}
	}
}

func (h *IndexedHandler) deleteObject(ctx context.Context, cleanPath string) {
	if err := h.store.DeleteObject(ctx, h.name, path.Join(h.objectRoot, cleanPath)); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, fs.ErrNotExist) {
		slog.Debug("indexed object delete failed", "instance", h.name, "path", cleanPath, "err", err)
	}
}

func (h *IndexedHandler) extraObjectMetadata(req *http.Request, route httpcache.Route, _ map[string]string, _ string) map[string]string {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		return nil
	}
	cleanPath := strings.TrimPrefix(route.ObjectPath, h.objectRoot+"/")
	switch h.classify(cleanPath) {
	case ResourceArtifact:
		if identity := snapshot.Artifacts[cleanPath]; identity != "" {
			return map[string]string{"indexed-identity": identity}
		}
	case ResourceAuxiliary:
		if identity := snapshot.Auxiliary[cleanPath]; identity != "" {
			return map[string]string{"indexed-identity": identity}
		}
	}
	return nil
}

func (h *IndexedHandler) putObject(ctx context.Context, cleanPath string, body []byte, headers map[string]string, extra map[string]string) error {
	objectPath := path.Join(h.objectRoot, cleanPath)
	meta := map[string]string{
		"content-type":   headers["Content-Type"],
		"content-length": headers["Content-Length"],
		"last-modified":  headers["Last-Modified"],
		"etag":           headers["ETag"],
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           h.mode,
		"cache":          "REFRESH",
	}
	if meta["content-length"] == "" {
		meta["content-length"] = strconv.Itoa(len(body))
	}
	for key, value := range extra {
		if value != "" {
			meta[key] = value
		}
	}
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			return err
		}
	}
	_, err := h.store.Put(ctx, h.name, objectPath, bytes.NewReader(body), meta)
	return err
}

func targetsToProbe(targets []MetadataTarget) []health.ProbeTarget {
	pt := make([]health.ProbeTarget, 0, len(targets))
	for _, t := range targets {
		pt = append(pt, health.ProbeTarget{Path: t.URL})
	}
	return pt
}

func (h *IndexedHandler) addRoot(path string, targets []MetadataTarget) {
	h.mu.Lock()
	if _, ok := h.targets[path]; ok {
		h.targets[path] = targets
		h.mu.Unlock()
	} else {
		h.targets[path] = targets
		h.mu.Unlock()
	}
	if h.health != nil {
		h.health.AddResource(path, targetsToProbe(targets), h.upstreams)
	}
}

func (h *IndexedHandler) discoverRoot(ctx context.Context, cleanPath string) {
	if h.discover == nil || h.classify(cleanPath) != ResourceMetadata {
		return
	}
	spec, ok := h.discover.Discover(cleanPath)
	if !ok {
		return
	}
	key := spec.Key()

	h.mu.Lock()
	_, existing := h.targets[key]
	h.mu.Unlock()
	if existing {
		return
	}

	targets := spec.Targets()
	h.addRoot(key, targets)
	if h.refreshTrigger != nil {
		select {
		case h.refreshTrigger <- struct{}{}:
		default:
		}
		return
	}
	h.wait.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("refresh root panic", "instance", h.name, "key", key, "panic", r)
			}
		}()
		defer h.wait.Done()
		_, _ = h.refreshRoot(ctx, key, time.Now())
	}()
}

func (h *IndexedHandler) refreshRoot(ctx context.Context, path string, now time.Time) (bool, error) {
	if h.health == nil {
		return false, nil
	}
	rhs, unlock, ok := h.health.TryStartRefresh(path)
	if !ok {
		return false, nil
	}
	defer unlock()

	h.mu.RLock()
	targets, ok := h.targets[path]
	h.mu.RUnlock()
	if !ok || len(targets) == 0 {
		h.health.FinishRefresh(path, rhs.Generation, nil, nil)
		return false, nil
	}

	snapshot, err := h.buildSnapshot(ctx, targets)
	if err != nil {
		h.health.FinishRefresh(path, rhs.Generation, refreshHealthError(err), nil)
		if _, exists := h.health.ResourceState(path); !exists {
			h.mu.Lock()
			delete(h.rootSnapshots, path)
			delete(h.targets, path)
			h.rebuildAggregateLocked()
			h.mu.Unlock()
		}
		return false, err
	}

	h.mu.Lock()
	h.rootSnapshots[path] = snapshot
	h.rebuildAggregateLocked()
	h.mu.Unlock()

	h.health.FinishRefresh(path, rhs.Generation, nil, targetsToProbe(targets))
	return true, nil
}

func (h *IndexedHandler) refreshDueRoots(ctx context.Context, now time.Time) (bool, error) {
	if h.health == nil {
		return false, nil
	}
	resources := h.health.ActiveResources()
	refreshed := false
	var firstErr error
	for _, rh := range resources {
		nextRefresh, ok := h.health.ResourceNextRefresh(rh.Path)
		if ok && !nextRefresh.IsZero() && nextRefresh.After(now) {
			continue
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

func (h *IndexedHandler) refreshAllRoots(ctx context.Context, now time.Time) (bool, error) {
	if h.health == nil {
		return false, nil
	}
	resources := h.health.ActiveResources()
	refreshed := false
	var firstErr error
	for _, rh := range resources {
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

func (h *IndexedHandler) rebuildAggregateLocked() {
	aggregate := &LiveSnapshot{
		Metadata:  map[string]struct{}{},
		Artifacts: map[string]string{},
		Auxiliary: map[string]string{},
	}
	for _, snap := range h.rootSnapshots {
		for p := range snap.Metadata {
			aggregate.Metadata[p] = struct{}{}
		}
		for p, identity := range snap.Artifacts {
			aggregate.Artifacts[p] = identity
		}
		for p, identity := range snap.Auxiliary {
			aggregate.Auxiliary[p] = identity
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
			aggState := h.health.AggregateState()
			switch aggState {
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

func (h *IndexedHandler) readCachedMetadata(ctx context.Context, cleanPath string) *MetadataBlob {
	objectPath := path.Join(h.objectRoot, cleanPath)
	info, err := h.store.StatObject(ctx, h.name, objectPath)
	if err != nil {
		return nil
	}
	fetchedAt, err := time.Parse(time.RFC3339Nano, info.Options["fetched-at"])
	if err != nil {
		return nil
	}
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil
	}
	headers := map[string]string{}
	for _, key := range []string{"Content-Type", "Content-Length", "Last-Modified", "ETag"} {
		if v := info.Options[strings.ToLower(key)]; v != "" {
			headers[key] = v
		}
	}
	return &MetadataBlob{Path: cleanPath, Body: body, Headers: headers, FetchedAt: fetchedAt}
}

func (h *IndexedHandler) touchMetadataObject(ctx context.Context, cleanPath string) {
	objectPath := path.Join(h.objectRoot, cleanPath)
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return
	}
	if _, err := h.store.Put(ctx, h.name, objectPath, bytes.NewReader(body), map[string]string{
		"content-type":   "application/octet-stream",
		"content-length": strconv.Itoa(len(body)),
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           h.mode,
		"cache":          "HIT",
	}); err != nil {
		slog.Warn("indexed touch failed", "instance", h.name, "path", objectPath, "err", err)
	}
}
