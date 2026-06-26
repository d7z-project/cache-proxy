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
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const DefaultMetadataRefreshTimeout = 2 * time.Minute
const DefaultBlockedRetryInterval = time.Hour

var (
	errMetadataNotFound  = errors.New("metadata upstream not found")
	errMetadataTransient = errors.New("metadata upstream transient failure")
	errMetadataForbidden = errors.New("metadata upstream forbidden")
)

func ResolveMetadataRefreshTimeout(value config.Duration) time.Duration {
	if value > 0 {
		return value.Duration()
	}
	return DefaultMetadataRefreshTimeout
}

func ResolveMetadataRefreshInterval(value config.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value.Duration()
	}
	return fallback
}

type RefreshState string

const (
	RefreshStateBooting    RefreshState = "booting"
	RefreshStateReady      RefreshState = "ready"
	RefreshStateRefreshing RefreshState = "refreshing"
	RefreshStateDegraded   RefreshState = "degraded"
)

type RefreshPolicy struct {
	Interval time.Duration
	Timeout  time.Duration
}

type MetadataFetchError struct {
	Path string
	Err  error
}

func (e MetadataFetchError) Error() string { return e.Path + ": " + e.Err.Error() }
func (e MetadataFetchError) Unwrap() error { return e.Err }

type MetadataTarget struct {
	URL        string
	Candidates []string
	Kind       string
	Repo       string
	Arch       string
}

type MetadataBlob struct {
	Path      string
	Body      []byte
	Headers   map[string]string
	FetchedAt time.Time
}

type LiveSnapshot struct {
	Metadata   map[string]struct{}
	Artifacts  map[string]string
	Auxiliary  map[string]string
	Companions map[string][]string // primary metadata path → companion paths invalidated on refresh
}

type SnapshotBuilder func(context.Context, *RefreshSession) (*LiveSnapshot, error)

type RefreshSession struct {
	handler *IndexedHandler
	blobs   map[string]MetadataBlob
	targets []MetadataTarget
}

func (s *RefreshSession) Targets() []MetadataTarget {
	return append([]MetadataTarget(nil), s.targets...)
}

func (s *RefreshSession) Fetch(ctx context.Context, target MetadataTarget) (MetadataBlob, error) {
	candidates := append([]string{target.URL}, target.Candidates...)
	for _, candidate := range candidates {
		if blob, ok := s.blobs[candidate]; ok {
			return blob, nil
		}
	}
	var lastErr error
	for _, candidate := range candidates {
		blob, err := s.handler.refreshMetadataObject(ctx, candidate)
		if err != nil {
			lastErr = err
			continue
		}
		for _, key := range candidates {
			s.blobs[key] = blob
		}
		return blob, nil
	}
	if lastErr == nil {
		lastErr = errors.New("metadata upstream fetch failed")
	}
	return MetadataBlob{}, MetadataFetchError{Path: target.URL, Err: lastErr}
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
	policy     RefreshPolicy
	build      SnapshotBuilder
	removal    RemovalPolicy

	metadataFreshFor time.Duration

	mu             sync.RWMutex
	state          RefreshState
	snapshot       *LiveSnapshot
	roots          map[string]*RepositoryRecord
	refreshing     bool
	refreshTrigger chan struct{}
	wait           sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, metadataFreshFor config.Freshness, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, refreshPolicy RefreshPolicy, discover Discoverer, seeds []RootSpec, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats) *IndexedHandler {
	ApplyDefaults(policy, metadataFreshFor)
	if refreshPolicy.Timeout <= 0 {
		refreshPolicy.Timeout = DefaultMetadataRefreshTimeout
	}
	handler := &IndexedHandler{
		name:       name,
		mode:       mode,
		objectRoot: objectRoot,
		store:      store,
		stats:      stats,
		classifier: classifier,
		upstreams:  append([]string(nil), upstreams...),
		discover:   discover,
		policy:     refreshPolicy,
		build:      builder,
		removal: RemovalPolicy{
			ConsecutiveNotFound: 3,
			MinNotFoundAge:      10 * time.Minute,
		},
		metadataFreshFor: metadataFreshFor.Duration(),
		state:            RefreshStateBooting,
		roots:            map[string]*RepositoryRecord{},
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
	}), stats)
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
		handler.observeRoot(seed, time.Time{})
	}
	handler.stats.SetMetadataState(handler.name, handler.mode, string(handler.state), false)
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
	h.restoreRoots(ctx)
	h.runRefreshCycle(ctx)
	if h.policy.Interval <= 0 || h.build == nil {
		return nil
	}
	h.wait.Add(1)
	go func() {
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
	if err := utils.WaitGroupContext(ctx, &h.wait); err != nil {
		return err
	}
	return h.base.CloseContext(ctx)
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

func (h *IndexedHandler) Refresh(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !h.beginRefresh() {
		return nil
	}
	return h.doRefresh(ctx, true)
}

func (h *IndexedHandler) runRefreshCycle(ctx context.Context) {
	if h.build == nil || ctx.Err() != nil || !h.beginRefresh() {
		return
	}
	h.doRefresh(ctx, false)
}

func (h *IndexedHandler) doRefresh(ctx context.Context, allRoots bool) error {
	startedAt := time.Now()
	refreshCtx := ctx
	if h.policy.Timeout > 0 {
		var cancel context.CancelFunc
		refreshCtx, cancel = context.WithTimeout(ctx, h.policy.Timeout)
		defer cancel()
	}
	var refreshed bool
	var err error
	if allRoots {
		refreshed, err = h.refreshAllRoots(refreshCtx, startedAt)
	} else {
		refreshed, err = h.refreshDueRoots(refreshCtx, startedAt)
	}
	if refreshed {
		if cleanupErr := h.Cleanup(refreshCtx); cleanupErr != nil && !errors.Is(cleanupErr, context.Canceled) {
			slog.Info("indexed cleanup failed after refresh", "instance", h.name, "mode", h.mode, "err", cleanupErr)
		}
	}
	h.finishRefresh(err, time.Since(startedAt))
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
	for _, upstream := range h.upstreams {
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

func (h *IndexedHandler) discoverRoot(ctx context.Context, cleanPath string) {
	if h.discover == nil || h.classify(cleanPath) != ResourceMetadata {
		return
	}
	spec, ok := h.discover.Discover(cleanPath)
	if !ok {
		return
	}
	record, changed := h.observeRoot(spec, time.Now())
	if !changed && record.State != RepositoryStatePending && record.State != RepositoryStateRemoved {
		return
	}
	if h.refreshTrigger != nil {
		select {
		case h.refreshTrigger <- struct{}{}:
		default:
		}
		return
	}
	refreshCtx := ctx
	if h.policy.Timeout > 0 {
		var cancel context.CancelFunc
		refreshCtx, cancel = context.WithTimeout(ctx, h.policy.Timeout)
		defer cancel()
	}
	_, _ = h.refreshRoot(refreshCtx, record, time.Now())
}

func (h *IndexedHandler) observeRoot(spec RootSpec, now time.Time) (*RepositoryRecord, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}
	key := spec.Key()
	record, ok := h.roots[key]
	if !ok {
		record = &RepositoryRecord{
			Spec:          spec,
			State:         RepositoryStatePending,
			LastSeenAt:    now,
			NextRefreshAt: now,
		}
		h.roots[key] = record
		return record, true
	}
	record.LastSeenAt = now
	changed := record.Spec.Merge(spec)
	if changed {
		record.NextRefreshAt = now
	}
	return record, changed
}

func (h *IndexedHandler) refreshDueRoots(ctx context.Context, now time.Time) (bool, error) {
	records := h.snapshotRecords()
	refreshed := false
	var firstErr error
	for _, record := range records {
		if !record.NextRefreshAt.IsZero() && record.NextRefreshAt.After(now) {
			continue
		}
		changed, err := h.refreshRoot(ctx, record, now)
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
	records := h.snapshotRecords()
	refreshed := false
	var firstErr error
	for _, record := range records {
		changed, err := h.refreshRoot(ctx, record, now)
		if changed {
			refreshed = true
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return refreshed, firstErr
}

func (h *IndexedHandler) snapshotRecords() []*RepositoryRecord {
	h.mu.RLock()
	defer h.mu.RUnlock()
	records := make([]*RepositoryRecord, 0, len(h.roots))
	for _, record := range h.roots {
		switch record.State {
		case RepositoryStateRemoved:
			continue
		default:
			records = append(records, record)
		}
	}
	return records
}

func (h *IndexedHandler) refreshRoot(ctx context.Context, record *RepositoryRecord, now time.Time) (bool, error) {
	h.mu.Lock()
	if record.State == RepositoryStateRemoved {
		h.mu.Unlock()
		return false, nil
	}
	targets := record.Spec.Targets()
	if record.State != RepositoryStatePending {
		record.State = RepositoryStateRefreshing
	}
	h.mu.Unlock()
	if len(targets) == 0 {
		return false, nil
	}
	snapshot, err := h.buildSnapshot(ctx, targets)
	if err != nil {
		return h.handleRootFailure(record, err, now)
	}
	h.mu.Lock()
	record.Snapshot = snapshot
	record.State = RepositoryStateActive
	record.LastRefreshAt = now
	record.LastSuccessAt = now
	record.NextRefreshAt = now.Add(h.policy.Interval)
	record.FirstNotFoundAt = time.Time{}
	record.ConsecutiveNotFound = 0
	record.ConsecutiveInvalid = 0
	record.ConsecutiveTransient = 0
	record.LastError = ""
	h.rebuildAggregateLocked()
	h.mu.Unlock()
	return true, nil
}

func (h *IndexedHandler) handleRootFailure(record *RepositoryRecord, err error, now time.Time) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	record.LastRefreshAt = now
	record.LastError = err.Error()
	switch {
	case errors.Is(err, errMetadataNotFound):
		record.ConsecutiveNotFound++
		if record.FirstNotFoundAt.IsZero() {
			record.FirstNotFoundAt = now
		}
		if record.Snapshot == nil {
			delete(h.roots, record.Spec.Key())
			h.rebuildAggregateLocked()
			return false, err
		}
		if record.ConsecutiveNotFound >= h.removal.ConsecutiveNotFound && now.Sub(record.FirstNotFoundAt) >= h.removal.MinNotFoundAge {
			delete(h.roots, record.Spec.Key())
			h.rebuildAggregateLocked()
			return true, err
		}
		record.State = RepositoryStateSuspect
		record.NextRefreshAt = now.Add(h.policy.Interval)
		return false, err
	case errors.Is(err, errMetadataForbidden):
		record.ConsecutiveInvalid++
		record.State = RepositoryStateBlocked
		record.NextRefreshAt = now.Add(DefaultBlockedRetryInterval)
		return false, err
	case errors.Is(err, errMetadataTransient), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		record.ConsecutiveTransient++
		if record.Snapshot != nil {
			record.State = RepositoryStateSuspect
		}
		record.NextRefreshAt = now.Add(h.policy.Interval)
		return false, err
	default:
		record.ConsecutiveInvalid++
		if record.ConsecutiveInvalid >= 3 {
			record.State = RepositoryStateBlocked
			record.NextRefreshAt = now.Add(DefaultBlockedRetryInterval)
			return false, err
		}
		if record.Snapshot != nil {
			record.State = RepositoryStateSuspect
		}
		record.NextRefreshAt = now.Add(h.policy.Interval)
		return false, err
	}
}

func (h *IndexedHandler) rebuildAggregateLocked() {
	aggregate := &LiveSnapshot{
		Metadata:  map[string]struct{}{},
		Artifacts: map[string]string{},
		Auxiliary: map[string]string{},
	}
	for _, record := range h.roots {
		if record.Snapshot == nil {
			continue
		}
		switch record.State {
		case RepositoryStatePending, RepositoryStateRemoved:
			continue
		}
		for path := range record.Snapshot.Metadata {
			aggregate.Metadata[path] = struct{}{}
		}
		for path, identity := range record.Snapshot.Artifacts {
			aggregate.Artifacts[path] = identity
		}
		for path, identity := range record.Snapshot.Auxiliary {
			aggregate.Auxiliary[path] = identity
		}
	}
	h.snapshot = aggregate
}

func (h *IndexedHandler) beginRefresh() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.refreshing {
		return false
	}
	h.refreshing = true
	h.state = RefreshStateRefreshing
	h.stats.SetMetadataState(h.name, h.mode, string(h.state), h.snapshot != nil)
	return true
}

func (h *IndexedHandler) finishRefresh(err error, duration time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.refreshing = false
	if err == nil {
		ready := h.snapshot != nil
		if ready {
			h.state = RefreshStateReady
		} else {
			h.state = RefreshStateBooting
		}
		h.stats.SetMetadataState(h.name, h.mode, string(h.state), ready)
		h.stats.RecordMetadataRefresh(h.name, h.mode, "success", duration, ready)
		return
	}
	if errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if h.snapshot == nil {
			h.state = RefreshStateBooting
			h.stats.SetMetadataState(h.name, h.mode, string(h.state), false)
		} else {
			h.state = RefreshStateReady
			h.stats.SetMetadataState(h.name, h.mode, string(h.state), true)
		}
		return
	}
	h.state = RefreshStateDegraded
	ready := h.snapshot != nil
	h.stats.SetMetadataState(h.name, h.mode, string(h.state), ready)
	h.stats.RecordMetadataRefresh(h.name, h.mode, h.resultForError(err), duration, ready)
	if !errors.Is(err, context.Canceled) {
		slog.Info("metadata refresh failed", "instance", h.name, "mode", h.mode, "state", h.state, "err", err)
	}
}

func (h *IndexedHandler) resultForError(err error) string {
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

func (h *IndexedHandler) currentSnapshot() *LiveSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.snapshot
}

func (h *IndexedHandler) classify(cleanPath string) ResourceClass {
	if h.classifier == nil {
		return ResourceAuxiliary
	}
	return h.classifier(cleanPath)
}

func (h *IndexedHandler) RootRecord(key string) (RepositoryRecord, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rec, ok := h.roots[key]
	if !ok {
		return RepositoryRecord{}, false
	}
	return *rec, true
}

func (h *IndexedHandler) HasRoot(key string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.roots[key]
	return ok
}

func (h *IndexedHandler) SetRootRecord(key string, record *RepositoryRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.roots[key] = record
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
	_, _ = h.store.Put(ctx, h.name, objectPath, bytes.NewReader(body), map[string]string{
		"content-type":   "application/octet-stream",
		"content-length": strconv.Itoa(len(body)),
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           h.mode,
		"cache":          "HIT",
	})
}

func (h *IndexedHandler) restoreRoots(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.discover == nil {
		return
	}
	prefix := h.objectRoot + "/"
	specByKey := map[string]RootSpec{}
	_ = fs.WalkDir(h.store.TenantFS(h.name), h.objectRoot, func(objectPath string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || strings.HasPrefix(path.Base(objectPath), "_") {
			return nil
		}
		cleanPath := strings.TrimPrefix(objectPath, prefix)
		if h.classify(cleanPath) != ResourceMetadata {
			return nil
		}
		if spec, ok := h.discover.Discover(cleanPath); ok {
			specByKey[spec.Key()] = spec
		}
		return nil
	})

	persisted := h.loadState(ctx)
	now := time.Now()
	for _, spec := range specByKey {
		record, _ := h.observeRootLocked(spec, now)
		if pr, ok := persisted.Roots[spec.Key()]; ok {
			record.State = RepositoryState(pr.State)
			record.LastRefreshAt = pr.LastRefreshAt
			record.LastSuccessAt = pr.LastSuccessAt
			record.ConsecutiveNotFound = pr.ConsecutiveNotFound
			record.ConsecutiveInvalid = pr.ConsecutiveInvalid
			record.ConsecutiveTransient = pr.ConsecutiveTransient
			record.LastError = pr.LastError
		}
		if record.State != RepositoryStateRemoved {
			record.NextRefreshAt = now
		}
	}
}

func (h *IndexedHandler) observeRootLocked(spec RootSpec, now time.Time) (*RepositoryRecord, bool) {
	key := spec.Key()
	record, ok := h.roots[key]
	if !ok {
		record = &RepositoryRecord{
			Spec:          spec,
			State:         RepositoryStatePending,
			LastSeenAt:    now,
			NextRefreshAt: now,
		}
		h.roots[key] = record
		return record, true
	}
	record.LastSeenAt = now
	changed := record.Spec.Merge(spec)
	if changed {
		record.NextRefreshAt = now
	}
	return record, changed
}

const stateFileName = "_state.yaml"

func (h *IndexedHandler) statePath() string {
	return path.Join(h.objectRoot, stateFileName)
}

func (h *IndexedHandler) saveState(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	h.mu.RLock()
	state := persistedState{Version: 1, Roots: map[string]rootStateRecord{}}
	for key, record := range h.roots {
		if record.State == RepositoryStateRemoved {
			continue
		}
		state.Roots[key] = rootStateRecord{
			State:                string(record.State),
			LastRefreshAt:        record.LastRefreshAt,
			LastSuccessAt:        record.LastSuccessAt,
			ConsecutiveNotFound:  record.ConsecutiveNotFound,
			ConsecutiveInvalid:   record.ConsecutiveInvalid,
			ConsecutiveTransient: record.ConsecutiveTransient,
			LastError:            record.LastError,
		}
	}
	h.mu.RUnlock()

	data, err := yaml.Marshal(state)
	if err != nil {
		slog.Warn("indexed state marshal failed", "instance", h.name, "err", err)
		return
	}
	stateObj := h.statePath()
	_ = h.store.MkdirAll(path.Join(h.name, h.objectRoot), 0o755)
	_, _ = h.store.Put(ctx, h.name, stateObj, bytes.NewReader(data), map[string]string{})
}

func (h *IndexedHandler) loadState(ctx context.Context) persistedState {
	stateObj := h.statePath()
	reader, err := h.store.OpenObject(ctx, h.name, stateObj)
	if err != nil {
		return persistedState{Version: 1, Roots: map[string]rootStateRecord{}}
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return persistedState{Version: 1, Roots: map[string]rootStateRecord{}}
	}
	var state persistedState
	if err := yaml.Unmarshal(data, &state); err != nil {
		slog.Warn("indexed state unmarshal failed", "instance", h.name, "err", err)
		return persistedState{Version: 1, Roots: map[string]rootStateRecord{}}
	}
	if state.Roots == nil {
		state.Roots = map[string]rootStateRecord{}
	}
	return state
}
