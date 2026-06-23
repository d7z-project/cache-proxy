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
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const DefaultMetadataRefreshTimeout = 2 * time.Minute

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
	Path    string
	Body    []byte
	Headers map[string]string
}

type LiveSnapshot struct {
	Metadata  map[string]struct{}
	Artifacts map[string]string
	Auxiliary map[string]string
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
	targets    []MetadataTarget
	policy     RefreshPolicy
	build      SnapshotBuilder

	mu         sync.RWMutex
	state      RefreshState
	snapshot   *LiveSnapshot
	refreshing bool
	wait       sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, metadataFreshFor config.Freshness, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, refreshPolicy RefreshPolicy, targets []MetadataTarget, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats) *IndexedHandler {
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
		targets:    append([]MetadataTarget(nil), targets...),
		policy:     refreshPolicy,
		build:      builder,
		state:      RefreshStateBooting,
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
	handler.stats.SetMetadataState(handler.name, handler.mode, string(handler.state), false)
	return handler
}

func (h *IndexedHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := cleanRequestPath(req.URL.Path)
	if cleanPath != "" {
		h.prepareRequest(req.Context(), cleanPath)
	}
	h.base.ServeHTTP(w, req)
}

func (h *IndexedHandler) Start(ctx context.Context) error {
	h.runRefreshCycle(ctx)
	if h.policy.Interval <= 0 || len(h.targets) == 0 || h.build == nil {
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
			}
		}
	}()
	return nil
}

func (h *IndexedHandler) Stop(context.Context) error {
	h.wait.Wait()
	h.base.Close()
	return nil
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
			slog.Warn("indexed cleanup delete failed", "instance", h.name, "path", objectPath, "err", err)
		}
		return nil
	})
}

func (h *IndexedHandler) Refresh(ctx context.Context) error {
	return h.refresh(ctx)
}

func (h *IndexedHandler) runRefreshCycle(ctx context.Context) {
	if len(h.targets) == 0 || h.build == nil || ctx.Err() != nil || !h.beginRefresh() {
		return
	}
	startedAt := time.Now()
	refreshCtx := ctx
	if h.policy.Timeout > 0 {
		var cancel context.CancelFunc
		refreshCtx, cancel = context.WithTimeout(ctx, h.policy.Timeout)
		defer cancel()
	}

	snapshot, err := h.buildSnapshot(refreshCtx)
	if err != nil {
		h.finishRefresh(err, time.Since(startedAt))
		return
	}
	h.commitSnapshot(snapshot)
	h.finishRefresh(nil, time.Since(startedAt))
	if err := h.Cleanup(refreshCtx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("indexed cleanup failed after refresh", "instance", h.name, "mode", h.mode, "err", err)
	}
}

func (h *IndexedHandler) refresh(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	startedAt := time.Now()
	snapshot, err := h.buildSnapshot(ctx)
	if err != nil {
		h.recordRefreshResult(h.resultForError(err), time.Since(startedAt), h.currentSnapshot() != nil)
		return err
	}
	h.commitSnapshot(snapshot)
	h.recordRefreshResult("success", time.Since(startedAt), true)
	return nil
}

func (h *IndexedHandler) buildSnapshot(ctx context.Context) (*LiveSnapshot, error) {
	session := &RefreshSession{
		handler: h,
		blobs:   map[string]MetadataBlob{},
		targets: append([]MetadataTarget(nil), h.targets...),
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
	return snapshot, nil
}

func (h *IndexedHandler) refreshMetadataObject(ctx context.Context, cleanPath string) (MetadataBlob, error) {
	for _, upstream := range h.upstreams {
		targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(cleanPath)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return MetadataBlob{}, err
		}
		request.Header.Set("User-Agent", h.client.UserAgent)
		response, err := h.client.Do(request)
		if err != nil {
			h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, 0)
			continue
		}
		h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, response.StatusCode)
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
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
	return MetadataBlob{}, errors.New("metadata upstream fetch failed")
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

func (h *IndexedHandler) commitSnapshot(snapshot *LiveSnapshot) {
	h.mu.Lock()
	h.snapshot = snapshot
	h.mu.Unlock()
}

func (h *IndexedHandler) finishRefresh(err error, duration time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.refreshing = false
	if err == nil {
		h.state = RefreshStateReady
		h.stats.SetMetadataState(h.name, h.mode, string(h.state), true)
		h.stats.RecordMetadataRefresh(h.name, h.mode, "success", duration, true)
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
		slog.Warn("metadata refresh failed", "instance", h.name, "mode", h.mode, "state", h.state, "err", err)
	}
}

func (h *IndexedHandler) recordRefreshResult(result string, duration time.Duration, ready bool) {
	state := RefreshStateDegraded
	if result == "success" {
		state = RefreshStateReady
	}
	h.mu.Lock()
	h.state = state
	h.mu.Unlock()
	h.stats.SetMetadataState(h.name, h.mode, string(state), ready)
	h.stats.RecordMetadataRefresh(h.name, h.mode, result, duration, ready)
}

func (h *IndexedHandler) resultForError(err error) string {
	var fetchErr MetadataFetchError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
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

func cleanRequestPath(target string) string {
	cleanPath := strings.TrimPrefix(path.Clean("/"+target), "/")
	if cleanPath == "." {
		return ""
	}
	return cleanPath
}
