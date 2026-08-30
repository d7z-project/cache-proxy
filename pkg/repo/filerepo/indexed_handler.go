package filerepo

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxMetadataObjectSize = 512 << 20
const snapshotSchemaVersion = 2

type rootEntry struct {
	root                        RepositoryRoot
	lastSeenAt                  time.Time
	lastSeenSavedAt             time.Time
	retired                     bool
	retirementCleanupGeneration string
}

type IndexedHandler struct {
	name             string
	mode             string
	objectRoot       string
	store            *blobfs.Store
	stats            *httpcache.Stats
	inspector        PathInspector
	finalizer        RootFinalizer
	validator        SnapshotValidator
	base             *httpcache.Handler
	client           *utils.HTTPClientWrapper
	upstreams        []string
	build            SnapshotBuilder
	serviceHealth    *health.ServiceHealth
	upstreamGate     *httpcache.UpstreamGate
	bus              *bus.Bus
	policy           Policy
	metadataFreshFor time.Duration
	rootExpireAfter  config.Expiration

	mu              sync.RWMutex
	roots           map[string]*rootEntry
	rootSnapshots   map[string]*LiveSnapshot
	currentView     map[string]currentViewEntry
	metadataReaders map[string]int
	lifecycleCtx    context.Context
	wait            sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, inspector PathInspector, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats, svcHealth *health.ServiceHealth, upstreamGate *httpcache.UpstreamGate) *IndexedHandler {
	ApplyDefaults(policy)
	handler := &IndexedHandler{
		name:            name,
		mode:            mode,
		objectRoot:      objectRoot,
		store:           store,
		stats:           stats,
		inspector:       inspector,
		upstreams:       append([]string(nil), upstreams...),
		build:           builder,
		serviceHealth:   svcHealth,
		upstreamGate:    upstreamGate,
		policy:          *policy,
		roots:           map[string]*rootEntry{},
		rootSnapshots:   map[string]*LiveSnapshot{},
		currentView:     map[string]currentViewEntry{},
		metadataReaders: map[string]int{},
	}
	if finalizer, ok := inspector.(RootFinalizer); ok {
		handler.finalizer = finalizer
	}
	if validator, ok := inspector.(SnapshotValidator); ok {
		handler.validator = validator
	}
	handler.base = httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:         mode,
		ExpireAfter:  expireAfter,
		Upstreams:    append([]string(nil), upstreams...),
		Transport:    transport,
		PassHeaders:  append([]string(nil), policy.PassHeaders...),
		BusyPolicy:   policy.AuxiliaryBusyPolicy,
		UpstreamGate: upstreamGate,
	}, store, &generationResolver{handler: handler, policy: policy}, stats, svcHealth)
	handler.client = utils.DefaultHTTPClientWrapper()
	httpcache.ConfigureClientTransport(handler.client, name, transport)
	handler.reportMetadataState()
	return handler
}

func (h *IndexedHandler) SetBus(b *bus.Bus) { h.bus = b }

func (h *IndexedHandler) SetMetadataFreshFor(value config.Freshness) {
	h.metadataFreshFor = value.Duration()
}

func (h *IndexedHandler) SetRootExpireAfter(value config.Expiration) {
	h.rootExpireAfter = value
}

func (h *IndexedHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "." {
		cleanPath = ""
	}
	if cleanPath == "" {
		h.base.ProxyPassthrough(w, req, "", "")
		return
	}
	if current, release, ok := h.lookupCurrentRequest(cleanPath); ok {
		if current.Class == ResourceMetadata {
			h.touchRoot(current.RootID)
			if h.currentPrimaryNeedsRefresh(current.RootID, cleanPath) {
				h.publishRefreshRequested(current.RootID)
			}
			h.serveCurrentMetadata(w, req, current, release)
			return
		}
		h.base.ServeHTTP(w, req)
		return
	}
	analysis := h.inspect(cleanPath)
	class := analysis.Class
	if class == ResourceUnknown {
		h.base.ProxyPassthrough(w, req, cleanPath, "")
		return
	}
	if class == ResourceMetadata {
		if rootID, snapshot, known := h.matchRepository(cleanPath); known {
			if snapshot != nil {
				if analysis.Role == DiscoveryUpdateRoot && analysis.Root.ID == rootID {
					h.registerRoot(analysis)
				}
				h.publishRefreshRequested(rootID)
				w.Header().Set("Retry-After", "1")
				w.Header().Set("X-Cache-Generation", snapshot.Generation)
				err := fmt.Errorf("metadata path %s is absent from current generation %s", cleanPath, snapshot.Generation)
				_ = httpcache.ErrorResponse(http.StatusServiceUnavailable, err).FlushClose(req, w)
				h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusServiceUnavailable, 0)
				return
			}
			if analysis.Role != DiscoveryIgnore && analysis.Root.ID == rootID {
				h.registerRoot(analysis)
			}
			h.touchRoot(rootID)
			h.publishRefreshRequested(rootID)
			h.base.ProxyPassthrough(w, req, cleanPath, "")
			return
		}
		if analysis.Role == DiscoveryIgnore {
			_ = httpcache.ErrorResponse(http.StatusNotFound, fmt.Errorf("metadata repository for %s is not discovered", cleanPath)).FlushClose(req, w)
			h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusNotFound, 0)
			return
		}
		status := h.base.ProxyPassthroughStatus(w, req, cleanPath, "")
		if status < http.StatusOK || (status >= http.StatusMultipleChoices && status != http.StatusNotModified) {
			return
		}
		rootID, created, changed := h.registerRoot(analysis)
		if h.bus != nil && rootID != "" && (created || changed) {
			h.publishDiscovered(rootID)
		}
		return
	}
	if current, ok := h.lookupCurrentContent(cleanPath, class); ok {
		h.touchRoot(current.RootID)
		h.base.ServeHTTP(w, req)
		return
	}
	h.base.ProxyPassthrough(w, req, cleanPath, "")
}

func (h *IndexedHandler) Start(ctx context.Context) error {
	h.lifecycleCtx = ctx
	h.restoreRoots(ctx)
	h.cleanCurrentRefTemps(ctx)
	h.restoreGenerations(ctx)
	h.reconcileMetadataTasks()
	return nil
}

func (h *IndexedHandler) Stop(ctx context.Context) error {
	if err := utils.WaitGroupContext(ctx, &h.wait); err != nil {
		return err
	}
	return h.base.CloseContext(ctx)
}

func (h *IndexedHandler) inspect(cleanPath string) DiscoveryResult {
	if h.inspector == nil {
		return DiscoveryResult{Class: ResourceSidecar}
	}
	return h.inspector.InspectPath(cleanPath)
}

func (h *IndexedHandler) rootSnapshot(rootKey string) *LiveSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rootSnapshots[rootKey]
}

func (h *IndexedHandler) publishDiscovered(rootID string) {
	if h.bus == nil || rootID == "" {
		return
	}
	h.bus.Publish(bus.Event{
		Type: bus.EventMetadataDiscovered,
		Payload: bus.MetadataDiscoveredPayload{
			Instance: h.name,
			RootID:   rootID,
		},
	})
}

func (h *IndexedHandler) publishRefreshRequested(rootID string) {
	if h.bus == nil || rootID == "" {
		return
	}
	h.bus.Publish(bus.Event{
		Type:    bus.EventMetadataRefreshRequested,
		Payload: bus.MetadataRefreshRequestedPayload{Instance: h.name, RootID: rootID},
	})
}

func (h *IndexedHandler) currentPrimaryNeedsRefresh(rootID, cleanPath string) bool {
	if h.metadataFreshFor <= 0 {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	snapshot := h.rootSnapshots[rootID]
	entry := h.roots[rootID]
	if snapshot == nil || entry == nil || time.Since(snapshot.Published) < h.metadataFreshFor {
		return false
	}
	return slices.Contains(entry.root.PrimaryMetadata, cleanPath)
}

func (h *IndexedHandler) reconcileMetadataTasks() {
	h.mu.RLock()
	rootIDs := make([]string, 0, len(h.roots))
	for rootID := range h.roots {
		rootIDs = append(rootIDs, rootID)
	}
	h.mu.RUnlock()
	for _, rootID := range rootIDs {
		h.publishDiscovered(rootID)
	}
}

func (h *IndexedHandler) canSkipRefresh(ctx context.Context, snapshot *LiveSnapshot, upstream string, targets []MetadataTarget) (bool, error) {
	if snapshot == nil || len(targets) == 0 {
		return false, nil
	}
	if _, err := h.store.StatObject(ctx, h.name, h.cleanupIndexPath(snapshot.RootID, snapshot.Generation)); err != nil {
		return false, nil
	}
	for _, target := range targets {
		resolved, ok := resolveSnapshotMetadata(snapshot, target)
		if !ok || resolved.StorePath == "" {
			return false, nil
		}
		info, err := h.store.StatObject(ctx, h.name, resolved.StorePath)
		if err != nil {
			return false, nil
		}
		etag := info.Options["etag"]
		lastModified := info.Options["last-modified"]
		if etag == "" && lastModified == "" {
			return false, nil
		}

		request, err := http.NewRequestWithContext(ctx, http.MethodHead, strings.TrimRight(upstream, "/")+"/"+httpcache.EscapePath(resolved.Path), nil)
		if err != nil {
			return false, err
		}
		request.Header.Set("User-Agent", h.client.UserAgent)
		if etag != "" {
			request.Header.Set("If-None-Match", etag)
		}
		if lastModified != "" {
			request.Header.Set("If-Modified-Since", lastModified)
		}

		release, err := h.upstreamGate.Acquire(ctx, upstream, httpcache.AdmissionRefresh)
		if err != nil {
			return false, err
		}
		start := time.Now()
		response, err := h.client.Do(request)
		latency := time.Since(start)
		if err != nil {
			release()
			h.stats.RecordUpstreamRequest(h.name, h.mode, upstream, http.MethodHead, 0, latency, 0)
			if ctx.Err() != nil {
				return false, err
			}
			return false, fmt.Errorf("%w: metadata head request: %v", errMetadataMirrorRetry, err)
		}
		h.stats.RecordUpstreamRequest(
			h.name,
			h.mode,
			upstream,
			http.MethodHead,
			response.StatusCode,
			latency,
			metadataContentLength(response),
		)
		_ = response.Body.Close()
		release()
		if response.StatusCode == http.StatusTooManyRequests {
			return false, h.upstreamGate.RateLimited(upstream, response.Header.Get("Retry-After"))
		}
		switch response.StatusCode {
		case http.StatusNotModified:
			continue
		case http.StatusOK:
			return false, nil
		case http.StatusMethodNotAllowed, http.StatusNotImplemented:
			return false, nil
		default:
			return false, nil
		}
	}
	return true, nil
}

func resolveSnapshotMetadata(snapshot *LiveSnapshot, target MetadataTarget) (MetadataObject, bool) {
	if snapshot == nil {
		return MetadataObject{}, false
	}
	for _, candidate := range append([]string{target.URL}, target.Candidates...) {
		obj, ok := snapshot.Metadata[candidate]
		if !ok || obj.StatusCode != 0 {
			continue
		}
		if obj.Path == "" || obj.Path == candidate {
			return obj, obj.StorePath != ""
		}
		resolved, ok := snapshot.Metadata[obj.Path]
		if !ok {
			return MetadataObject{}, false
		}
		if resolved.Path == "" {
			resolved.Path = obj.Path
		}
		return resolved, resolved.StorePath != ""
	}
	return MetadataObject{}, false
}
