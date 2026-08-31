package filerepo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxMetadataObjectSize = 512 << 20
const snapshotSchemaVersion = 3

type rootEntry struct {
	root                        RepositoryRoot
	closureRevision             uint64
	lastSeenAt                  time.Time
	lastValidatedAt             time.Time
	lastSeenSavedAt             time.Time
	retired                     bool
	retirementCleanupGeneration string
	retirementCleanupDigest     string
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
	triggerRefresh   func(string)
	policy           Policy
	metadataFreshFor time.Duration
	rootExpireAfter  config.Expiration

	mu               sync.RWMutex
	stateWriteMu     sync.Mutex
	stateLifecycleMu sync.Mutex
	stateSignal      chan struct{}
	stateStop        chan chan error
	stateWriterDone  chan struct{}
	stateStopped     bool
	stateRevision    uint64
	roots            map[string]*rootEntry
	rootSnapshots    map[string]*LiveSnapshot
	currentView      map[string]currentViewEntry
	metadataReaders  map[string]int
	refreshing       map[string]bool
	refreshErrors    map[string]string
	lifecycleCtx     context.Context
	wait             sync.WaitGroup
}

func NewIndexedHandler(name, mode, objectRoot string, inspector PathInspector, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, builder SnapshotBuilder, store *blobfs.Store, stats *httpcache.Stats, svcHealth *health.ServiceHealth, upstreamGate *httpcache.UpstreamGate) *IndexedHandler {
	ApplyPolicyDefaults(policy)
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
		refreshing:      map[string]bool{},
		refreshErrors:   map[string]string{},
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

func (h *IndexedHandler) SetRefreshTrigger(trigger func(string)) { h.triggerRefresh = trigger }

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
				h.triggerMetadataRefresh(current.RootID)
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
				if analysis.Role != DiscoveryIgnore && analysis.Root.ID == rootID {
					h.registerRoot(analysis)
				}
				h.triggerMetadataRefresh(rootID)
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
			h.triggerMetadataRefresh(rootID)
			h.base.ProxyPassthrough(w, req, cleanPath, "")
			return
		}
		if analysis.Role == DiscoveryIgnore {
			h.base.ProxyPassthrough(w, req, cleanPath, "")
			return
		}
		status := h.base.ProxyPassthroughStatus(w, req, cleanPath, "")
		if status < http.StatusOK || (status >= http.StatusMultipleChoices && status != http.StatusNotModified) {
			return
		}
		rootID, created, changed := h.registerRoot(analysis)
		if rootID != "" && (created || changed) {
			h.triggerMetadataRefresh(rootID)
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
	h.startStateWriter()
	if err := h.restoreRoots(ctx); err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stopErr := h.stopStateWriter(stopCtx)
		cancel()
		return errors.Join(fmt.Errorf("restore repository roots: %w", err), stopErr)
	}
	h.cleanCurrentRefTemps(ctx)
	if err := h.restoreGenerations(ctx); err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stopErr := h.stopStateWriter(stopCtx)
		cancel()
		return errors.Join(fmt.Errorf("restore metadata generations: %w", err), stopErr)
	}
	h.reconcileMetadataTasks()
	return nil
}

func (h *IndexedHandler) Stop(ctx context.Context) error {
	if err := h.stopStateWriter(ctx); err != nil {
		return err
	}
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

func (h *IndexedHandler) triggerMetadataRefresh(rootID string) {
	if h.triggerRefresh == nil || rootID == "" {
		return
	}
	h.triggerRefresh(rootID)
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
		h.triggerMetadataRefresh(rootID)
	}
}
