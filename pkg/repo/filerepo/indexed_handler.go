package filerepo

import (
	"context"
	"net/http"
	"sync"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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
	rebuild    CleanupIndexBuilder
	sh         *health.ServiceHealth
	bus        *bus.Bus

	mu            sync.RWMutex
	snapshot      *LiveSnapshot
	roots         map[string]*rootEntry
	rootSnapshots map[string]*LiveSnapshot
	cleanupIndex  map[string]cleanupIndex
	lifecycleCtx  context.Context
	wait          sync.WaitGroup
}

type cleanupIndex struct {
	generation string
	paths      []string
}

func NewIndexedHandler(name, mode, objectRoot string, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, discover Discoverer, builder SnapshotBuilder, rebuild CleanupIndexBuilder, store *blobfs.Store, stats *httpcache.Stats, svcHealth *health.ServiceHealth, downloads *httpcache.DownloadLimiter) *IndexedHandler {
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
		rebuild:       rebuild,
		sh:            svcHealth,
		roots:         map[string]*rootEntry{},
		rootSnapshots: map[string]*LiveSnapshot{},
		cleanupIndex:  map[string]cleanupIndex{},
	}
	handler.base = httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:            mode,
		ExpireAfter:     expireAfter,
		Upstreams:       append([]string(nil), upstreams...),
		Transport:       transport,
		PassHeaders:     append([]string(nil), policy.PassHeaders...),
		BusyPolicy:      policy.AuxiliaryBusyPolicy,
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

func (h *IndexedHandler) classify(cleanPath string) ResourceClass {
	if h.classifier == nil {
		return ResourceAuxiliary
	}
	return h.classifier(cleanPath)
}
