package filerepo

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
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
	root RepositoryRoot
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
	lifecycleCtx  context.Context
	wait          sync.WaitGroup
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
		rootID, created := h.discoverRoot(cleanPath)
		if h.tryServeMetadata(w, req, cleanPath) {
			return
		}
		if created && h.bus != nil {
			h.bus.Publish(bus.Event{
				Type: bus.EventMetadataDiscovered,
				Payload: bus.MetadataDiscoveredPayload{
					Instance: h.name,
					RootID:   rootID,
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

func (h *IndexedHandler) rootSnapshot(rootKey string) *LiveSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rootSnapshots[rootKey]
}

func (h *IndexedHandler) canSkipRefresh(ctx context.Context, snapshot *LiveSnapshot, upstream string, targets []MetadataTarget) (bool, error) {
	if snapshot == nil || len(targets) == 0 {
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

		response, err := h.client.Do(request)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return false, err
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return false, err
			}
			return false, nil
		}
		response.Body.Close()
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
		if !ok {
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
