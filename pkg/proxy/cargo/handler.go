package cargo

import (
	"context"
	"net/http"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type handler struct {
	name        string
	expireAfter config.Expiration
	store       *blobfs.Store
	stats       *httpcache.Stats
	base        *httpcache.Handler
}

func newHandler(name, mode string, runtime httpcache.RuntimeConfig, store *blobfs.Store, resolver httpcache.Resolver, stats *httpcache.Stats, expireAfter config.Expiration) *handler {
	base := httpcache.NewHandler(name, runtime, store, resolver, stats, nil)
	return &handler{
		name:        name,
		expireAfter: expireAfter,
		store:       store,
		stats:       stats,
		base:        base,
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, config.ModeCargo, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	h.base.ServeHTTP(w, req)
}

func (h *handler) Start(context.Context) error { return nil }

func (h *handler) Stop(ctx context.Context) error {
	return h.base.CloseContext(ctx)
}

func (h *handler) Cleanup(ctx context.Context) error {
	if h.expireAfter.IsNever() || h.expireAfter.IsUnset() {
		return nil
	}
	return httpcache.CleanupStoreTenant(ctx, h.store, h.name, h.expireAfter.Duration())
}
