package cargo

import (
	"context"
	"net/http"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type handler struct {
	base *httpcache.Handler
}

func newHandler(name string, runtime httpcache.RuntimeConfig, store *blobfs.Store, resolver httpcache.Resolver, stats *httpcache.Stats) *handler {
	base := httpcache.NewHandler(name, runtime, store, resolver, stats, nil)
	return &handler{base: base}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.base.ServeHTTP(w, req)
}

func (h *handler) Start(context.Context) error { return nil }

func (h *handler) Stop(ctx context.Context) error {
	return h.base.CloseContext(ctx)
}

func (h *handler) Cleanup(ctx context.Context) error {
	return h.base.Cleanup(ctx)
}
