package httpcache

import (
	"gopkg.d7z.net/blobfs"
	"log/slog"
	"net/http"
	"net/url"
	"sync"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type Route struct {
	ObjectPath     string
	UpstreamPath   string
	TargetURL      string
	Policy         string
	FreshFor       config.Freshness
	BusyPolicy     string
	ExpireAfter    config.Expiration
	RequestHeaders map[string]string
	RewriteKind    string
}

type Resolver interface {
	Resolve(req *http.Request) (Route, error)
}

type RuntimeConfig struct {
	Mode            string
	ExpireAfter     config.Expiration
	Upstreams       []string
	Transport       *config.TransportConfig
	BusyPolicy      string
	DefaultFreshFor config.Freshness
	PassHeaders     []string
	MetadataFunc    func(*http.Request, Route, map[string]string, string) map[string]string
}

type Handler struct {
	name     string
	config   RuntimeConfig
	store    *blobfs.Store
	client   *utils.HttpClientWrapper
	locks    *utils.RWLockGroup
	resolver Resolver
	stats    *Stats
	wait     sync.WaitGroup
}

type remoteOptions struct {
	AcceptErrors bool
	Record       bool
}

func NewHandler(name string, runtime RuntimeConfig, store *blobfs.Store, resolver Resolver, stats *Stats) *Handler {
	client := utils.DefaultHttpClientWrapper()
	if runtime.Transport != nil {
		if runtime.Transport.UserAgent != "" {
			client.UserAgent = runtime.Transport.UserAgent
		}
		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			slog.Warn("cannot configure transport options, unexpected transport type", "instance", name)
		} else {
			if runtime.Transport.Proxy != "" {
				proxyURL, err := url.Parse(runtime.Transport.Proxy)
				if err == nil {
					transport.Proxy = http.ProxyURL(proxyURL)
				} else {
					slog.Warn("invalid transport proxy URL", "instance", name, "proxy", runtime.Transport.Proxy, "err", err)
				}
			}
			if runtime.Transport.Timeout > 0 {
				transport.DialContext = utils.DefaultDialContext(runtime.Transport.Timeout.Duration())
			}
		}
	}
	return &Handler{name: name, config: runtime, store: store, client: client, locks: utils.NewRWLockGroup(), resolver: resolver, stats: stats}
}

func (h *Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		resp.Header().Set("Allow", "GET, HEAD")
		http.Error(resp, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, h.config.Mode, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	result, err := h.handle(req.Context(), req)
	if err != nil {
		slog.Warn("proxy request failed", "instance", h.name, "mode", h.config.Mode, "method", req.Method, "path", req.URL.Path, "err", err)
		http.Error(resp, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, h.config.Mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	status := result.StatusCode
	cache := result.Headers["X-Cache"]
	bytes := responseBytes(result.Headers)
	if err := result.FlushClose(req, resp); err != nil {
		slog.Warn("flush response failed", "instance", h.name, "err", err)
		if status < 500 {
			status = http.StatusBadGateway
		}
	}
	h.stats.RecordRequest(h.name, h.config.Mode, req.Method, cache, status, bytes)
}

func (h *Handler) Close() {
	h.wait.Wait()
}
