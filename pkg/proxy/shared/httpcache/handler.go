package httpcache

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"sync"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
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
	AuthRequired   bool
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
	name   string
	config RuntimeConfig
	store  *blobfs.Store
	client *utils.HttpClientWrapper
	locks  *utils.RWLockGroup
	resolver  Resolver
	stats     *Stats
	health    *health.ServiceHealth
	wait      sync.WaitGroup
	downloads sync.Map
}

type remoteOptions struct {
	AcceptErrors bool
	Record       bool
	TargetURL    string
}

func NewHandler(name string, runtime RuntimeConfig, store *blobfs.Store, resolver Resolver, stats *Stats, svcHealth *health.ServiceHealth) *Handler {
	client := utils.DefaultHttpClientWrapper()
	client.UserAgent = ModeUserAgent(runtime.Mode)
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
	return &Handler{name: name, config: runtime, store: store, client: client, locks: utils.NewRWLockGroup(), resolver: resolver, stats: stats, health: svcHealth}
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
		slog.Info("proxy request failed", "instance", h.name, "mode", h.config.Mode, "method", req.Method, "path", req.URL.Path, "err", err)
		http.Error(resp, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, h.config.Mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	status := result.StatusCode
	cache := result.Headers["X-Cache"]
	bytes := ResponseBytes(result.Headers)
	StripInternal(result.Headers)
	if err := result.FlushClose(req, resp); err != nil {
		slog.Info("flush response failed", "instance", h.name, "err", err)
		if status < 500 {
			status = http.StatusBadGateway
		}
	}
	h.stats.RecordRequest(h.name, h.config.Mode, req.Method, cache, status, bytes)
}

func (h *Handler) Close() {
	h.wait.Wait()
}

func (h *Handler) CloseContext(ctx context.Context) error {
	return utils.WaitGroupContext(ctx, &h.wait)
}

func (h *Handler) ProxyPassthrough(resp http.ResponseWriter, req *http.Request, upstreamPath string) {
	h.wait.Add(1)
	defer h.wait.Done()

	route := Route{
		UpstreamPath: upstreamPath,
		Policy:       config.PolicyBypass,
	}
	result, err := h.bypass(req.Context(), req, route)
	if err != nil {
		slog.Info("proxy passthrough failed", "instance", h.name, "mode", h.config.Mode, "method", req.Method, "path", req.URL.Path, "upstream_path", upstreamPath, "err", err)
		http.Error(resp, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, h.config.Mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	status := result.StatusCode
	cache := result.Headers["X-Cache"]
	bytes := ResponseBytes(result.Headers)
	StripInternal(result.Headers)
	if err := result.FlushClose(req, resp); err != nil {
		slog.Info("flush passthrough response failed", "instance", h.name, "err", err)
		if status < 500 {
			status = http.StatusBadGateway
		}
	}
	h.stats.RecordRequest(h.name, h.config.Mode, req.Method, cache, status, bytes)
}

func ModeUserAgent(mode string) string {
	switch mode {
	case config.ModeNPM:
		return "npm/10.8.0"
	case config.ModeGo:
		return "Go-http-client/2.0"
	case config.ModeMaven:
		return "Apache-Maven/3.9.6"
	case config.ModeCargo:
		return "cargo/1.79.0"
	case config.ModePyPI:
		return "pip/24.0"
	case config.ModeOCI:
		return "docker/27.0.0"
	case config.ModeAPK:
		return "apk-tools/2.14.0"
	case config.ModeDEB:
		return "Debian APT-HTTP/1.3"
	case config.ModeRPM:
		return "dnf/4.19.0"
	case config.ModePacman:
		return "pacman/6.1.0"
	default:
		return "cache-proxy"
	}
}
