package httpcache

import (
	"context"
	"errors"
	"io"
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
	ObjectPath         string
	UpstreamPath       string
	TargetURL          string
	AllowedTargetHosts []string
	Policy             string
	FreshFor           config.Freshness
	BusyPolicy         string
	ExpireAfter        config.Expiration
	RequestHeaders     map[string]string
	RewriteKind        string
	AuthRequired       bool
	PreferredUpstream  string
}

type Resolver interface {
	Resolve(req *http.Request) (Route, error)
}

type RuntimeConfig struct {
	Mode               string
	ExpireAfter        config.Expiration
	Upstreams          []string
	Transport          *config.TransportConfig
	BusyPolicy         string
	DefaultFreshFor    config.Freshness
	PassHeaders        []string
	AllowedTargetHosts []string
	MetadataFunc       func(*http.Request, Route, map[string]string, string) map[string]string
	VerifyFunc         func(*http.Request, Route, io.ReadSeeker) error
}

type Handler struct {
	name                string
	config              RuntimeConfig
	store               *blobfs.Store
	client              *utils.HttpClientWrapper
	locks               *utils.RWLockGroup
	resolver            Resolver
	stats               *Stats
	health              *health.ServiceHealth
	wait                sync.WaitGroup
	downloads           sync.Map
	parsedUpstreamHosts []string
}

type remoteOptions struct {
	AcceptErrors       bool
	Record             bool
	TargetURL          string
	AllowedTargetHosts []string
	PreferredUpstream  string
}

func NewHandler(name string, runtime RuntimeConfig, store *blobfs.Store, resolver Resolver, stats *Stats, svcHealth *health.ServiceHealth) *Handler {
	client := utils.DefaultHttpClientWrapper()
	ConfigureClientTransport(client, name, runtime.Mode, runtime.Transport)
	hosts := make([]string, 0, len(runtime.Upstreams))
	for _, u := range runtime.Upstreams {
		if pu, err := url.Parse(u); err == nil && pu.Host != "" {
			hosts = append(hosts, pu.Host)
		}
	}
	return &Handler{name: name, config: runtime, store: store, client: client, locks: utils.NewRWLockGroup(), resolver: resolver, stats: stats, health: svcHealth, parsedUpstreamHosts: hosts}
}

func ConfigureClientTransport(client *utils.HttpClientWrapper, name, mode string, transport *config.TransportConfig) {
	client.UserAgent = ModeUserAgent(mode)
	if transport == nil {
		return
	}
	if transport.UserAgent != "" {
		client.UserAgent = transport.UserAgent
	}
	baseTransport, ok := client.Transport.(*http.Transport)
	if !ok {
		slog.Warn("cannot configure transport, unexpected transport type", "instance", name)
		return
	}
	if transport.Proxy != "" {
		if proxyURL, err := url.Parse(transport.Proxy); err == nil {
			baseTransport.Proxy = http.ProxyURL(proxyURL)
		} else {
			slog.Warn("invalid transport proxy URL", "instance", name, "proxy", transport.Proxy, "err", err)
		}
	}
	if transport.Timeout > 0 {
		baseTransport.DialContext = utils.DefaultDialContext(transport.Timeout.Duration())
	}
	if transport.MaxIdleConns > 0 {
		baseTransport.MaxIdleConns = transport.MaxIdleConns
		baseTransport.MaxIdleConnsPerHost = transport.MaxIdleConns
	}
	if transport.MaxConnsPerHost > 0 {
		baseTransport.MaxConnsPerHost = transport.MaxConnsPerHost
	}
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
		resp.Header().Set("Retry-After", "5")
		status := http.StatusBadGateway
		if errors.Is(err, ErrUpstreamUnavailable) {
			status = http.StatusServiceUnavailable
		}
		http.Error(resp, http.StatusText(status), status)
		h.stats.RecordRequest(h.name, h.config.Mode, req.Method, "ERROR", status, 0)
		return
	}
	h.flushResult(req, resp, result, "flush response failed")
}

func (h *Handler) flushResult(req *http.Request, resp http.ResponseWriter, result *utils.ResponseWrapper, logMsg string) {
	status := result.StatusCode
	cache := result.Headers["X-Cache"]
	bytes := ResponseBytes(result.Headers)
	StripInternal(result.Headers)
	if err := result.FlushClose(req, resp); err != nil {
		slog.Info(logMsg, "instance", h.name, "err", err)
	}
	h.stats.RecordRequest(h.name, h.config.Mode, req.Method, cache, status, bytes)
}

func (h *Handler) Close() {
	h.wait.Wait()
}

func (h *Handler) CloseContext(ctx context.Context) error {
	return utils.WaitGroupContext(ctx, &h.wait)
}

func (h *Handler) ProxyPassthrough(resp http.ResponseWriter, req *http.Request, upstreamPath string, preferredUpstream string) {
	h.wait.Add(1)
	defer h.wait.Done()

	route := Route{
		UpstreamPath:      upstreamPath,
		Policy:            config.PolicyBypass,
		PreferredUpstream: preferredUpstream,
	}
	result, err := h.bypass(req.Context(), req, route)
	if err != nil {
		slog.Info("proxy passthrough failed", "instance", h.name, "mode", h.config.Mode, "method", req.Method, "path", req.URL.Path, "upstream_path", upstreamPath, "err", err)
		resp.Header().Set("Retry-After", "5")
		status := http.StatusBadGateway
		if errors.Is(err, ErrUpstreamUnavailable) {
			status = http.StatusServiceUnavailable
		}
		http.Error(resp, http.StatusText(status), status)
		h.stats.RecordRequest(h.name, h.config.Mode, req.Method, "ERROR", status, 0)
		return
	}
	if result.Headers["X-Cache"] == "BYPASS" {
		result.Headers["X-Cache"] = "PASSTHROUGH"
	}
	h.flushResult(req, resp, result, "flush passthrough response failed")
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
