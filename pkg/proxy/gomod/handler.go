package gomod

import (
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/goproxy/goproxy"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type Handler struct {
	name  string
	cfg   config.InstanceConfig
	inner http.Handler
	stats *proxy.Stats
	wait  sync.WaitGroup
}

func NewHandler(name string, cfg config.InstanceConfig, store *blobfs.Store, stats *proxy.Stats) (*Handler, error) {
	goCfg := cfg.Go
	if goCfg == nil {
		goCfg = &config.GoConfig{}
	}
	tempDir, err := os.MkdirTemp("", "cache-proxy-go-*")
	if err != nil {
		return nil, err
	}
	transport := &statsTransport{base: transportForConfig(name, cfg.Transport), stats: stats, instance: name}
	env := goFetcherEnv(cfg.Upstreams, goCfg)
	fetcher := &goproxy.GoFetcher{
		Env:       env,
		TempDir:   tempDir,
		Transport: transport,
	}
	inner := &goproxy.Goproxy{
		Fetcher:       fetcher,
		ProxiedSumDBs: goCfg.ProxiedSumDBs,
		Cacher:        newBlobFSCacher(store, name, cfg.ExpireAfter),
		TempDir:       tempDir,
		Transport:     transport,
		Logger:        slog.Default().With("instance", name, "mode", config.ModeGo),
	}
	return &Handler{name: name, cfg: cfg, inner: closeableTempHandler{Handler: inner, tempDir: tempDir}, stats: stats}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, h.cfg.Mode, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	h.wait.Add(1)
	defer h.wait.Done()

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	next := req
	if h.cfg.Go == nil || !h.cfg.Go.DisableModuleFetchHeader {
		next = req.Clone(req.Context())
		next.Header = req.Header.Clone()
		next.Header.Del("Disable-Module-Fetch")
	}
	h.inner.ServeHTTP(rec, next)
	cache := rec.Header().Get("X-Cache")
	if cache == "" {
		cache = "GO"
	}
	h.stats.RecordRequest(h.name, h.cfg.Mode, req.Method, cache, rec.status, uint64(rec.bytes))
}

func (h *Handler) Close() {
	h.wait.Wait()
	if closer, ok := h.inner.(interface{ Close() }); ok {
		closer.Close()
	}
}

type closeableTempHandler struct {
	http.Handler
	tempDir string
}

func (h closeableTempHandler) Close() {
	_ = os.RemoveAll(h.tempDir)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += n
	return n, err
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func goFetcherEnv(upstreams []string, cfg *config.GoConfig) []string {
	proxies := append([]string(nil), upstreams...)
	sumdb := strings.TrimSpace(cfg.SumDB)
	if sumdb == "" {
		sumdb = "sum.golang.org"
	}
	env := []string{
		"GOPROXY=" + strings.Join(proxies, ","),
		"GOSUMDB=" + sumdb,
		"GOPRIVATE=",
		"GONOPROXY=",
		"GONOSUMDB=" + strings.TrimSpace(cfg.NoSumDB),
	}
	return env
}

func transportForConfig(instance string, cfg *config.TransportConfig) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg == nil {
		return transport
	}
	if cfg.Proxy != "" {
		proxyURL, err := url.Parse(cfg.Proxy)
		if err == nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		} else {
			slog.Warn("invalid Go module transport proxy URL", "instance", instance, "proxy", cfg.Proxy, "err", err)
		}
	}
	if cfg.Timeout > 0 {
		transport.DialContext = utils.DefaultDialContext(cfg.Timeout.Duration())
	}
	return transport
}

type statsTransport struct {
	base     http.RoundTripper
	stats    *proxy.Stats
	instance string
}

func (t *statsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		t.stats.RecordUpstream(t.instance, config.ModeGo, req.Method, 0)
		return nil, err
	}
	t.stats.RecordUpstream(t.instance, config.ModeGo, req.Method, resp.StatusCode)
	return resp, nil
}
