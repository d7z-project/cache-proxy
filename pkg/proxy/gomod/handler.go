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
	name   string
	meta   config.InstanceMeta
	policy *Policy
	inner  http.Handler
	stats  *proxy.Stats
	wait   sync.WaitGroup
}

func NewHandler(name string, meta config.InstanceMeta, source config.InstanceSource, policy *Policy, store *blobfs.Store, stats *proxy.Stats) (*Handler, error) {
	if policy == nil {
		policy = &Policy{}
	}
	tempDir, err := os.MkdirTemp("", "cache-proxy-go-*")
	if err != nil {
		return nil, err
	}
	transport := &statsTransport{base: transportForConfig(name, source.Transport), stats: stats, instance: name}
	fetcher, err := newUpstreamProxyFetcher(source.Upstreams, transport)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	inner := &goproxy.Goproxy{
		Fetcher:       fetcher,
		ProxiedSumDBs: proxiedSumDBs(policy),
		Cacher:        newBlobFSCacher(store, name, meta.ExpireAfter),
		TempDir:       tempDir,
		Transport:     transport,
		Logger:        slog.Default().With("instance", name, "mode", config.ModeGo),
	}
	return &Handler{name: name, meta: meta, policy: policy, inner: closeableTempHandler{Handler: inner, tempDir: tempDir}, stats: stats}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	if modulePath, ok := modulePathFromTarget(strings.TrimPrefix(req.URL.Path, "/")); ok && matchesPrivateModule(h.policy, modulePath) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "PRIVATE", http.StatusNotFound, 0)
		return
	}
	h.wait.Add(1)
	defer h.wait.Done()

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	next := req
	if h.policy == nil || !h.policy.DisableModuleFetchHeader {
		next = req.Clone(req.Context())
		next.Header = req.Header.Clone()
		next.Header.Del("Disable-Module-Fetch")
	}
	h.inner.ServeHTTP(rec, next)
	cache := rec.Header().Get("X-Cache")
	if cache == "" {
		cache = "GO"
	}
	h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, cache, rec.status, uint64(rec.bytes))
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

func proxiedSumDBs(policy *Policy) []string {
	if policy == nil || policy.SumDB == nil || !policy.SumDB.Enabled {
		return nil
	}
	name := strings.TrimSpace(policy.SumDB.Name)
	rawURL := strings.TrimSpace(policy.SumDB.URL)
	if name == "" || rawURL == "" {
		return nil
	}
	return []string{name + " " + rawURL}
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
