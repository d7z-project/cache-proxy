package cargo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	stdpath "path"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type Policy struct {
	IndexFreshFor   config.Duration `json:"indexFreshFor,omitempty" yaml:"index_fresh_for,omitempty"`
	IndexBusyPolicy string          `json:"indexBusyPolicy,omitempty" yaml:"index_busy_policy,omitempty"`
	CratePolicy     string          `json:"cratePolicy,omitempty" yaml:"crate_policy,omitempty"`
	AuthRequired    bool            `json:"authRequired,omitempty" yaml:"auth_required,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModeCargo }

func (Driver) DecodeJSON(data json.RawMessage) (any, error) {
	policy := &Policy{}
	if len(data) == 0 || string(data) == "null" {
		return policy, nil
	}
	if err := json.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) {
	return json.Marshal(policy)
}

func (Driver) DecodeYAML(data []byte) (any, error) {
	policy := &Policy{}
	if len(data) == 0 {
		return policy, nil
	}
	if err := yaml.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (Driver) EncodeYAML(policy any) ([]byte, error) { return yaml.Marshal(policy) }

func (Driver) ApplyDefaults(spec *proxydriver.ResolvedSpec) {
	policy := spec.Policy.(*Policy)
	if policy.IndexBusyPolicy == "" {
		policy.IndexBusyPolicy = config.BusyPolicyStale
	}
	if policy.CratePolicy == "" {
		policy.CratePolicy = config.PolicyImmutable
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) != 1 {
		return errors.New("cargo mode requires exactly one upstream")
	}
	policy := spec.Policy.(*Policy)
	if policy.IndexBusyPolicy != config.BusyPolicyBypass && policy.IndexBusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid cargo index busy policy %q", policy.IndexBusyPolicy)
	}
	if policy.CratePolicy != config.PolicyBypass && policy.CratePolicy != config.PolicyImmutable && policy.CratePolicy != config.PolicyRevalidate {
		return fmt.Errorf("invalid cargo crate policy %q", policy.CratePolicy)
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Duration {
	return 0
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler, err := newHandler(name, spec.Source, spec.Meta, spec.Policy.(*Policy), store, stats)
	if err != nil {
		return nil, nil, err
	}
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return cargoRoute(spec.Policy.(*Policy), strings.TrimPrefix(path.Clean("/"+lookupPath), "/"))
}

type handler struct {
	name   string
	source config.InstanceSource
	meta   config.InstanceMeta
	policy *Policy
	store  *blobfs.Store
	stats  *proxy.Stats
	client *http.Client
	wait   sync.WaitGroup
}

func newHandler(name string, source config.InstanceSource, meta config.InstanceMeta, policy *Policy, store *blobfs.Store, stats *proxy.Stats) (*handler, error) {
	base := transportForConfig(name, source.Transport)
	return &handler{
		name:   name,
		source: source,
		meta:   meta,
		policy: policy,
		store:  store,
		stats:  stats,
		client: &http.Client{Transport: &statsTransport{base: base, stats: stats, instance: name, mode: config.ModeCargo}},
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	h.wait.Add(1)
	defer h.wait.Done()

	route, err := cargoRoute(h.policy, strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/"))
	if err != nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	if route.ObjectPath == "" {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	body, status, headers, cache, err := h.resolve(req.Context(), req, route)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	defer body.Close()
	for key, value := range headers {
		w.Header().Set(key, value)
	}
	w.WriteHeader(status)
	var written int64
	if req.Method != http.MethodHead {
		written, _ = io.Copy(w, body)
	}
	h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, cache, status, uint64(written))
}

func (h *handler) Close() { h.wait.Wait() }

func (h *handler) resolve(ctx context.Context, req *http.Request, route proxy.Route) (io.ReadCloser, int, map[string]string, string, error) {
	if route.Policy == config.PolicyImmutable {
		if body, headers, err := h.openCached(ctx, route.ObjectPath); err == nil {
			return body, http.StatusOK, headers, "HIT", nil
		}
	}
	if route.Policy == config.PolicyRevalidate || strings.HasPrefix(route.ObjectPath, "cargo/index/") {
		if body, headers, fresh, err := h.openFreshCached(ctx, route.ObjectPath, route.FreshFor); err == nil && fresh {
			return body, http.StatusOK, headers, "FRESH", nil
		}
	}
	data, headers, err := h.fetchUpstream(ctx, req, route)
	if err != nil {
		if body, cachedHeaders, cacheErr := h.openCached(ctx, route.ObjectPath); cacheErr == nil && strings.HasPrefix(route.ObjectPath, "cargo/index/") {
			return body, http.StatusOK, cachedHeaders, "STALE", nil
		}
		return nil, 0, nil, "", err
	}
	if err := h.putCached(ctx, route.ObjectPath, data, headers); err != nil {
		return nil, 0, nil, "", err
	}
	return io.NopCloser(bytes.NewReader(data)), http.StatusOK, headers, "MISS", nil
}

func (h *handler) fetchUpstream(ctx context.Context, req *http.Request, route proxy.Route) ([]byte, map[string]string, error) {
	upstream := strings.TrimRight(h.source.Upstreams[0], "/")
	var rawURL string
	if strings.HasPrefix(route.ObjectPath, "cargo/crates/") {
		cfg, err := h.fetchConfig(ctx)
		if err != nil {
			return nil, nil, err
		}
		crate, version, err := parseCargoDownloadPath(strings.TrimPrefix(route.ObjectPath, "cargo/crates/"))
		if err != nil {
			return nil, nil, err
		}
		rawURL = cargoDownloadURL(cfg.DL, crate, version)
	} else {
		rawURL = upstream + "/" + strings.TrimPrefix(route.UpstreamPath, "/")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := h.client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("cargo upstream returned %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	headers := map[string]string{}
	if strings.HasSuffix(route.ObjectPath, "config.json") {
		data, err = h.rewriteConfig(req, data)
		if err != nil {
			return nil, nil, err
		}
		headers["Content-Type"] = "application/json"
	} else if strings.HasPrefix(route.ObjectPath, "cargo/index/") {
		headers["Content-Type"] = "text/plain; charset=utf-8"
	} else {
		headers["Content-Type"] = "application/octet-stream"
	}
	headers["fetched-at"] = time.Now().UTC().Format(time.RFC3339Nano)
	return data, headers, nil
}

type cargoConfig struct {
	DL           string `json:"dl"`
	AuthRequired bool   `json:"auth-required,omitempty"`
}

func (h *handler) rewriteConfig(req *http.Request, data []byte) ([]byte, error) {
	cfg := cargoConfig{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	cfg.DL = joinBaseAndPath(externalBaseURL(req), "/api/v1/crates/{crate}/{version}/download")
	if h.policy.AuthRequired {
		cfg.AuthRequired = true
	}
	return json.Marshal(cfg)
}

func (h *handler) fetchConfig(ctx context.Context) (cargoConfig, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(h.source.Upstreams[0], "/")+"/config.json", nil)
	if err != nil {
		return cargoConfig{}, err
	}
	resp, err := h.client.Do(request)
	if err != nil {
		return cargoConfig{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cargoConfig{}, fmt.Errorf("cargo upstream config returned %d", resp.StatusCode)
	}
	var cfg cargoConfig
	err = json.NewDecoder(resp.Body).Decode(&cfg)
	return cfg, err
}

func cargoRoute(policy *Policy, lookupPath string) (proxy.Route, error) {
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "config.json"
	}
	switch {
	case lookupPath == "config.json":
		return proxy.Route{ObjectPath: "cargo/index/config.json", UpstreamPath: "config.json", Policy: config.PolicyRevalidate, FreshFor: policy.IndexFreshFor}, nil
	case strings.HasPrefix(lookupPath, "api/v1/crates/") && strings.HasSuffix(lookupPath, "/download"):
		return proxy.Route{ObjectPath: "cargo/crates/" + strings.TrimPrefix(lookupPath, "api/v1/crates/"), UpstreamPath: lookupPath, Policy: policy.CratePolicy}, nil
	default:
		return proxy.Route{ObjectPath: "cargo/index/" + lookupPath, UpstreamPath: lookupPath, Policy: config.PolicyRevalidate, FreshFor: policy.IndexFreshFor}, nil
	}
}

func parseCargoDownloadPath(objectPath string) (string, string, error) {
	parts := strings.Split(objectPath, "/")
	if len(parts) < 3 || parts[len(parts)-1] != "download" {
		return "", "", fmt.Errorf("invalid cargo download path")
	}
	return parts[0], parts[1], nil
}

func cargoDownloadURL(template, crate, version string) string {
	replacer := strings.NewReplacer(
		"{crate}", crate,
		"{version}", version,
		"{prefix}", "",
		"{lowerprefix}", "",
	)
	return replacer.Replace(template)
}

func (h *handler) openCached(ctx context.Context, objectPath string) (io.ReadCloser, map[string]string, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, nil, err
	}
	return reader, cloneHeaders(reader.Info().Options), nil
}

func (h *handler) openFreshCached(ctx context.Context, objectPath string, freshFor config.Duration) (io.ReadCloser, map[string]string, bool, error) {
	reader, headers, err := h.openCached(ctx, objectPath)
	if err != nil {
		return nil, nil, false, err
	}
	if freshFor <= 0 {
		return reader, headers, false, nil
	}
	fetchedAt, err := utils.ParseFetchedAt(headers["fetched-at"])
	if err != nil {
		_ = reader.Close()
		return nil, nil, false, err
	}
	return reader, headers, time.Since(fetchedAt) <= freshFor.Duration(), nil
}

func (h *handler) putCached(ctx context.Context, objectPath string, data []byte, headers map[string]string) error {
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			return err
		}
	}
	_, err := h.store.Put(ctx, h.name, objectPath, bytes.NewReader(data), headers)
	return err
}

func cloneHeaders(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func externalBaseURL(req *http.Request) string {
	scheme := req.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if req.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := req.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = req.Host
	}
	prefix := normalizedProxyPrefix(req.Header.Get("X-Cache-Proxy-Prefix"))
	if prefix == "" {
		prefix = normalizedProxyPrefix(strings.TrimSuffix(req.URL.Path, "/config.json"))
	}
	return scheme + "://" + host + prefix
}

func normalizedProxyPrefix(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	cleaned := stdpath.Clean("/" + strings.TrimPrefix(trimmed, "/"))
	if cleaned == "." || cleaned == "/" {
		return ""
	}
	return cleaned
}

func joinBaseAndPath(base, suffix string) string {
	cleanBase := strings.TrimRight(base, "/")
	cleanSuffix := "/" + strings.TrimLeft(suffix, "/")
	return cleanBase + cleanSuffix
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
	mode     string
}

func (t *statsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		t.stats.RecordUpstream(t.instance, t.mode, req.Method, 0)
		return nil, err
	}
	t.stats.RecordUpstream(t.instance, t.mode, req.Method, resp.StatusCode)
	return resp, nil
}

func init() {
	proxydriver.Default.Register(Driver{})
}
