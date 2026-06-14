package pypi

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	stdpath "path"
	"regexp"
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
	SimpleFreshFor    config.Duration `json:"simpleFreshFor,omitempty" yaml:"simple_fresh_for,omitempty"`
	SimpleBusyPolicy  string          `json:"simpleBusyPolicy,omitempty" yaml:"simple_busy_policy,omitempty"`
	FilePolicy        string          `json:"filePolicy,omitempty" yaml:"file_policy,omitempty"`
	ProxyJSON         bool            `json:"proxyJson,omitempty" yaml:"proxy_json,omitempty"`
	ProxyCoreMetadata bool            `json:"proxyCoreMetadata,omitempty" yaml:"proxy_core_metadata,omitempty"`
	ProxySignatures   bool            `json:"proxySignatures,omitempty" yaml:"proxy_signatures,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModePyPI }

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

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) { return json.Marshal(policy) }

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
	if policy.SimpleBusyPolicy == "" {
		policy.SimpleBusyPolicy = config.BusyPolicyStale
	}
	if policy.FilePolicy == "" {
		policy.FilePolicy = config.PolicyImmutable
	}
	if !policy.ProxyJSON {
		policy.ProxyJSON = true
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) != 1 {
		return fmt.Errorf("pypi mode requires exactly one upstream")
	}
	policy := spec.Policy.(*Policy)
	if policy.SimpleBusyPolicy != config.BusyPolicyBypass && policy.SimpleBusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid pypi simple busy policy %q", policy.SimpleBusyPolicy)
	}
	if policy.FilePolicy != config.PolicyBypass && policy.FilePolicy != config.PolicyImmutable && policy.FilePolicy != config.PolicyRevalidate {
		return fmt.Errorf("invalid pypi file policy %q", policy.FilePolicy)
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Duration { return 0 }

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler := &handler{
		name:   name,
		source: spec.Source,
		meta:   spec.Meta,
		policy: spec.Policy.(*Policy),
		store:  store,
		stats:  stats,
		client: &http.Client{Transport: &statsTransport{base: transportForConfig(spec.Source.Transport), stats: stats, instance: name}},
	}
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return routeForPath(spec.Policy.(*Policy), strings.TrimPrefix(path.Clean("/"+lookupPath), "/"))
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

func (h *handler) Close() { h.wait.Wait() }

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	h.wait.Add(1)
	defer h.wait.Done()
	route, err := routeForPath(h.policy, strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/"))
	if err != nil {
		http.NotFound(w, req)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	body, headers, cache, err := h.resolve(req.Context(), req, route)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	defer body.Close()
	for key, value := range headers {
		w.Header().Set(key, value)
	}
	if req.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, cache, http.StatusOK, 0)
		return
	}
	w.WriteHeader(http.StatusOK)
	written, _ := io.Copy(w, body)
	h.stats.RecordRequest(h.name, h.meta.Mode, req.Method, cache, http.StatusOK, uint64(written))
}

func (h *handler) resolve(ctx context.Context, req *http.Request, route proxy.Route) (io.ReadCloser, map[string]string, string, error) {
	if route.Policy == config.PolicyImmutable {
		if body, headers, err := h.openCached(ctx, route.ObjectPath); err == nil {
			return body, headers, "HIT", nil
		}
	}
	if strings.HasPrefix(route.ObjectPath, "pypi/simple/") {
		if body, headers, fresh, err := h.openFreshCached(ctx, route.ObjectPath, route.FreshFor); err == nil && fresh {
			return body, headers, "FRESH", nil
		}
	}
	data, headers, err := h.fetchUpstream(ctx, req, route)
	if err != nil {
		if strings.HasPrefix(route.ObjectPath, "pypi/simple/") {
			if body, cachedHeaders, cacheErr := h.openCached(ctx, route.ObjectPath); cacheErr == nil {
				return body, cachedHeaders, "STALE", nil
			}
		}
		return nil, nil, "", err
	}
	if err := h.putCached(ctx, route.ObjectPath, data, headers); err != nil {
		return nil, nil, "", err
	}
	return io.NopCloser(bytes.NewReader(data)), headers, "MISS", nil
}

func (h *handler) fetchUpstream(ctx context.Context, req *http.Request, route proxy.Route) ([]byte, map[string]string, error) {
	var rawURL string
	switch {
	case strings.HasPrefix(route.ObjectPath, "pypi/files/"):
		sourceURL, err := decodeSourceURL(path.Base(route.ObjectPath))
		if err != nil {
			return nil, nil, err
		}
		rawURL = sourceURL
	default:
		rawURL = strings.TrimRight(h.source.Upstreams[0], "/") + "/" + strings.TrimPrefix(route.UpstreamPath, "/")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, err
	}
	if strings.HasSuffix(route.ObjectPath, ".json") {
		request.Header.Set("Accept", "application/vnd.pypi.simple.v1+json")
	}
	resp, err := h.client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("pypi upstream returned %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	headers := map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)}
	if strings.HasPrefix(route.ObjectPath, "pypi/simple/") {
		data, headers, err = h.rewriteSimple(req, rawURL, resp.Header.Get("Content-Type"), data)
		if err != nil {
			return nil, nil, err
		}
		return data, headers, nil
	}
	headers["Content-Type"] = resp.Header.Get("Content-Type")
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = "application/octet-stream"
	}
	return data, headers, nil
}

func (h *handler) rewriteSimple(req *http.Request, upstreamPageURL, contentType string, data []byte) ([]byte, map[string]string, error) {
	if strings.Contains(contentType, "json") {
		next, err := h.rewriteSimpleJSON(req, upstreamPageURL, data)
		if err != nil {
			return nil, nil, err
		}
		return next, map[string]string{"Content-Type": "application/vnd.pypi.simple.v1+json", "fetched-at": time.Now().UTC().Format(time.RFC3339Nano)}, nil
	}
	next := rewriteSimpleHTML(proxyBaseURL(req), upstreamPageURL, data)
	return next, map[string]string{"Content-Type": "text/html; charset=utf-8", "fetched-at": time.Now().UTC().Format(time.RFC3339Nano)}, nil
}

func (h *handler) rewriteSimpleJSON(req *http.Request, upstreamPageURL string, data []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	files, ok := payload["files"].([]any)
	if !ok {
		return data, nil
	}
	base := proxyBaseURL(req)
	for _, item := range files {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if rawURL, ok := obj["url"].(string); ok && rawURL != "" {
			obj["url"] = joinBaseAndPath(base, "/files/"+encodeSourceURL(resolveURL(upstreamPageURL, rawURL)))
		}
	}
	return json.Marshal(payload)
}

var hrefPattern = regexp.MustCompile(`href="([^"]+)"`)

func rewriteSimpleHTML(base, upstreamPageURL string, data []byte) []byte {
	return hrefPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		parts := hrefPattern.FindSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		rawURL := string(parts[1])
		resolved := resolveURL(upstreamPageURL, rawURL)
		return []byte(`href="` + joinBaseAndPath(base, "/files/"+encodeSourceURL(resolved)) + `"`)
	})
}

func routeForPath(policy *Policy, lookupPath string) (proxy.Route, error) {
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "simple/"
	}
	switch {
	case lookupPath == "simple" || lookupPath == "simple/":
		return proxy.Route{ObjectPath: "pypi/simple/root.html", UpstreamPath: "simple/", Policy: config.PolicyRevalidate, FreshFor: policy.SimpleFreshFor}, nil
	case strings.HasPrefix(lookupPath, "simple/"):
		trimmed := strings.TrimPrefix(lookupPath, "simple/")
		if strings.HasSuffix(trimmed, "/json") {
			name := normalizeProjectName(strings.TrimSuffix(trimmed, "/json"))
			return proxy.Route{ObjectPath: "pypi/simple/" + name + ".json", UpstreamPath: "simple/" + name + "/", Policy: config.PolicyRevalidate, FreshFor: policy.SimpleFreshFor}, nil
		}
		name := normalizeProjectName(strings.TrimSuffix(trimmed, "/"))
		return proxy.Route{ObjectPath: "pypi/simple/" + name + ".html", UpstreamPath: "simple/" + name + "/", Policy: config.PolicyRevalidate, FreshFor: policy.SimpleFreshFor}, nil
	case strings.HasPrefix(lookupPath, "files/"):
		return proxy.Route{ObjectPath: "pypi/files/" + path.Base(lookupPath), UpstreamPath: lookupPath, Policy: policy.FilePolicy}, nil
	default:
		return proxy.Route{ObjectPath: "pypi/files/" + encodeSourceURL(strings.TrimPrefix(lookupPath, "/")), UpstreamPath: lookupPath, Policy: policy.FilePolicy}, nil
	}
}

func normalizeProjectName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	return strings.ReplaceAll(name, ".", "-")
}

func encodeSourceURL(raw string) string {
	return hex.EncodeToString([]byte(raw))
}

func decodeSourceURL(value string) (string, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func resolveURL(base, raw string) string {
	baseURL, _ := url.Parse(base)
	ref, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return baseURL.ResolveReference(ref).String()
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

func proxyBaseURL(req *http.Request) string {
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
		prefix = req.URL.Path
		if idx := strings.Index(prefix, "/simple/"); idx >= 0 {
			prefix = prefix[:idx]
		}
		prefix = normalizedProxyPrefix(prefix)
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

func transportForConfig(cfg *config.TransportConfig) http.RoundTripper {
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
}

func (t *statsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		t.stats.RecordUpstream(t.instance, config.ModePyPI, req.Method, 0)
		return nil, err
	}
	t.stats.RecordUpstream(t.instance, config.ModePyPI, req.Method, resp.StatusCode)
	return resp, nil
}

func init() {
	proxydriver.Default.Register(Driver{})
}
