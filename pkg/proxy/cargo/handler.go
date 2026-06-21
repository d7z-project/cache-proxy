package cargo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type handler struct {
	name     string
	upstream string
	policy   *Policy
	store    *blobfs.Store
	stats    *httpcache.Stats
	client   *http.Client
	wait     sync.WaitGroup
}

type cargoConfig struct {
	DL           string `json:"dl"`
	AuthRequired bool   `json:"auth-required,omitempty"`
}

func newHandler(name, upstream string, transport *config.TransportConfig, policy *Policy, store *blobfs.Store, stats *httpcache.Stats) (*handler, error) {
	return &handler{
		name:     name,
		upstream: upstream,
		policy:   policy,
		store:    store,
		stats:    stats,
		client: &http.Client{Transport: &statsTransport{
			base:     transportForConfig(transport),
			stats:    stats,
			instance: name,
			mode:     config.ModeCargo,
		}},
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, config.ModeCargo, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	h.wait.Add(1)
	defer h.wait.Done()

	route, err := cargoRoute(h.policy, strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/"))
	if err != nil || route.ObjectPath == "" {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, config.ModeCargo, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	body, status, headers, cache, err := h.resolve(req.Context(), req, route)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, config.ModeCargo, req.Method, "ERROR", http.StatusBadGateway, 0)
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
	h.stats.RecordRequest(h.name, config.ModeCargo, req.Method, cache, status, uint64(written))
}

func (h *handler) Start(context.Context) error { return nil }

func (h *handler) Stop(context.Context) error {
	h.wait.Wait()
	return nil
}

func (h *handler) resolve(ctx context.Context, req *http.Request, route httpcache.Route) (io.ReadCloser, int, map[string]string, string, error) {
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
		if route.BusyPolicy == config.BusyPolicyStale {
			if body, cachedHeaders, cacheErr := h.openCached(ctx, route.ObjectPath); cacheErr == nil {
				return body, http.StatusOK, cachedHeaders, "STALE", nil
			}
		}
		return nil, 0, nil, "", err
	}
	if err := h.putCached(ctx, route.ObjectPath, data, headers); err != nil {
		return nil, 0, nil, "", err
	}
	return io.NopCloser(bytes.NewReader(data)), http.StatusOK, headers, "MISS", nil
}

func (h *handler) fetchUpstream(ctx context.Context, req *http.Request, route httpcache.Route) ([]byte, map[string]string, error) {
	rawURL := strings.TrimRight(h.upstream, "/") + "/" + strings.TrimPrefix(route.UpstreamPath, "/")
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
	headers := map[string]string{"fetched-at": time.Now().UTC().Format(time.RFC3339Nano)}
	switch {
	case strings.HasSuffix(route.ObjectPath, "config.json"):
		data, err = h.rewriteConfig(req, data)
		if err != nil {
			return nil, nil, err
		}
		headers["Content-Type"] = "application/json"
	case strings.HasPrefix(route.ObjectPath, "cargo/index/"):
		headers["Content-Type"] = "text/plain; charset=utf-8"
	default:
		headers["Content-Type"] = "application/octet-stream"
	}
	return data, headers, nil
}

func (h *handler) rewriteConfig(req *http.Request, data []byte) ([]byte, error) {
	cfg := cargoConfig{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	cfg.DL = httpcache.JoinBaseAndPath(httpcache.ExternalBaseURL(req), "/api/v1/crates/{crate}/{version}/download")
	if h.policy.AuthRequired {
		cfg.AuthRequired = true
	}
	return json.Marshal(cfg)
}

func (h *handler) fetchConfig(ctx context.Context) (cargoConfig, error) {
	const objectPath = "cargo/internal/config.json"
	if body, _, fresh, err := h.openFreshCached(ctx, objectPath, h.policy.IndexFreshFor); err == nil && fresh {
		defer body.Close()
		var cfg cargoConfig
		return cfg, json.NewDecoder(body).Decode(&cfg)
	}
	data, _, err := h.fetchRawConfig(ctx)
	if err != nil {
		if h.policy.IndexBusyPolicy == config.BusyPolicyStale {
			if body, _, _, openErr := h.openFreshCached(ctx, objectPath, config.FreshnessForever); openErr == nil {
				defer body.Close()
				var cfg cargoConfig
				return cfg, json.NewDecoder(body).Decode(&cfg)
			}
		}
		return cargoConfig{}, err
	}
	var cfg cargoConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cargoConfig{}, err
	}
	if err := h.putCached(ctx, objectPath, data, map[string]string{
		"Content-Type": "application/json",
		"fetched-at":   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return cargoConfig{}, err
	}
	return cfg, nil
}

func (h *handler) fetchRawConfig(ctx context.Context) ([]byte, map[string]string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(h.upstream, "/")+"/config.json", nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := h.client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("cargo upstream config returned %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return data, map[string]string{"Content-Type": "application/json"}, nil
}

func (h *handler) openCached(ctx context.Context, objectPath string) (io.ReadCloser, map[string]string, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, nil, err
	}
	return reader, cloneHeaders(reader.Info().Options), nil
}

func (h *handler) openFreshCached(ctx context.Context, objectPath string, freshFor config.Freshness) (io.ReadCloser, map[string]string, bool, error) {
	reader, headers, err := h.openCached(ctx, objectPath)
	if err != nil {
		return nil, nil, false, err
	}
	if freshFor.IsUnset() || freshFor.IsForever() {
		return reader, headers, freshFor.IsForever(), nil
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
