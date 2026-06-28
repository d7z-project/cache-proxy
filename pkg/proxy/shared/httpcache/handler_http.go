package httpcache

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func RewriteNPMTarballs(value any, upstreams []string, publicBase string) bool {
	changed := false
	switch typed := value.(type) {
	case map[string]any:
		if dist, ok := typed["dist"].(map[string]any); ok {
			if tarball, ok := dist["tarball"].(string); ok {
				if rewritten := RewriteNPMTarballURL(tarball, upstreams, publicBase); rewritten != tarball {
					dist["tarball"] = rewritten
					changed = true
				}
			}
		}
		for _, nested := range typed {
			if RewriteNPMTarballs(nested, upstreams, publicBase) {
				changed = true
			}
		}
	case []any:
		for _, nested := range typed {
			if RewriteNPMTarballs(nested, upstreams, publicBase) {
				changed = true
			}
		}
	}
	return changed
}

func RewriteNPMTarballURL(rawURL string, upstreams []string, publicBase string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return rawURL
	}
	for _, upstream := range upstreams {
		base, err := url.Parse(upstream)
		if err != nil || base.Host == "" || !strings.EqualFold(parsed.Host, base.Host) {
			continue
		}
		basePath := strings.TrimRight(base.EscapedPath(), "/")
		tarballPath := parsed.EscapedPath()
		if basePath != "" {
			if tarballPath == basePath {
				tarballPath = ""
			} else if strings.HasPrefix(tarballPath, basePath+"/") {
				tarballPath = strings.TrimPrefix(tarballPath, basePath)
			} else {
				continue
			}
		}
		if tarballPath == "" {
			tarballPath = "/"
		}
		if parsed.RawQuery != "" {
			tarballPath += "?" + parsed.RawQuery
		}
		return strings.TrimRight(publicBase, "/") + tarballPath
	}
	return rawURL
}

func publicBaseURL(req *http.Request) string {
	prefix := strings.TrimRight(req.Header.Get("X-Cache-Proxy-Prefix"), "/")
	return BaseURL(req) + prefix
}

func (h *Handler) openRemote(ctx context.Context, method, upstreamPath string, options remoteOptions, headers map[string]string) (*utils.ResponseWrapper, error) {
	if options.TargetURL != "" {
		return h.doTargetURL(ctx, method, options, headers)
	}

	pathPart, rawQuery, _ := strings.Cut(upstreamPath, "?")
	upstreams := h.buildUpstreamList()

	var lastErr error
	for i, candidate := range upstreams {
		result, err := h.tryUpstream(ctx, method, pathPart, rawQuery, candidate, i, len(upstreams), options, headers)
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return nil, fmt.Errorf("no upstream url configured")
	}
	return nil, lastErr
}

func (h *Handler) doTargetURL(ctx context.Context, method string, options remoteOptions, headers map[string]string) (*utils.ResponseWrapper, error) {
	if err := h.validateTargetURL(options.TargetURL, options.AllowedTargetHosts); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, options.TargetURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := h.client.Do(request)
	if err != nil {
		if options.Record {
			h.stats.RecordUpstream(h.name, h.config.Mode, method, 0)
		}
		return nil, err
	}
	if options.Record {
		h.stats.RecordUpstream(h.name, h.config.Mode, method, response.StatusCode)
	}
	if !options.AcceptErrors && response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return nil, fmt.Errorf("upstream %s failed with %d", method, response.StatusCode)
	}
	return responseFromHTTP(response), nil
}

func (h *Handler) validateTargetURL(rawURL string, routeAllowed []string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid target url")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("target url must use http or https")
	}
	allowed := append([]string(nil), h.config.AllowedTargetHosts...)
	allowed = append(allowed, routeAllowed...)
	for _, upstream := range h.config.Upstreams {
		if parsedUpstream, err := url.Parse(upstream); err == nil && parsedUpstream.Host != "" {
			allowed = append(allowed, parsedUpstream.Host)
		}
	}
	for _, host := range allowed {
		if strings.EqualFold(parsed.Host, host) {
			return nil
		}
	}
	return fmt.Errorf("target url host %q is not allowed", parsed.Host)
}

func (h *Handler) buildUpstreamList() []struct {
	URL    string
	Weight float64
} {
	if h.health != nil {
		weighted := h.health.WeightedUpstreams(h.config.Upstreams)
		upstreams := make([]struct {
			URL    string
			Weight float64
		}, 0, len(weighted))
		for _, wu := range weighted {
			upstreams = append(upstreams, struct {
				URL    string
				Weight float64
			}{wu.URL, wu.Weight})
		}
		return upstreams
	}
	upstreams := make([]struct {
		URL    string
		Weight float64
	}, 0, len(h.config.Upstreams))
	for _, url := range h.config.Upstreams {
		upstreams = append(upstreams, struct {
			URL    string
			Weight float64
		}{url, 1.0})
	}
	return upstreams
}

func (h *Handler) tryUpstream(ctx context.Context, method, pathPart, rawQuery string, candidate struct {
	URL    string
	Weight float64
}, idx, total int, options remoteOptions, headers map[string]string) (*utils.ResponseWrapper, error) {
	targetURL := strings.TrimRight(candidate.URL, "/") + "/" + EscapePath(pathPart)
	if rawQuery != "" {
		targetURL += "?" + rawQuery
	}
	request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		slog.Debug("upstream request build failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
		return nil, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		if options.Record {
			h.stats.RecordUpstream(h.name, h.config.Mode, method, 0)
		}
		if h.health != nil {
			h.health.RecordFailure(candidate.URL, err)
		}
		slog.Debug("upstream request failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
		if idx+1 < total {
			slog.Debug("upstream failover retry", "instance", h.name, "method", method, "from", redactedURL(targetURL))
		}
		return nil, err
	}
	slog.Debug("upstream response received", "instance", h.name, "method", method, "url", redactedURL(targetURL), "status", response.StatusCode)
	if options.Record {
		h.stats.RecordUpstream(h.name, h.config.Mode, method, response.StatusCode)
	}
	if h.health != nil {
		h.health.RecordResult(candidate.URL, response.StatusCode, latency)
	}
	if options.AcceptErrors && shouldFailoverStatus(response.StatusCode) && idx+1 < total {
		_ = response.Body.Close()
		err = fmt.Errorf("upstream %s returned retryable status %d", method, response.StatusCode)
		slog.Debug("upstream failover retry", "instance", h.name, "method", method, "from", redactedURL(targetURL), "status", response.StatusCode)
		return nil, err
	}
	if !options.AcceptErrors && response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		err = fmt.Errorf("upstream %s failed with %d", method, response.StatusCode)
		if idx+1 < total {
			slog.Debug("upstream failover retry", "instance", h.name, "method", method, "from", redactedURL(targetURL))
		}
		return nil, err
	}
	return responseFromHTTP(response), nil
}

func shouldFailoverStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func (h *Handler) requestHeaders(req *http.Request) map[string]string {
	headers := map[string]string{}
	if value := req.Header.Get("Range"); value != "" {
		headers["Range"] = value
	}
	if len(h.config.PassHeaders) == 0 {
		return headers
	}
	for _, name := range h.config.PassHeaders {
		if !passableHeader(name) {
			continue
		}
		if value := req.Header.Get(name); value != "" {
			headers[http.CanonicalHeaderKey(name)] = value
		}
	}
	return headers
}

func (h *Handler) remoteHeaders(req *http.Request, route Route, extra map[string]string) map[string]string {
	headers := map[string]string{}
	if req != nil {
		headers = h.requestHeaders(req)
	}
	for key, value := range route.RequestHeaders {
		headers[key] = value
	}
	for key, value := range extra {
		headers[key] = value
	}
	return headers
}

func passableHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "host", "authorization",
		"x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-prefix", "x-real-ip":
		return false
	default:
		return true
	}
}
