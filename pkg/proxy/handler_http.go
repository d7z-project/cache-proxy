package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func rewriteNPMTarballs(value any, upstreams []string, publicBase string) bool {
	changed := false
	switch typed := value.(type) {
	case map[string]any:
		if dist, ok := typed["dist"].(map[string]any); ok {
			if tarball, ok := dist["tarball"].(string); ok {
				if rewritten := rewriteNPMTarballURL(tarball, upstreams, publicBase); rewritten != tarball {
					dist["tarball"] = rewritten
					changed = true
				}
			}
		}
		for _, nested := range typed {
			if rewriteNPMTarballs(nested, upstreams, publicBase) {
				changed = true
			}
		}
	case []any:
		for _, nested := range typed {
			if rewriteNPMTarballs(nested, upstreams, publicBase) {
				changed = true
			}
		}
	}
	return changed
}

func rewriteNPMTarballURL(rawURL string, upstreams []string, publicBase string) string {
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
	scheme := req.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if req.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := req.Host
	if forwardedHost := req.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		host = forwardedHost
	}
	prefix := strings.TrimRight(req.Header.Get("X-Cache-Proxy-Prefix"), "/")
	return scheme + "://" + host + prefix
}

func (h *Handler) openRemote(ctx context.Context, method, upstreamPath string, options remoteOptions, headers map[string]string) (*utils.ResponseWrapper, error) {
	var lastErr error
	if target := headers[""]; target != "" {
		requestHeaders := mapsCloneWithoutEmptyKey(headers)
		request, err := http.NewRequestWithContext(ctx, method, target, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("User-Agent", h.client.UserAgent)
		for key, value := range requestHeaders {
			request.Header.Set(key, value)
		}
		if auth := h.staticAuthorization(); auth != "" && request.Header.Get("Authorization") == "" {
			request.Header.Set("Authorization", auth)
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
	for _, baseURL := range h.config.Upstreams {
		pathPart, rawQuery, _ := strings.Cut(upstreamPath, "?")
		targetURL := strings.TrimRight(baseURL, "/") + "/" + EscapePath(pathPart)
		if rawQuery != "" {
			targetURL += "?" + rawQuery
		}
		request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
		if err != nil {
			lastErr = err
			slog.Debug("upstream request build failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
			continue
		}
		request.Header.Set("User-Agent", h.client.UserAgent)
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		if auth := h.staticAuthorization(); auth != "" && request.Header.Get("Authorization") == "" {
			request.Header.Set("Authorization", auth)
		}
		response, err := h.client.Do(request)
		if err != nil {
			if options.Record {
				h.stats.RecordUpstream(h.name, h.config.Mode, method, 0)
			}
			lastErr = err
			slog.Debug("upstream request failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
			continue
		}
		slog.Debug("upstream response received", "instance", h.name, "method", method, "url", redactedURL(targetURL), "status", response.StatusCode)
		if h.config.Mode == config.ModeOCI && response.StatusCode == http.StatusUnauthorized {
			retry, retryErr := h.retryOCIChallenge(ctx, method, targetURL, headers, response)
			if retryErr != nil {
				lastErr = retryErr
				continue
			}
			if retry != nil {
				response = retry
				slog.Debug("upstream response received after oci auth", "instance", h.name, "method", method, "url", redactedURL(targetURL), "status", response.StatusCode)
			}
		}
		if options.Record {
			h.stats.RecordUpstream(h.name, h.config.Mode, method, response.StatusCode)
		}
		if !options.AcceptErrors && response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			lastErr = fmt.Errorf("upstream %s failed with %d", method, response.StatusCode)
			continue
		}
		return responseFromHTTP(response), nil
	}
	if lastErr == nil {
		lastErr = errors.New("no upstream url configured")
	}
	return nil, lastErr
}

func (h *Handler) requestHeaders(req *http.Request) map[string]string {
	headers := map[string]string{}
	if value := req.Header.Get("Range"); value != "" {
		headers["Range"] = value
	}
	if h.config.Mode == config.ModeOCI {
		headers["Accept"] = ociManifestAccept
	}
	if len(h.config.PassHeaders) == 0 {
		return headers
	}
	for _, name := range h.passHeaders() {
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
	if route.TargetURL != "" {
		headers[""] = route.TargetURL
	}
	return headers
}

func mapsCloneWithoutEmptyKey(headers map[string]string) map[string]string {
	clone := make(map[string]string, len(headers))
	for key, value := range headers {
		if key == "" {
			continue
		}
		clone[key] = value
	}
	return clone
}

func (h *Handler) passHeaders() []string {
	return h.config.PassHeaders
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
