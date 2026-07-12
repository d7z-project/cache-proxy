package flatpak

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *Handler) serveDescriptor(w http.ResponseWriter, req *http.Request, cleanPath string) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		h.base.ServeHTTP(w, req)
		return
	}
	route, err := routeForPath(&Policy{
		MetadataFreshFor:   config.Freshness(defaultMetadataFreshFor),
		MetadataBusyPolicy: config.BusyPolicyStale,
	}, cleanPath)
	if err != nil {
		httpcache.ErrorResponse(http.StatusBadRequest, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", http.StatusBadRequest, 0)
		return
	}
	if body, headers, ok := h.openFreshDescriptor(req.Context(), route.ObjectPath); ok {
		h.flushDescriptor(w, req, rewriteDescriptor(req, body), headers, "FRESH")
		return
	}
	body, headers, err := h.fetchDescriptor(req.Context(), route)
	if err != nil {
		httpcache.ErrorResponse(http.StatusBadGateway, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	h.flushDescriptor(w, req, rewriteDescriptor(req, body), headers, "MISS")
}

func (h *Handler) openFreshDescriptor(ctx context.Context, objectPath string) ([]byte, map[string]string, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, nil, false
	}
	defer reader.Close()
	info := reader.Info()
	fetchedAt, err := utils.ParseFetchedAt(info.Options["fetched-at"])
	if err != nil || time.Since(fetchedAt) > defaultDescriptorFreshFor {
		return nil, nil, false
	}
	body, err := io.ReadAll(io.LimitReader(reader, maxDescriptorSize+1))
	if err != nil || len(body) > maxDescriptorSize {
		return nil, nil, false
	}
	headers := map[string]string{}
	for key, value := range info.Options {
		headers[httpcache.HeaderName(key)] = value
	}
	return body, headers, true
}

func (h *Handler) fetchDescriptor(ctx context.Context, route httpcache.Route) ([]byte, map[string]string, error) {
	var firstErr error
	for _, upstream := range h.upstreams {
		targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(route.UpstreamPath)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("create flatpak descriptor request: %w", err)
		}
		request.Header.Set("User-Agent", h.client.UserAgent)
		start := time.Now()
		response, err := h.client.Do(request)
		latency := time.Since(start)
		if err != nil {
			h.stats.RecordUpstreamRequest(h.name, config.ModeFlatpak, upstream, http.MethodGet, 0, latency, 0)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		body, headers, err := h.readDescriptorResponse(ctx, route.ObjectPath, upstream, response, latency)
		response.Body.Close()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		return body, headers, nil
	}
	if firstErr == nil {
		firstErr = errMetadataUnavailable
	}
	return nil, nil, firstErr
}

func (h *Handler) readDescriptorResponse(
	ctx context.Context,
	objectPath string,
	upstream string,
	response *http.Response,
	latency time.Duration,
) ([]byte, map[string]string, error) {
	h.stats.RecordUpstreamRequest(
		h.name,
		config.ModeFlatpak,
		upstream,
		http.MethodGet,
		response.StatusCode,
		latency,
		flatpakContentLength(response),
	)
	if response.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("flatpak descriptor upstream returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(utils.NewRateLimitReader(h.client.WrapBody(response.Body)), maxDescriptorSize+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read flatpak descriptor: %w", err)
	}
	if len(body) > maxDescriptorSize {
		return nil, nil, fmt.Errorf("flatpak descriptor exceeds %d bytes", maxDescriptorSize)
	}
	headers := map[string]string{}
	for key, value := range response.Header {
		if len(value) > 0 {
			headers[http.CanonicalHeaderKey(key)] = value[0]
		}
	}
	meta := map[string]string{
		"content-type":   headers["Content-Type"],
		"content-length": strconv.Itoa(len(body)),
		"last-modified":  headers["Last-Modified"],
		"etag":           headers["Etag"],
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           config.ModeFlatpak,
		"cache":          "MISS",
	}
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(objectPath)), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create flatpak descriptor directory: %w", err)
	}
	if _, err := h.store.Put(ctx, h.name, objectPath, bytes.NewReader(body), meta); err != nil {
		return nil, nil, fmt.Errorf("store flatpak descriptor: %w", err)
	}
	return body, headers, nil
}

func (h *Handler) flushDescriptor(
	w http.ResponseWriter,
	req *http.Request,
	body []byte,
	headers map[string]string,
	cacheStatus string,
) {
	responseHeaders := map[string]string{
		"Content-Length": strconv.Itoa(len(body)),
		"X-Cache":        cacheStatus,
	}
	for key, value := range headers {
		responseHeaders[httpcache.HeaderName(key)] = value
	}
	responseHeaders["Content-Length"] = strconv.Itoa(len(body))
	httpcache.StripInternal(responseHeaders)
	response := &utils.ResponseWrapper{
		StatusCode: http.StatusOK,
		Headers:    responseHeaders,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	_ = response.FlushClose(req, w)
	h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, cacheStatus, http.StatusOK, uint64(len(body)))
}
