package flatpak

import (
	"bytes"
	"context"
	"errors"
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
	route, err := routeForPath(&Policy{}, cleanPath)
	if err != nil {
		_ = httpcache.ErrorResponse(http.StatusBadRequest, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", http.StatusBadRequest, 0)
		return
	}
	if body, headers, ok := h.openFreshDescriptor(req.Context(), req, route.ObjectPath); ok {
		h.flushDescriptor(w, req, http.StatusOK, rewriteDescriptor(req, body), headers, "FRESH")
		return
	}
	userAgent, _ := h.client.RequestUserAgent(req)
	body, headers, cacheStatus, status, err := h.fetchDescriptor(req.Context(), route, userAgent, !h.client.UserAgentConfigured)
	if err != nil {
		status := http.StatusBadGateway
		if retryAfter, admissionError := httpcache.AdmissionRetryAfterSeconds(err); admissionError {
			status = http.StatusServiceUnavailable
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		_ = httpcache.ErrorResponse(status, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", status, 0)
		return
	}
	if status == http.StatusOK {
		body = rewriteDescriptor(req, body)
	}
	h.flushDescriptor(w, req, status, body, headers, cacheStatus)
}

func (h *Handler) openFreshDescriptor(ctx context.Context, req *http.Request, objectPath string) ([]byte, map[string]string, bool) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return nil, nil, false
	}
	defer func() { _ = reader.Close() }()
	info := reader.Info()
	if !httpcache.CacheSupportsRequestUserAgent(h.client, req, info.Options) {
		return nil, nil, false
	}
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

func (h *Handler) fetchDescriptor(
	ctx context.Context,
	route httpcache.Route,
	userAgent string,
	rejectUserAgentVariants bool,
) ([]byte, map[string]string, string, int, error) {
	var firstErr error
	for _, upstream := range h.upstreams {
		release, err := h.upstreamGate.Acquire(ctx, upstream, httpcache.AdmissionForeground)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, httpcache.ErrAdmissionOverloaded) {
				return nil, nil, "", 0, err
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(route.UpstreamPath)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			release()
			return nil, nil, "", 0, fmt.Errorf("create flatpak descriptor request: %w", err)
		}
		request.Header.Set("User-Agent", userAgent)
		start := time.Now()
		response, err := h.client.Do(request)
		latency := time.Since(start)
		if err != nil {
			release()
			h.stats.RecordUpstreamRequest(h.name, config.ModeFlatpak, upstream, http.MethodGet, 0, latency, 0)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		storeResponse := !rejectUserAgentVariants || !utils.VariesByUserAgent(response.Header.Values("Vary")...)
		h.stats.RecordUpstreamRequest(
			h.name,
			config.ModeFlatpak,
			upstream,
			http.MethodGet,
			response.StatusCode,
			latency,
			flatpakContentLength(response),
		)
		body, readErr := io.ReadAll(io.LimitReader(utils.NewRateLimitReader(h.client.WrapBody(response.Body)), maxDescriptorSize+1))
		headers := map[string]string{}
		for key, value := range response.Header {
			if len(value) > 0 {
				headers[http.CanonicalHeaderKey(key)] = strings.Join(value, ", ")
			}
		}
		_ = response.Body.Close()
		release()
		if readErr != nil {
			return nil, nil, "", 0, fmt.Errorf("read flatpak descriptor: %w", readErr)
		}
		if len(body) > maxDescriptorSize {
			return nil, nil, "", 0, fmt.Errorf("flatpak descriptor exceeds %d bytes", maxDescriptorSize)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			_ = h.upstreamGate.RateLimited(upstream, response.Header.Get("Retry-After"))
			return body, headers, "BYPASS", response.StatusCode, nil
		}
		if response.StatusCode != http.StatusOK {
			if response.StatusCode != http.StatusBadGateway && response.StatusCode != http.StatusServiceUnavailable && response.StatusCode != http.StatusGatewayTimeout {
				return body, headers, "BYPASS", response.StatusCode, nil
			}
			err = fmt.Errorf("flatpak descriptor upstream returned HTTP %d", response.StatusCode)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !storeResponse {
			return body, headers, "BYPASS", http.StatusOK, nil
		}
		meta := map[string]string{
			"content-type":                    headers["Content-Type"],
			"content-length":                  strconv.Itoa(len(body)),
			"last-modified":                   headers["Last-Modified"],
			"etag":                            headers["Etag"],
			"vary":                            headers["Vary"],
			"fetched-at":                      time.Now().UTC().Format(time.RFC3339Nano),
			"mode":                            config.ModeFlatpak,
			"cache":                           "MISS",
			httpcache.UserAgentReviewedOption: "true",
		}
		if err := h.store.MkdirAll(path.Join(h.name, path.Dir(route.ObjectPath)), 0o755); err != nil {
			return nil, nil, "", 0, fmt.Errorf("create flatpak descriptor directory: %w", err)
		}
		if _, err := h.store.Put(ctx, h.name, route.ObjectPath, bytes.NewReader(body), meta); err != nil {
			return nil, nil, "", 0, fmt.Errorf("store flatpak descriptor: %w", err)
		}
		return body, headers, "MISS", http.StatusOK, nil
	}
	if firstErr == nil {
		firstErr = errMetadataUnavailable
	}
	return nil, nil, "", 0, firstErr
}

func (h *Handler) flushDescriptor(
	w http.ResponseWriter,
	req *http.Request,
	status int,
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
		StatusCode: status,
		Headers:    responseHeaders,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	_ = response.FlushClose(req, w)
	h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, cacheStatus, status, uint64(len(body)))
}
