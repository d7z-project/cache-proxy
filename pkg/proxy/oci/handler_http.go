package oci

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *handler) remoteRequest(ctx context.Context, method, upstreamPath string, headers map[string]string) (*http.Response, error) {
	targetURL := h.upstream + "/" + httpcache.EscapePath(strings.TrimLeft(upstreamPath, "/"))
	request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	if auth := h.staticAuthorization(); auth != "" {
		request.Header.Set("Authorization", auth)
	}
	slog.Debug("oci upstream request", "instance", h.name, "method", method, "url", targetURL)
	response, err := h.client.Do(request)
	if err != nil {
		h.stats.RecordUpstream(h.name, config.ModeOCI, method, 0)
		return nil, err
	}
	if response.StatusCode == http.StatusUnauthorized {
		retry, retryErr := h.retryChallenge(ctx, method, targetURL, headers, response)
		if retryErr != nil {
			h.stats.RecordUpstream(h.name, config.ModeOCI, method, 0)
			return nil, retryErr
		}
		if retry != nil {
			response = retry
		}
	}
	h.stats.RecordUpstream(h.name, config.ModeOCI, method, response.StatusCode)
	slog.Debug("oci upstream response", "instance", h.name, "method", method, "url", targetURL, "status", response.StatusCode)
	response.Body = utils.NewRateLimitReader(response.Body)
	return response, nil
}

func (h *handler) copyRemote(w http.ResponseWriter, req *http.Request, response *http.Response, cache string) (int, uint64, error) {
	return h.writeResponse(w, req.Method, response.StatusCode, objectHeaders(response.Header, int(response.ContentLength), cache), response.Body)
}

func (h *handler) writeResponse(w http.ResponseWriter, method string, status int, headers map[string]string, body io.Reader) (int, uint64, error) {
	for key, value := range headers {
		if value != "" {
			w.Header().Set(key, value)
		}
	}
	w.WriteHeader(status)
	if method == http.MethodHead || body == nil {
		return status, httpcache.ResponseBytes(headers), nil
	}
	written, err := io.Copy(w, body)
	return status, uint64(written), err
}
