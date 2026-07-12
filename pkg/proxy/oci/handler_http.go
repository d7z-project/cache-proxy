package oci

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

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
	release := h.stats.BeginUpstreamRequest(h.name, config.ModeOCI, h.upstream)
	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		release()
		h.stats.RecordUpstreamRequest(h.name, config.ModeOCI, h.upstream, method, 0, latency, 0)
		return nil, err
	}
	if response.StatusCode == http.StatusUnauthorized {
		retry, retryErr := h.retryChallenge(ctx, method, targetURL, headers, response)
		if retryErr != nil {
			release()
			h.stats.RecordUpstreamRequest(h.name, config.ModeOCI, h.upstream, method, 0, latency, 0)
			return nil, retryErr
		}
		if retry != nil {
			_ = response.Body.Close()
			response = retry
		}
	}
	h.stats.RecordUpstreamRequest(
		h.name,
		config.ModeOCI,
		h.upstream,
		method,
		response.StatusCode,
		latency,
		ociContentLength(response),
	)
	slog.Debug("oci upstream response", "instance", h.name, "method", method, "url", targetURL, "status", response.StatusCode)
	response.Body = utils.NewRateLimitReader(h.client.WrapBody(response.Body))
	response.Body = &closeCallbackBody{ReadCloser: response.Body, done: release}
	return response, nil
}

type closeCallbackBody struct {
	io.ReadCloser
	done func()
	once sync.Once
}

func (b *closeCallbackBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}

func ociContentLength(response *http.Response) uint64 {
	if response == nil || response.ContentLength <= 0 {
		return 0
	}
	return uint64(response.ContentLength)
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
