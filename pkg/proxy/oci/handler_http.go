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

func (h *handler) remoteRequest(ctx context.Context, method, upstreamPath, userAgent string, headers map[string]string) (*http.Response, error) {
	targetURL := h.upstream + "/" + httpcache.EscapePath(strings.TrimLeft(upstreamPath, "/"))
	if userAgent == "" {
		userAgent = h.client.UserAgent
	}
	send := func(authorization string) (*http.Response, error) {
		releaseAdmission, err := h.downloadsLimiter.AcquireUpstream(ctx, h.name, h.upstream, false)
		if err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
		if err != nil {
			releaseAdmission()
			return nil, err
		}
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		request.Header.Set("User-Agent", userAgent)
		if authorization == "" {
			authorization = h.staticAuthorization()
		}
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		slog.Debug("oci upstream request", "instance", h.name, "method", method, "url", targetURL)
		releaseStats := h.stats.BeginUpstreamRequest(h.name, config.ModeOCI, h.upstream)
		start := time.Now()
		response, err := h.client.Do(request)
		latency := time.Since(start)
		if err != nil {
			releaseStats()
			releaseAdmission()
			h.stats.RecordUpstreamRequest(h.name, config.ModeOCI, h.upstream, method, 0, latency, 0)
			return nil, err
		}
		if response.StatusCode == http.StatusTooManyRequests {
			h.downloadsLimiter.ObserveResponse(h.upstream, response.StatusCode, response.Header.Get("Retry-After"))
		}
		slog.Debug("oci upstream response", "instance", h.name, "method", method, "url", targetURL, "status", response.StatusCode)
		counted := &countingReadCloser{ReadCloser: utils.NewRateLimitReader(h.client.WrapBody(response.Body))}
		status := response.StatusCode
		response.Body = &closeCallbackBody{ReadCloser: counted, done: func() {
			releaseStats()
			releaseAdmission()
			h.stats.RecordUpstreamRequest(h.name, config.ModeOCI, h.upstream, method, status, latency, counted.bytes)
		}}
		return response, nil
	}

	response, err := send("")
	if err != nil || response.StatusCode != http.StatusUnauthorized {
		return response, err
	}
	challenge, ok := parseOCIChallenge(response.Header.Get("WWW-Authenticate"))
	scheme := strings.ToLower(challenge.scheme)
	if !ok || (scheme != "bearer" && scheme != "basic") {
		return response, nil
	}
	if scheme == "basic" && h.basicAuthorization() == "" {
		return response, nil
	}
	_ = response.Body.Close()
	authorization, err := h.authorizationForChallenge(ctx, challenge)
	if err != nil || authorization == "" {
		return nil, err
	}
	return send(authorization)
}

type closeCallbackBody struct {
	io.ReadCloser
	done func()
	once sync.Once
}

type countingReadCloser struct {
	io.ReadCloser
	bytes uint64
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes += uint64(n)
	return n, err
}

func (b *closeCallbackBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
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
