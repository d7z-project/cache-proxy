package oci

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

func (h *handler) readUpstream(admissionCtx, transferCtx context.Context, method, upstreamPath, rawQuery, userAgent string, headers map[string]string) (*http.Response, error) {
	if method != http.MethodGet && method != http.MethodHead {
		return nil, errors.New("oci upstream read must use GET or HEAD")
	}
	targetURL := h.upstream + "/" + transport.EscapePathSegments(strings.TrimLeft(upstreamPath, "/"))
	if rawQuery != "" {
		targetURL += "?" + rawQuery
	}
	if userAgent == "" {
		userAgent = h.client.UserAgent
	}
	send := func(authorization string) (*http.Response, error) {
		request, err := http.NewRequestWithContext(transferCtx, method, targetURL, nil)
		if err != nil {
			return nil, err
		}
		request = transport.WithAdmission(admissionCtx, request, transport.AdmissionForeground)
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
			h.stats.RecordUpstreamRequest(h.name, config.ModeOCI, h.upstream, method, 0, latency, 0)
			return nil, err
		}
		slog.Debug("oci upstream response", "instance", h.name, "method", method, "url", targetURL, "status", response.StatusCode)
		counted := &countingReadCloser{ReadCloser: h.client.WrapBody(response.Body)}
		status := response.StatusCode
		response.Body = &closeCallbackBody{ReadCloser: counted, done: func() {
			releaseStats()
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
	authorization, err := h.authorizationForChallenge(transferCtx, challenge)
	if err != nil || authorization == "" {
		return nil, err
	}
	h.auth.tokenMu.Lock()
	h.auth.preemptive = authorization
	h.auth.tokenMu.Unlock()
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
	transport.CopyEndToEndHeaders(w.Header(), response.Header)
	w.Header().Set("X-Cache", cache)
	w.WriteHeader(response.StatusCode)
	if req.Method == http.MethodHead {
		return response.StatusCode, uint64(max(response.ContentLength, 0)), nil
	}
	written, err := io.Copy(w, response.Body)
	return response.StatusCode, uint64(written), err
}

func (h *handler) writeResponse(w http.ResponseWriter, method string, headers map[string]string, body io.Reader) (int, uint64, error) {
	for key, value := range headers {
		if value != "" {
			w.Header().Set(key, value)
		}
	}
	w.WriteHeader(http.StatusOK)
	if method == http.MethodHead || body == nil {
		return http.StatusOK, responseBytes(headers), nil
	}
	written, err := io.Copy(w, body)
	return http.StatusOK, uint64(written), err
}
