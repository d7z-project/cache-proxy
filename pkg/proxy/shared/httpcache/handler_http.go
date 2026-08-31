package httpcache

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

var ErrUpstreamUnavailable = errors.New("upstream unavailable")

const responseSourceUpstreamHeader = "source-upstream"

type upstreamCandidate struct {
	URL       string
	Preferred bool
}

func RewriteNPMMetadata(src io.Reader, dst io.Writer, upstreams []string, publicBase string) error {
	decoder := json.NewDecoder(src)
	decoder.UseNumber()
	buffered := bufio.NewWriterSize(dst, 32<<10)
	if err := rewriteNPMJSONValue(decoder, buffered, upstreams, publicBase, false, false); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected JSON token %v", token)
	}
	return buffered.Flush()
}

func rewriteNPMJSONValue(decoder *json.Decoder, dst io.Writer, upstreams []string, publicBase string, distObject, tarballValue bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if tarball, ok := token.(string); ok && tarballValue {
		token = RewriteNPMTarballURL(tarball, upstreams, publicBase)
	}
	delim, composite := token.(json.Delim)
	if !composite {
		encoded, err := json.Marshal(token)
		if err == nil {
			_, err = dst.Write(encoded)
		}
		return err
	}
	if _, err := io.WriteString(dst, string(delim)); err != nil {
		return err
	}
	switch delim {
	case '{':
		first := true
		for decoder.More() {
			if !first {
				if _, err := io.WriteString(dst, ","); err != nil {
					return err
				}
			}
			first = false
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("json object key is not a string")
			}
			encodedKey, err := json.Marshal(key)
			if err != nil {
				return err
			}
			if _, err := dst.Write(encodedKey); err != nil {
				return err
			}
			if _, err := io.WriteString(dst, ":"); err != nil {
				return err
			}
			if err := rewriteNPMJSONValue(decoder, dst, upstreams, publicBase, key == "dist", distObject && key == "tarball"); err != nil {
				return err
			}
		}
	case '[':
		first := true
		for decoder.More() {
			if !first {
				if _, err := io.WriteString(dst, ","); err != nil {
					return err
				}
			}
			first = false
			if err := rewriteNPMJSONValue(decoder, dst, upstreams, publicBase, false, false); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	_, err = io.WriteString(dst, string(closing.(json.Delim)))
	return err
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
		return h.openTargetURL(ctx, method, options, headers)
	}

	pathPart, rawQuery, _ := strings.Cut(upstreamPath, "?")
	upstreams := h.buildUpstreamList(options)

	var lastErr error
	for i, candidate := range upstreams {
		result, err := h.tryUpstream(ctx, method, pathPart, rawQuery, candidate, i, len(upstreams), options, headers)
		if err == nil {
			slog.Debug("upstream selected", "instance", h.name, "method", method, "path", upstreamPath, "upstream", redactedURL(candidate.URL))
			return result, nil
		}
		if !errors.Is(err, ErrUpstreamUnavailable) || ctx.Err() != nil || options.DisableFailover {
			return nil, err
		}
		lastErr = err
	}
	if lastErr == nil {
		if len(h.config.Upstreams) > 0 {
			return nil, fmt.Errorf("%w: no upstream is available", ErrUpstreamUnavailable)
		}
		return nil, errors.New("no upstream url configured")
	}
	return nil, lastErr
}

func (h *Handler) openTargetURL(ctx context.Context, method string, options remoteOptions, headers map[string]string) (*utils.ResponseWrapper, error) {
	if err := h.validateTargetURL(options.TargetURL, options.AllowedTargetHosts); err != nil {
		return nil, err
	}
	admissionCtx := options.AdmissionContext
	if admissionCtx == nil {
		admissionCtx = ctx
	}
	releaseAdmission, err := h.upstreamGate.Acquire(admissionCtx, options.TargetURL, AdmissionForeground)
	if err != nil {
		return nil, err
	}
	if err := admissionCtx.Err(); err != nil {
		releaseAdmission()
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, options.TargetURL, nil)
	if err != nil {
		releaseAdmission()
		return nil, err
	}
	userAgent := options.UserAgent
	if userAgent == "" {
		userAgent = h.client.UserAgent
	}
	for key, value := range headersForOrigin(headers, options.ValidatorOrigin, statsUpstreamKey(options.TargetURL)) {
		request.Header.Set(key, value)
	}
	request.Header.Set("User-Agent", userAgent)
	statsUpstream := statsUpstreamKey(options.TargetURL)
	release := func() {}
	if options.Record {
		release = h.stats.BeginUpstreamRequest(h.name, h.config.Mode, statsUpstream)
	}
	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		releaseAdmission()
		release()
		if options.Record {
			h.stats.RecordUpstreamRequest(h.name, h.config.Mode, statsUpstream, method, 0, latency, 0)
			if h.health != nil {
				h.health.RecordFailure(options.TargetURL, err)
			}
		}
		return nil, fmt.Errorf("%w: %w", ErrUpstreamUnavailable, err)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		_ = h.upstreamGate.RateLimited(options.TargetURL, response.Header.Get("Retry-After"))
	}
	if options.Record && h.health != nil {
		h.health.RecordResult(options.TargetURL, response.StatusCode, latency)
	}
	slog.Debug("target url success", "instance", h.name, "method", method, "url", redactedURL(options.TargetURL), "status", response.StatusCode, "latency", latency)
	result := responseFromHTTP(h.client, response)
	result.Headers[responseSourceUpstreamHeader] = statsUpstream
	result.Body = h.recordUpstreamBody(result.Body, func() { releaseAdmission(); release() }, options.Record, statsUpstream, method, response.StatusCode, latency)
	return result, nil
}

func (h *Handler) validateTargetURL(rawURL string, routeAllowed []string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("invalid target url")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("target url must use http or https")
	}
	allowed := append(h.parsedUpstreamHosts, h.config.AllowedTargetHosts...)
	allowed = append(allowed, routeAllowed...)
	for _, host := range allowed {
		if strings.EqualFold(parsed.Host, host) {
			return nil
		}
	}
	return fmt.Errorf("target url host %q is not allowed", parsed.Host)
}

func (h *Handler) buildUpstreamList(options remoteOptions) []upstreamCandidate {
	upstreams := make([]upstreamCandidate, 0, len(h.config.Upstreams))
	for _, upstream := range h.config.Upstreams {
		upstreams = append(upstreams, upstreamCandidate{
			URL:       upstream,
			Preferred: upstream == options.PreferredUpstream,
		})
	}
	if options.PreferredUpstream != "" {
		for i := range upstreams {
			if upstreams[i].URL == options.PreferredUpstream {
				item := upstreams[i]
				copy(upstreams[1:i+1], upstreams[0:i])
				upstreams[0] = item
				break
			}
		}
	}
	return upstreams
}

func (h *Handler) tryUpstream(
	ctx context.Context,
	method, pathPart, rawQuery string,
	candidate upstreamCandidate,
	idx, total int,
	options remoteOptions,
	headers map[string]string,
) (*utils.ResponseWrapper, error) {
	targetURL := strings.TrimRight(candidate.URL, "/") + "/" + EscapePath(pathPart)
	if rawQuery != "" {
		targetURL += "?" + rawQuery
	}
	admissionCtx := options.AdmissionContext
	if admissionCtx == nil {
		admissionCtx = ctx
	}
	releaseAdmission, err := h.upstreamGate.Acquire(admissionCtx, candidate.URL, AdmissionForeground)
	if err != nil {
		return nil, err
	}
	if err := admissionCtx.Err(); err != nil {
		releaseAdmission()
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		releaseAdmission()
		slog.Debug("upstream request build failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
		return nil, err
	}
	userAgent := options.UserAgent
	if userAgent == "" {
		userAgent = h.client.UserAgent
	}
	for key, value := range headersForOrigin(headers, options.ValidatorOrigin, candidate.URL) {
		request.Header.Set(key, value)
	}
	request.Header.Set("User-Agent", userAgent)
	release := func() {}
	if options.Record {
		release = h.stats.BeginUpstreamRequest(h.name, h.config.Mode, candidate.URL)
	}
	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		releaseAdmission()
		release()
		if options.Record {
			h.stats.RecordUpstreamRequest(h.name, h.config.Mode, candidate.URL, method, 0, latency, 0)
		}
		if h.health != nil {
			h.health.RecordFailure(candidate.URL, err)
		}
		slog.Debug("upstream request failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
		if idx+1 < total {
			slog.Debug("upstream failover retry", "instance", h.name, "method", method, "from", redactedURL(targetURL))
		}
		return nil, fmt.Errorf("%w: %w", ErrUpstreamUnavailable, err)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		_ = h.upstreamGate.RateLimited(candidate.URL, response.Header.Get("Retry-After"))
	}
	slog.Debug("upstream response received", "instance", h.name, "method", method, "url", redactedURL(targetURL), "upstream", redactedURL(candidate.URL), "status", response.StatusCode, "latency", latency)
	if h.health != nil {
		h.health.RecordResult(candidate.URL, response.StatusCode, latency)
	}
	if options.AcceptErrors && upstreamStatusIsFailure(response.StatusCode) {
		if options.Record {
			h.stats.RecordUpstreamRequest(h.name, h.config.Mode, candidate.URL, method, response.StatusCode, latency, 0)
		}
		_ = response.Body.Close()
		releaseAdmission()
		release()
		err = fmt.Errorf("%w: upstream %s returned retryable status %d", ErrUpstreamUnavailable, method, response.StatusCode)
		h.logUpstreamFailover(method, pathPart, candidate.URL, response.StatusCode, err)
		if idx+1 < total {
			slog.Debug("upstream failover retry", "instance", h.name, "method", method, "from", redactedURL(targetURL), "status", response.StatusCode)
		}
		return nil, err
	}
	if !options.AcceptErrors && response.StatusCode != http.StatusOK {
		if options.Record {
			h.stats.RecordUpstreamRequest(h.name, h.config.Mode, candidate.URL, method, response.StatusCode, latency, 0)
		}
		_ = response.Body.Close()
		releaseAdmission()
		release()
		if upstreamStatusIsFailure(response.StatusCode) {
			err = fmt.Errorf("%w: upstream %s failed with %d", ErrUpstreamUnavailable, method, response.StatusCode)
		} else {
			err = fmt.Errorf("upstream %s failed with %d", method, response.StatusCode)
		}
		if errors.Is(err, ErrUpstreamUnavailable) && idx+1 < total {
			slog.Debug("upstream failover retry", "instance", h.name, "method", method, "from", redactedURL(targetURL))
		}
		return nil, err
	}
	result := responseFromHTTP(h.client, response)
	result.Headers[responseSourceUpstreamHeader] = candidate.URL
	result.Body = h.recordUpstreamBody(result.Body, func() { releaseAdmission(); release() }, options.Record, candidate.URL, method, response.StatusCode, latency)
	return result, nil
}

func headersForOrigin(headers map[string]string, validatorOrigin, targetOrigin string) map[string]string {
	if validatorOrigin != "" && validatorOrigin == targetOrigin {
		return headers
	}
	filtered := make(map[string]string, len(headers))
	for key, value := range headers {
		if strings.EqualFold(key, "If-None-Match") || strings.EqualFold(key, "If-Modified-Since") {
			continue
		}
		filtered[key] = value
	}
	return filtered
}

func (h *Handler) recordUpstreamBody(body io.ReadCloser, release func(), record bool, upstream, method string, status int, latency time.Duration) io.ReadCloser {
	counted := &countingBody{ReadCloser: body}
	return &closeCallbackBody{ReadCloser: counted, done: func() {
		release()
		if record {
			h.stats.RecordUpstreamRequest(h.name, h.config.Mode, upstream, method, status, latency, counted.bytesRead())
		}
	}}
}

func statsUpstreamKey(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return rawURL
	}
	return strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host)
}

func upstreamStatusIsFailure(status int) bool {
	return status == 0 || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

func (h *Handler) logUpstreamFailover(method, upstreamPath, upstream string, status int, err error) {
	slog.Warn("upstream response triggered failover", "instance", h.name, "mode", h.config.Mode,
		"method", method, "upstream_path", upstreamPath, "upstream", redactedURL(upstream),
		"status", status, "reason", "retryable_failure", "err", err)
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
	case "", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "host", "authorization", "user-agent",
		"x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-prefix", "x-real-ip":
		return false
	default:
		return true
	}
}
