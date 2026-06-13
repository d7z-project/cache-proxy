package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type Route struct {
	ObjectPath         string
	UpstreamPath       string
	Policy             string
	FreshFor           config.Duration
	ExpireAfter        config.Duration
	RewriteNPMMetadata bool
}

type Resolver interface {
	Resolve(req *http.Request) (Route, error)
}

type Handler struct {
	name     string
	cfg      config.InstanceConfig
	store    *blobfs.Store
	client   *utils.HttpClientWrapper
	locks    *utils.RWLockGroup
	resolver Resolver
	stats    *Stats
	wait     sync.WaitGroup

	ociTokenMu sync.Mutex
	ociTokens  map[string]ociToken
	ociGroup   singleflight.Group
}

type remoteOptions struct {
	AcceptErrors bool
	Record       bool
}

type ociToken struct {
	value  string
	expire time.Time
}

type ociChallenge struct {
	scheme string
	realm  string
	params map[string]string
}

const ociManifestAccept = "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v1+json, application/json"

func NewHandler(name string, cfg config.InstanceConfig, store *blobfs.Store, resolver Resolver, stats *Stats) *Handler {
	client := utils.DefaultHttpClientWrapper()
	if cfg.Transport != nil {
		if cfg.Transport.UserAgent != "" {
			client.UserAgent = cfg.Transport.UserAgent
		}
		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			slog.Warn("cannot configure transport options, unexpected transport type", "instance", name)
		} else {
			if cfg.Transport.Proxy != "" {
				proxyURL, err := url.Parse(cfg.Transport.Proxy)
				if err == nil {
					transport.Proxy = http.ProxyURL(proxyURL)
				} else {
					slog.Warn("invalid transport proxy URL", "instance", name, "proxy", cfg.Transport.Proxy, "err", err)
				}
			}
			if cfg.Transport.Timeout > 0 {
				transport.DialContext = utils.DefaultDialContext(cfg.Transport.Timeout.Duration())
			}
		}
	}
	return &Handler{name: name, cfg: cfg, store: store, client: client, locks: utils.NewRWLockGroup(), resolver: resolver, stats: stats, ociTokens: map[string]ociToken{}}
}

func (h *Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		resp.Header().Set("Allow", "GET, HEAD")
		http.Error(resp, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, h.cfg.Mode, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	result, err := h.handle(req.Context(), req)
	if err != nil {
		slog.Warn("proxy request failed", "instance", h.name, "mode", h.cfg.Mode, "method", req.Method, "path", req.URL.Path, "err", err)
		http.Error(resp, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, h.cfg.Mode, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	status := result.StatusCode
	cache := result.Headers["X-Cache"]
	bytes := responseBytes(result.Headers)
	if err := result.FlushClose(req, resp); err != nil {
		slog.Warn("flush response failed", "instance", h.name, "err", err)
		if status < 500 {
			status = http.StatusBadGateway
		}
	}
	h.stats.RecordRequest(h.name, h.cfg.Mode, req.Method, cache, status, bytes)
}

func (h *Handler) Close() {
	h.wait.Wait()
}

func (h *Handler) handle(ctx context.Context, req *http.Request) (*utils.ResponseWrapper, error) {
	h.wait.Add(1)
	defer h.wait.Done()
	route, err := h.resolver.Resolve(req)
	if err != nil {
		return nil, err
	}
	slog.Debug("proxy route resolved", "instance", h.name, "mode", h.cfg.Mode, "method", req.Method, "path", req.URL.Path, "object", route.ObjectPath, "upstream_path", route.UpstreamPath, "policy", route.Policy)
	if route.Policy == config.PolicyBypass {
		return h.bypass(ctx, req, route)
	}

	lock := h.locks.Get(route.ObjectPath)
	if !lock.TryLock() {
		return h.lockBusy(ctx, req, route)
	}
	defer lock.Unlock()
	if req.Header.Get("Range") != "" {
		cached, err := h.openValidCached(ctx, route)
		if err == nil {
			cached.Headers["X-Cache"] = "HIT"
			return h.rewriteResponse(req, route, cached), nil
		}
		return h.bypass(ctx, req, route)
	}

	cached, err := h.openCached(ctx, route)
	if err != nil {
		slog.Debug("cache miss", "instance", h.name, "object", route.ObjectPath, "err", err)
		return h.downloadAndOpen(ctx, req, route, "MISS")
	}
	if route.Policy == config.PolicyImmutable {
		cached.Headers["X-Cache"] = "HIT"
		return h.rewriteResponse(req, route, cached), nil
	}
	if h.fresh(route, cached.Headers) {
		cached.Headers["X-Cache"] = "FRESH"
		return h.rewriteResponse(req, route, cached), nil
	}
	valid, err := h.validateCached(ctx, route, cached.Headers)
	if err != nil {
		_ = cached.Close()
		return errorResponse(http.StatusBadGateway, err), nil
	}
	if valid {
		cached.Headers["X-Cache"] = "HIT"
		return h.rewriteResponse(req, route, cached), nil
	}
	_ = cached.Close()
	slog.Debug("cache stale", "instance", h.name, "object", route.ObjectPath)
	return h.downloadAndOpen(ctx, req, route, "REFRESH")
}

func (h *Handler) lockBusy(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	if h.cfg.Cache.BusyPolicy == config.BusyPolicyStale && req.Header.Get("Range") == "" {
		cached, err := h.openCached(ctx, route)
		if err == nil {
			cached.Headers["X-Cache"] = "STALE"
			if route.Policy == config.PolicyImmutable {
				cached.Headers["X-Cache"] = "HIT"
			}
			if h.fresh(route, cached.Headers) {
				cached.Headers["X-Cache"] = "FRESH"
			}
			return h.rewriteResponse(req, route, cached), nil
		}
	}
	slog.Debug("cache lock busy, bypass upstream", "instance", h.name, "object", route.ObjectPath)
	return h.bypass(ctx, req, route)
}

func (h *Handler) bypass(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	response, err := h.openRemote(ctx, req.Method, route.UpstreamPath, remoteOptions{AcceptErrors: true, Record: true}, h.requestHeaders(req))
	if response != nil {
		response.Headers["X-Cache"] = "BYPASS"
		response = h.rewriteResponse(req, route, response)
	}
	return response, err
}

func (h *Handler) openCached(ctx context.Context, route Route) (*utils.ResponseWrapper, error) {
	reader, err := h.store.OpenObject(ctx, h.name, route.ObjectPath)
	if err != nil {
		return nil, err
	}
	info := reader.Info()
	headers := map[string]string{"Content-Length": strconv.FormatInt(info.Size, 10)}
	for key, value := range info.Options {
		headers[headerName(key)] = value
	}
	if h.expired(route, info.Options) {
		_ = reader.Close()
		_ = h.store.DeleteObject(ctx, h.name, route.ObjectPath)
		return nil, errors.New("cached object expired")
	}
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = mime.TypeByExtension(path.Ext(route.ObjectPath))
	}
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = "application/octet-stream"
	}
	return &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}, nil
}

func (h *Handler) openValidCached(ctx context.Context, route Route) (*utils.ResponseWrapper, error) {
	cached, err := h.openCached(ctx, route)
	if err != nil {
		return nil, err
	}
	if route.Policy == config.PolicyImmutable {
		return cached, nil
	}
	if h.fresh(route, cached.Headers) {
		return cached, nil
	}
	valid, err := h.validateCached(ctx, route, cached.Headers)
	if err != nil || !valid {
		_ = cached.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("cached object is invalid")
	}
	return cached, nil
}

func (h *Handler) validateCached(ctx context.Context, route Route, cached map[string]string) (bool, error) {
	headers := map[string]string{}
	if etag := cached["ETag"]; etag != "" {
		headers["If-None-Match"] = etag
	}
	if lastModified := cached["Last-Modified"]; lastModified != "" {
		headers["If-Modified-Since"] = lastModified
	}
	resp, err := h.openRemote(ctx, http.MethodHead, route.UpstreamPath, remoteOptions{AcceptErrors: true}, headers)
	if err != nil {
		return false, err
	}
	defer resp.Close()
	switch resp.StatusCode {
	case http.StatusNotModified:
		return true, nil
	case http.StatusOK:
		if cached["ETag"] != "" && resp.Headers["ETag"] == cached["ETag"] {
			return true, nil
		}
		return cached["ETag"] == "" && resp.Headers["Last-Modified"] == cached["Last-Modified"] && resp.Headers["Content-Length"] == cached["Content-Length"], nil
	case http.StatusNotFound, http.StatusGone:
		_ = h.store.DeleteObject(ctx, h.name, route.ObjectPath)
		return false, nil
	default:
		if resp.StatusCode >= 500 {
			return false, fmt.Errorf("upstream HEAD failed with %d", resp.StatusCode)
		}
		return false, nil
	}
}

func (h *Handler) downloadAndOpen(ctx context.Context, req *http.Request, route Route, status string) (*utils.ResponseWrapper, error) {
	h.stats.AddActiveDownload(h.name, h.cfg.Mode, 1)
	defer h.stats.AddActiveDownload(h.name, h.cfg.Mode, -1)
	resp, err := h.openRemote(ctx, http.MethodGet, route.UpstreamPath, remoteOptions{AcceptErrors: true, Record: true}, h.requestHeaders(req))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Headers["X-Cache"] = "BYPASS"
		return h.rewriteResponse(req, route, resp), nil
	}
	defer resp.Close()
	tempFile, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tempFile.Name())
	if _, err = io.Copy(tempFile, resp.Body); err != nil {
		_ = tempFile.Close()
		return nil, err
	}
	if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
		_ = tempFile.Close()
		return nil, err
	}
	if parent := path.Dir(route.ObjectPath); parent != "." {
		if err = h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			_ = tempFile.Close()
			return nil, err
		}
	}
	meta := metadata(resp.Headers, h.cfg.Mode, status)
	if _, err = h.store.Put(ctx, h.name, route.ObjectPath, tempFile, meta); err != nil {
		_ = tempFile.Close()
		return nil, err
	}
	if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
		_ = tempFile.Close()
		return nil, err
	}
	stat, err := tempFile.Stat()
	if err != nil {
		_ = tempFile.Close()
		return nil, err
	}
	headers := map[string]string{"Content-Length": strconv.FormatInt(stat.Size(), 10), "X-Cache": status}
	for key, value := range meta {
		headers[headerName(key)] = value
	}
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = mime.TypeByExtension(path.Ext(route.ObjectPath))
	}
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = "application/octet-stream"
	}
	return h.rewriteResponse(req, route, &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: tempFile}), nil
}

func (h *Handler) rewriteResponse(req *http.Request, route Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
	if !route.RewriteNPMMetadata || req.Method == http.MethodHead || response.Body == nil {
		return response
	}
	const maxRewriteBody = 50 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRewriteBody+1))
	_ = response.Body.Close()
	if err != nil {
		return errorResponse(http.StatusBadGateway, err)
	}
	if len(body) > maxRewriteBody {
		return errorResponse(http.StatusBadGateway, errors.New("npm metadata too large to rewrite"))
	}
	var document any
	if err := json.Unmarshal(body, &document); err != nil {
		response.Body = io.NopCloser(bytes.NewReader(body))
		return response
	}
	if rewriteNPMTarballs(document, h.cfg.Upstreams, publicBaseURL(req)) {
		body, err = json.Marshal(document)
		if err != nil {
			return errorResponse(http.StatusBadGateway, err)
		}
		response.Headers["Content-Length"] = strconv.Itoa(len(body))
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response
}

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
	for _, baseURL := range h.cfg.Upstreams {
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
				h.stats.RecordUpstream(h.name, h.cfg.Mode, method, 0)
			}
			lastErr = err
			slog.Debug("upstream request failed", "instance", h.name, "method", method, "url", redactedURL(targetURL), "err", err)
			continue
		}
		slog.Debug("upstream response received", "instance", h.name, "method", method, "url", redactedURL(targetURL), "status", response.StatusCode)
		if h.cfg.Mode == config.ModeOCI && response.StatusCode == http.StatusUnauthorized {
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
			h.stats.RecordUpstream(h.name, h.cfg.Mode, method, response.StatusCode)
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

func (h *Handler) retryOCIChallenge(ctx context.Context, method, targetURL string, headers map[string]string, response *http.Response) (*http.Response, error) {
	challenge, ok := parseOCIChallenge(response.Header.Get("WWW-Authenticate"))
	if !ok {
		return nil, nil
	}
	_ = response.Body.Close()
	var auth string
	switch strings.ToLower(challenge.scheme) {
	case "bearer":
		token, err := h.ociBearerToken(ctx, challenge)
		if err != nil {
			return nil, err
		}
		auth = "Bearer " + token
	case "basic":
		if basic := h.ociBasicAuthorization(); basic != "" {
			auth = basic
		}
	default:
		return nil, nil
	}
	if auth == "" {
		return nil, nil
	}
	request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("Authorization", auth)
	return h.client.Do(request)
}

func (h *Handler) ociBearerToken(ctx context.Context, challenge ociChallenge) (string, error) {
	key := challenge.realm + "\x00" + challenge.params["service"] + "\x00" + challenge.params["scope"]
	now := time.Now()
	h.ociTokenMu.Lock()
	h.cleanupExpiredOCITokens(now)
	if cached := h.ociTokens[key]; cached.value != "" && now.Before(cached.expire) {
		h.ociTokenMu.Unlock()
		return cached.value, nil
	}
	h.ociTokenMu.Unlock()

	value, err, _ := h.ociGroup.Do(key, func() (any, error) {
		now := time.Now()
		h.ociTokenMu.Lock()
		h.cleanupExpiredOCITokens(now)
		if cached := h.ociTokens[key]; cached.value != "" && now.Before(cached.expire) {
			h.ociTokenMu.Unlock()
			return cached.value, nil
		}
		h.ociTokenMu.Unlock()

		token, expire, err := h.fetchOCIBearerToken(ctx, challenge, now)
		if err != nil {
			return "", err
		}
		h.ociTokenMu.Lock()
		h.ociTokens[key] = ociToken{value: token, expire: expire}
		h.ociTokenMu.Unlock()
		return token, nil
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (h *Handler) cleanupExpiredOCITokens(now time.Time) {
	for key, token := range h.ociTokens {
		if token.value == "" || !now.Before(token.expire) {
			delete(h.ociTokens, key)
		}
	}
}

func (h *Handler) fetchOCIBearerToken(ctx context.Context, challenge ociChallenge, now time.Time) (string, time.Time, error) {
	tokenURL, err := url.Parse(challenge.realm)
	if err != nil || tokenURL.Scheme == "" || tokenURL.Host == "" {
		return "", time.Time{}, fmt.Errorf("invalid OCI token realm %q", challenge.realm)
	}
	query := tokenURL.Query()
	for _, name := range []string{"service", "scope"} {
		if value := challenge.params[name]; value != "" {
			query.Set(name, value)
		}
	}
	tokenURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return "", time.Time{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	if basic := h.ociBasicAuthorization(); basic != "" {
		request.Header.Set("Authorization", basic)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return "", time.Time{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("OCI token request failed with %d", response.StatusCode)
	}
	var payload struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		IssuedAt    string `json:"issued_at"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", time.Time{}, err
	}
	token := payload.Token
	if token == "" {
		token = payload.AccessToken
	}
	if token == "" {
		return "", time.Time{}, errors.New("OCI token response is empty")
	}
	issuedAt := now
	if payload.IssuedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, payload.IssuedAt); err == nil {
			issuedAt = parsed
		}
	}
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	expire := issuedAt.Add(ttl)
	if ttl > time.Minute {
		expire = expire.Add(-30 * time.Second)
	}
	return token, expire, nil
}

func redactedURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	parsed.User = nil
	return parsed.String()
}

func parseOCIChallenge(header string) (ociChallenge, bool) {
	header = strings.TrimSpace(header)
	scheme, rest, ok := strings.Cut(header, " ")
	if !ok || scheme == "" {
		return ociChallenge{}, false
	}
	challenge := ociChallenge{scheme: scheme, params: map[string]string{}}
	for _, part := range splitChallengeParams(rest) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.Trim(strings.TrimSpace(value), `"`)
		value = strings.ReplaceAll(value, `\"`, `"`)
		if key == "realm" {
			challenge.realm = value
		}
		challenge.params[key] = value
	}
	if strings.EqualFold(challenge.scheme, "bearer") && challenge.realm == "" {
		return ociChallenge{}, false
	}
	return challenge, true
}

func splitChallengeParams(value string) []string {
	parts := []string{}
	start := 0
	quoted := false
	escaped := false
	for index, char := range value {
		if escaped {
			escaped = false
			continue
		}
		if char == '\\' {
			escaped = true
			continue
		}
		if char == '"' {
			quoted = !quoted
			continue
		}
		if char == ',' && !quoted {
			if part := strings.TrimSpace(value[start:index]); part != "" {
				parts = append(parts, part)
			}
			start = index + 1
		}
	}
	if part := strings.TrimSpace(value[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func (h *Handler) requestHeaders(req *http.Request) map[string]string {
	headers := map[string]string{}
	if value := req.Header.Get("Range"); value != "" {
		headers["Range"] = value
	}
	if h.cfg.Mode == config.ModeOCI {
		headers["Accept"] = ociManifestAccept
	}
	if h.cfg.Mode != config.ModeFile {
		return headers
	}
	for _, name := range h.cfg.PassHeaders {
		if !passableHeader(name) {
			continue
		}
		if value := req.Header.Get(name); value != "" {
			headers[http.CanonicalHeaderKey(name)] = value
		}
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

func (h *Handler) expired(route Route, options map[string]string) bool {
	expireAfter := route.ExpireAfter
	if expireAfter <= 0 {
		expireAfter = h.cfg.ExpireAfter
	}
	if expireAfter <= 0 {
		return false
	}
	fetchedAt, err := utils.ParseFetchedAt(options["fetched-at"])
	return err == nil && time.Since(fetchedAt) > expireAfter.Duration()
}

func (h *Handler) fresh(route Route, headers map[string]string) bool {
	freshFor := route.FreshFor
	if freshFor <= 0 {
		freshFor = h.cfg.Cache.FreshFor
	}
	if freshFor <= 0 {
		return false
	}
	fetchedAt, err := utils.ParseFetchedAt(headers["fetched-at"])
	return err == nil && time.Since(fetchedAt) <= freshFor.Duration()
}

func (h *Handler) staticAuthorization() string {
	if h.cfg.Mode == config.ModeOCI {
		if bearer := h.ociBearerAuthorization(); bearer != "" {
			return bearer
		}
		return ""
	}
	return ""
}

func (h *Handler) ociBearerAuthorization() string {
	if h.cfg.Mode != config.ModeOCI || h.cfg.OCI == nil || h.cfg.OCI.Auth == nil {
		return ""
	}
	auth := h.cfg.OCI.Auth
	switch strings.ToLower(auth.Type) {
	case "bearer":
		if auth.Token == "" {
			return ""
		}
		return "Bearer " + auth.Token
	default:
		return ""
	}
}

func (h *Handler) ociBasicAuthorization() string {
	if h.cfg.Mode != config.ModeOCI || h.cfg.OCI == nil || h.cfg.OCI.Auth == nil || strings.ToLower(h.cfg.OCI.Auth.Type) != "basic" {
		return ""
	}
	auth := h.cfg.OCI.Auth
	if auth.Username == "" && auth.Password == "" {
		return ""
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth.Username+":"+auth.Password))
}

func errorResponse(status int, err error) *utils.ResponseWrapper {
	return &utils.ResponseWrapper{StatusCode: status, Headers: map[string]string{"Content-Type": "text/plain; charset=utf-8", "X-Cache": "ERROR"}, Body: io.NopCloser(strings.NewReader(err.Error()))}
}

func responseFromHTTP(response *http.Response) *utils.ResponseWrapper {
	return &utils.ResponseWrapper{StatusCode: response.StatusCode, Headers: copyHeaders(response.Header), Body: response.Body}
}

func copyHeaders(headers http.Header) map[string]string {
	result := map[string]string{}
	for _, key := range []string{"Content-Type", "Content-Length", "Last-Modified", "Content-Range", "Accept-Ranges", "ETag", "Docker-Content-Digest", "Docker-Distribution-API-Version"} {
		if value := headers.Get(key); value != "" {
			result[key] = value
		}
	}
	return result
}

func responseBytes(headers map[string]string) uint64 {
	value := headers["Content-Length"]
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func metadata(headers map[string]string, mode, status string) map[string]string {
	result := map[string]string{"mode": mode, "cache": status, "fetched-at": time.Now().UTC().Format(time.RFC3339Nano)}
	for _, key := range []string{"Content-Type", "Content-Length", "Last-Modified", "ETag", "Docker-Content-Digest"} {
		if value := headers[key]; value != "" {
			result[strings.ToLower(key)] = value
		}
	}
	return result
}

func headerName(key string) string {
	switch key {
	case "content-type":
		return "Content-Type"
	case "content-length":
		return "Content-Length"
	case "last-modified":
		return "Last-Modified"
	case "etag":
		return "ETag"
	case "docker-content-digest":
		return "Docker-Content-Digest"
	default:
		return key
	}
}

func SafePath(value string) bool {
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func EscapePath(value string) string {
	parts := strings.Split(value, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func HashKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
