package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const manifestAccept = "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.oci.artifact.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v1+json, application/json"

type handler struct {
	name        string
	upstream    string
	expireAfter config.Expiration
	policy      *Policy
	store       *blobfs.Store
	stats       *httpcache.Stats
	client      *utils.HttpClientWrapper
	wait        sync.WaitGroup

	tokenMu sync.Mutex
	tokens  map[string]ociToken
	group   singleflight.Group
}

type refState struct {
	Repo           string            `yaml:"repo"`
	Ref            string            `yaml:"ref"`
	FetchedAt      time.Time         `yaml:"fetched_at"`
	ExpireAfter    config.Expiration `yaml:"expire_after"`
	ManifestDigest string            `yaml:"manifest_digest,omitempty"`
	BlobDigests    []string          `yaml:"blob_digests,omitempty"`
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

type descriptor struct {
	Digest string `json:"digest"`
}

func newHandler(name string, block Block, expireAfter config.Expiration, store *blobfs.Store, stats *httpcache.Stats) *handler {
	client := utils.DefaultHttpClientWrapper()
	if block.Transport != nil {
		if block.Transport.UserAgent != "" {
			client.UserAgent = block.Transport.UserAgent
		}
		if transport, ok := client.Transport.(*http.Transport); ok {
			if block.Transport.Proxy != "" {
				if proxyURL, err := url.Parse(block.Transport.Proxy); err == nil {
					transport.Proxy = http.ProxyURL(proxyURL)
				}
			}
			if block.Transport.Timeout > 0 {
				transport.DialContext = utils.DefaultDialContext(block.Transport.Timeout.Duration())
			}
		}
	}
	return &handler{
		name:        name,
		upstream:    strings.TrimRight(block.Upstream, "/"),
		expireAfter: expireAfter,
		policy:      &block.Policy,
		store:       store,
		stats:       stats,
		client:      client,
		tokens:      map[string]ociToken{},
	}
}

func (h *handler) Start(context.Context) error { return nil }

func (h *handler) Stop(context.Context) error {
	h.wait.Wait()
	return nil
}

func (h *handler) Cleanup(ctx context.Context) error {
	return fs.WalkDir(h.store.TenantFS(h.name), "oci/refs", func(current string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || entry.IsDir() || path.Base(current) != "state.yaml" {
			return nil
		}
		state, readErr := h.readState(ctx, current)
		if readErr != nil || h.stateExpired(state) {
			if removeErr := h.deleteTree(ctx, path.Dir(current)); removeErr != nil && !errors.Is(removeErr, context.Canceled) {
				slog.Warn("oci cleanup delete failed", "instance", h.name, "prefix", path.Dir(current), "err", removeErr)
			}
		}
		return nil
	})
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	h.wait.Add(1)
	defer h.wait.Done()

	resolved, err := resolveRequest(req, h.policy)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	status, cache, bytes, err := h.serve(req.Context(), w, req, resolved)
	if err != nil {
		slog.Warn("oci proxy failed", "instance", h.name, "method", req.Method, "path", req.URL.Path, "err", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", http.StatusBadGateway, 0)
		return
	}
	h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, cache, status, bytes)
}

func (h *handler) serve(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	if resolved.match.policy == config.PolicyBypass || resolved.kind == requestPing || resolved.kind == requestTags || resolved.kind == requestBypass {
		return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
	switch resolved.kind {
	case requestManifest:
		return h.serveManifest(ctx, w, req, resolved)
	case requestBlob:
		return h.serveBlob(ctx, w, req, resolved)
	default:
		return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
}

func (h *handler) serveManifest(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	statePath := h.refStatePath(resolved.repo, resolved.ref)
	state, err := h.readState(ctx, statePath)
	if err == nil && !h.stateExpired(state) {
		if status, bytes, cacheErr := h.serveCachedObject(ctx, w, req, h.refManifestPath(resolved.repo, resolved.ref), "HIT"); cacheErr == nil {
			return status, "HIT", bytes, nil
		}
	}

	staleState := state
	status, bytes, fetchErr := h.fetchManifest(ctx, w, req, resolved)
	if fetchErr == nil {
		return status, "MISS", bytes, nil
	}
	if resolved.match.busyPolicy == config.BusyPolicyStale && staleState.Repo != "" {
		if staleStatus, staleBytes, cacheErr := h.serveCachedObject(ctx, w, req, h.refManifestPath(resolved.repo, resolved.ref), "STALE"); cacheErr == nil {
			return staleStatus, "STALE", staleBytes, nil
		}
	}
	return 0, "", 0, fetchErr
}

func (h *handler) serveBlob(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	state, err := h.findBlobState(ctx, resolved.repo, resolved.digest)
	if err == nil {
		if status, bytes, cacheErr := h.serveCachedObject(ctx, w, req, h.refBlobPath(state.Repo, state.Ref, resolved.digest), "HIT"); cacheErr == nil {
			return status, "HIT", bytes, nil
		}
	}
	if err != nil {
		return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
	return h.fetchBlob(ctx, w, req, resolved, state)
}

func (h *handler) fetchManifest(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, uint64, error) {
	h.stats.AddActiveDownload(h.name, config.ModeOCI, 1)
	defer h.stats.AddActiveDownload(h.name, config.ModeOCI, -1)

	response, err := h.remoteRequest(ctx, http.MethodGet, resolved.upstreamPath, map[string]string{"Accept": manifestAccept})
	if err != nil {
		return 0, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return h.copyRemote(w, req, response, "BYPASS")
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, 0, err
	}
	manifestDigest := response.Header.Get("Docker-Content-Digest")
	if manifestDigest == "" {
		sum := sha256.Sum256(body)
		manifestDigest = "sha256:" + hex.EncodeToString(sum[:])
	}
	state := refState{
		Repo:           resolved.repo,
		Ref:            resolved.ref,
		FetchedAt:      time.Now().UTC(),
		ExpireAfter:    effectiveExpire(resolved.match.expireAfter, h.expireAfter),
		ManifestDigest: manifestDigest,
		BlobDigests:    collectBlobDigests(body),
	}
	if err := h.putObject(ctx, h.refManifestPath(resolved.repo, resolved.ref), body, response.Header, map[string]string{"docker-content-digest": manifestDigest}); err != nil {
		return 0, 0, err
	}
	if err := h.writeState(ctx, state); err != nil {
		return 0, 0, err
	}
	return h.writeResponse(w, req.Method, http.StatusOK, manifestHeaders(response.Header, body, manifestDigest), bytes.NewReader(body))
}

func (h *handler) fetchBlob(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request, state refState) (int, string, uint64, error) {
	h.stats.AddActiveDownload(h.name, config.ModeOCI, 1)
	defer h.stats.AddActiveDownload(h.name, config.ModeOCI, -1)

	response, err := h.remoteRequest(ctx, http.MethodGet, resolved.upstreamPath, nil)
	if err != nil {
		return 0, "", 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, "", 0, err
	}
	if err := h.putObject(ctx, h.refBlobPath(state.Repo, state.Ref, resolved.digest), body, response.Header, nil); err != nil {
		return 0, "", 0, err
	}
	status, bytes, err := h.writeResponse(w, req.Method, http.StatusOK, objectHeaders(response.Header, len(body), "MISS"), bytes.NewReader(body))
	return status, "MISS", bytes, err
}

func (h *handler) serveCachedObject(ctx context.Context, w http.ResponseWriter, req *http.Request, objectPath, cache string) (int, uint64, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return 0, 0, err
	}
	defer reader.Close()
	info := reader.Info()
	headers := map[string]string{
		"Content-Length": info.Options["content-length"],
		"Content-Type":   info.Options["content-type"],
		"ETag":           info.Options["etag"],
		"Last-Modified":  info.Options["last-modified"],
		"X-Cache":        cache,
	}
	if digest := info.Options["docker-content-digest"]; digest != "" {
		headers["Docker-Content-Digest"] = digest
	}
	if headers["Content-Length"] == "" {
		headers["Content-Length"] = strconv.FormatInt(info.Size, 10)
	}
	return h.writeResponse(w, req.Method, http.StatusOK, headers, reader)
}

func (h *handler) serveRemote(ctx context.Context, w http.ResponseWriter, req *http.Request, upstreamPath, cache string, headers map[string]string) (int, string, uint64, error) {
	response, err := h.remoteRequest(ctx, req.Method, upstreamPath, headers)
	if err != nil {
		return 0, "", 0, err
	}
	defer response.Body.Close()
	status, bytes, err := h.copyRemote(w, req, response, cache)
	return status, cache, bytes, err
}

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
	return response, nil
}

func (h *handler) retryChallenge(ctx context.Context, method, targetURL string, headers map[string]string, response *http.Response) (*http.Response, error) {
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
		auth = h.basicAuthorization()
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

func (h *handler) staticAuthorization() string {
	if h.policy.Auth == nil {
		return ""
	}
	switch strings.ToLower(h.policy.Auth.Type) {
	case "bearer":
		if h.policy.Auth.Token != "" {
			return "Bearer " + h.policy.Auth.Token
		}
	}
	return ""
}

func (h *handler) basicAuthorization() string {
	if h.policy.Auth == nil || strings.ToLower(h.policy.Auth.Type) != "basic" {
		return ""
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(h.policy.Auth.Username+":"+h.policy.Auth.Password))
}

func (h *handler) ociBearerToken(ctx context.Context, challenge ociChallenge) (string, error) {
	key := challenge.realm + "\x00" + challenge.params["service"] + "\x00" + challenge.params["scope"]
	now := time.Now()
	h.tokenMu.Lock()
	for itemKey, token := range h.tokens {
		if token.value == "" || !now.Before(token.expire) {
			delete(h.tokens, itemKey)
		}
	}
	if token := h.tokens[key]; token.value != "" && now.Before(token.expire) {
		h.tokenMu.Unlock()
		return token.value, nil
	}
	h.tokenMu.Unlock()

	value, err, _ := h.group.Do(key, func() (any, error) {
		token, expire, err := h.fetchBearerToken(ctx, challenge, time.Now())
		if err != nil {
			return "", err
		}
		h.tokenMu.Lock()
		h.tokens[key] = ociToken{value: token, expire: expire}
		h.tokenMu.Unlock()
		return token, nil
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (h *handler) fetchBearerToken(ctx context.Context, challenge ociChallenge, now time.Time) (string, time.Time, error) {
	tokenURL, err := url.Parse(challenge.realm)
	if err != nil || tokenURL.Scheme == "" || tokenURL.Host == "" {
		return "", time.Time{}, errors.New("invalid OCI token realm")
	}
	query := tokenURL.Query()
	for _, key := range []string{"service", "scope"} {
		if value := challenge.params[key]; value != "" {
			query.Set(key, value)
		}
	}
	tokenURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return "", time.Time{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	if basic := h.basicAuthorization(); basic != "" {
		request.Header.Set("Authorization", basic)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return "", time.Time{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", time.Time{}, errors.New("OCI token request failed")
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
	if payload.IssuedAt != "" {
		if issuedAt, err := time.Parse(time.RFC3339, payload.IssuedAt); err == nil {
			now = issuedAt
		}
	}
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	expire := now.Add(ttl)
	if ttl > time.Minute {
		expire = expire.Add(-30 * time.Second)
	}
	return token, expire, nil
}

func (h *handler) readState(ctx context.Context, objectPath string) (refState, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return refState{}, err
	}
	defer reader.Close()
	var state refState
	if err := yaml.NewDecoder(reader).Decode(&state); err != nil {
		return refState{}, err
	}
	return state, nil
}

func (h *handler) writeState(ctx context.Context, state refState) error {
	data, err := yaml.Marshal(&state)
	if err != nil {
		return err
	}
	return h.putRaw(ctx, h.refStatePath(state.Repo, state.Ref), data, map[string]string{"content-type": "application/yaml"})
}

func (h *handler) findBlobState(ctx context.Context, repo, digest string) (refState, error) {
	base := path.Join("oci/refs", repo)
	var matched refState
	err := fs.WalkDir(h.store.TenantFS(h.name), base, func(current string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || path.Base(current) != "state.yaml" {
			return nil
		}
		state, readErr := h.readState(ctx, current)
		if readErr != nil || h.stateExpired(state) {
			return nil
		}
		for _, item := range state.BlobDigests {
			if item == digest {
				matched = state
				return fs.SkipAll
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.SkipAll) {
		return refState{}, err
	}
	if matched.Repo == "" {
		return refState{}, fs.ErrNotExist
	}
	return matched, nil
}

func (h *handler) putObject(ctx context.Context, objectPath string, body []byte, headers http.Header, extra map[string]string) error {
	meta := map[string]string{
		"content-type":   headers.Get("Content-Type"),
		"content-length": strconv.Itoa(len(body)),
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
	}
	for _, key := range []string{"ETag", "Last-Modified", "Docker-Content-Digest"} {
		if value := headers.Get(key); value != "" {
			meta[strings.ToLower(key)] = value
		}
	}
	for key, value := range extra {
		if value != "" {
			meta[key] = value
		}
	}
	return h.putRaw(ctx, objectPath, body, meta)
}

func (h *handler) putRaw(ctx context.Context, objectPath string, body []byte, meta map[string]string) error {
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			return err
		}
	}
	_, err := h.store.Put(ctx, h.name, objectPath, bytes.NewReader(body), meta)
	return err
}

func (h *handler) stateExpired(state refState) bool {
	expireAfter := effectiveExpire(state.ExpireAfter, h.expireAfter)
	return !expireAfter.IsNever() && !expireAfter.IsUnset() && time.Now().After(state.FetchedAt.Add(expireAfter.Duration()))
}

func (h *handler) deleteTree(ctx context.Context, prefix string) error {
	var objects []string
	if err := fs.WalkDir(h.store.TenantFS(h.name), prefix, func(current string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		objects = append(objects, current)
		return nil
	}); err != nil {
		return err
	}
	for _, objectPath := range objects {
		if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func (h *handler) refStatePath(repo, ref string) string {
	return path.Join(h.refDir(repo, ref), "state.yaml")
}

func (h *handler) refManifestPath(repo, ref string) string {
	return path.Join(h.refDir(repo, ref), "manifest")
}

func (h *handler) refBlobPath(repo, ref, digest string) string {
	return path.Join(h.refDir(repo, ref), "blobs", strings.ReplaceAll(digest, ":", "/"))
}

func (h *handler) refDir(repo, ref string) string {
	return path.Join("oci/refs", repo, httpcache.HashKey(ref))
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
		return status, responseBytes(headers), nil
	}
	written, err := io.Copy(w, body)
	return status, uint64(written), err
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
	var parts []string
	start := 0
	quoted := false
	escaped := false
	for index, char := range value {
		if escaped {
			escaped = false
			continue
		}
		switch char {
		case '\\':
			escaped = true
		case '"':
			quoted = !quoted
		case ',':
			if !quoted {
				if part := strings.TrimSpace(value[start:index]); part != "" {
					parts = append(parts, part)
				}
				start = index + 1
			}
		}
	}
	if part := strings.TrimSpace(value[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func collectBlobDigests(body []byte) []string {
	var doc struct {
		Config descriptor   `json:"config"`
		Layers []descriptor `json:"layers"`
		Blobs  []descriptor `json:"blobs"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	var digests []string
	for _, item := range append(append([]descriptor{doc.Config}, doc.Layers...), doc.Blobs...) {
		if item.Digest == "" {
			continue
		}
		if _, ok := seen[item.Digest]; ok {
			continue
		}
		seen[item.Digest] = struct{}{}
		digests = append(digests, item.Digest)
	}
	return digests
}

func effectiveExpire(current, fallback config.Expiration) config.Expiration {
	if current.IsUnset() {
		return fallback
	}
	return current
}

func objectHeaders(headers http.Header, length int, cache string) map[string]string {
	result := map[string]string{
		"Content-Type":   headers.Get("Content-Type"),
		"Content-Length": headers.Get("Content-Length"),
		"ETag":           headers.Get("ETag"),
		"Last-Modified":  headers.Get("Last-Modified"),
		"X-Cache":        cache,
	}
	if length >= 0 && result["Content-Length"] == "" {
		result["Content-Length"] = strconv.Itoa(length)
	}
	if digest := headers.Get("Docker-Content-Digest"); digest != "" {
		result["Docker-Content-Digest"] = digest
	}
	return result
}

func manifestHeaders(headers http.Header, body []byte, digest string) map[string]string {
	result := objectHeaders(headers, len(body), "MISS")
	if digest != "" {
		result["Docker-Content-Digest"] = digest
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
