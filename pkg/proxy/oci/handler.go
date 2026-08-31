package oci

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxTokenCacheEntries = 2048

type tokenCacheEntry struct {
	key    string
	expire time.Time
}

func newHandler(name string, block Block, expireAfter config.Expiration, store *blobfs.Store, stats *httpcache.Stats, upstreamGate *httpcache.UpstreamGate) *handler {
	client := utils.DefaultHTTPClientWrapper()
	httpcache.ConfigureClientTransport(client, name, block.Transport)
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	return &handler{
		name:            name,
		upstream:        strings.TrimRight(block.Upstream, "/"),
		expireAfter:     expireAfter,
		policy:          &block.Policy,
		store:           store,
		stats:           stats,
		client:          client,
		upstreamGate:    upstreamGate,
		lifecycleCtx:    lifecycleCtx,
		cancel:          cancel,
		auth:            authHandler{tokens: map[string]ociToken{}},
		refLocks:        utils.NewRWLockGroup(),
		manifestReaders: map[string]int{},
	}
}

func (h *handler) Start(ctx context.Context) error {
	h.cancel()
	h.lifecycleCtx, h.cancel = context.WithCancel(ctx)
	return nil
}

func (h *handler) trimTokenCacheLocked(now time.Time, keepKey string) {
	for key, token := range h.auth.tokens {
		if token.value == "" || !now.Before(token.expire) {
			delete(h.auth.tokens, key)
		}
	}
	if len(h.auth.tokens) <= maxTokenCacheEntries {
		return
	}

	entries := make([]tokenCacheEntry, 0, len(h.auth.tokens))
	for key, token := range h.auth.tokens {
		if key == keepKey {
			continue
		}
		entries = append(entries, tokenCacheEntry{key: key, expire: token.expire})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].expire.Equal(entries[j].expire) {
			return entries[i].key < entries[j].key
		}
		return entries[i].expire.Before(entries[j].expire)
	})
	for _, entry := range entries {
		if len(h.auth.tokens) <= maxTokenCacheEntries {
			return
		}
		delete(h.auth.tokens, entry.key)
	}
}

func (h *handler) Stop(ctx context.Context) error {
	h.closeMu.Lock()
	h.closing = true
	h.cancel()
	h.closeMu.Unlock()
	return utils.WaitGroupContext(ctx, &h.wait)
}

func (h *handler) beginOperation() bool {
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closing {
		return false
	}
	h.wait.Add(1)
	return true
}

func (h *handler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	deleted := 0
	referencedManifests := make(map[string]struct{})
	refsBusy := false
	err := fs.WalkDir(h.store.TenantFS(h.name), "oci/refs", func(current string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		baseName := path.Base(current)
		if strings.HasPrefix(baseName, "state.yaml.tmp.") {
			statePath := path.Join(path.Dir(current), "state.yaml")
			lock := h.refLocks.Get(statePath)
			if !lock.TryLock() {
				refsBusy = true
				return nil
			}
			defer lock.Unlock()
			if opts.DryRun {
				deleted++
				slog.Info("oci cleanup dry-run delete abandoned state", "instance", h.name, "path", current)
				return nil
			}
			if deleteErr := h.store.DeleteObject(ctx, h.name, current); deleteErr != nil {
				return fmt.Errorf("delete abandoned OCI ref state %s: %w", current, deleteErr)
			}
			deleted++
			return nil
		}
		if baseName != "state.yaml" {
			return nil
		}
		lock := h.refLocks.Get(current)
		if !lock.TryLock() {
			refsBusy = true
			return nil
		}
		state, readErr := h.readState(ctx, current)
		expired := readErr != nil || h.stateExpired(state)
		if !expired {
			referencedManifests[h.manifestPath(state.ManifestDigest)] = struct{}{}
			lock.Unlock()
			return nil
		}
		if opts.DryRun {
			if readErr == nil {
				referencedManifests[h.manifestPath(state.ManifestDigest)] = struct{}{}
			}
			deleted++
			slog.Info("oci cleanup dry-run delete", "instance", h.name, "prefix", path.Dir(current))
			lock.Unlock()
			return nil
		}
		if removeErr := h.deleteTree(ctx, path.Dir(current)); removeErr != nil {
			lock.Unlock()
			return fmt.Errorf("delete expired OCI ref %s: %w", path.Dir(current), removeErr)
		}
		deleted++
		lock.Unlock()
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	if err != nil || (opts.BatchSize > 0 && deleted >= opts.BatchSize) || refsBusy || h.expireAfter.IsNever() || h.expireAfter.IsUnset() {
		return err
	}
	err = fs.WalkDir(h.store.TenantFS(h.name), "oci/manifests", func(current string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if _, referenced := referencedManifests[current]; referenced {
			return nil
		}
		h.manifestMu.Lock()
		active := h.manifestReaders[current] > 0
		h.manifestMu.Unlock()
		if active {
			return nil
		}
		removed, cleanupErr := h.cleanupExpiredObject(ctx, current, "manifest", opts)
		if removed {
			deleted++
		}
		return cleanupErr
	})
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	if err != nil || (opts.BatchSize > 0 && deleted >= opts.BatchSize) {
		return err
	}
	err = fs.WalkDir(h.store.TenantFS(h.name), "oci/blobs", func(current string, entry fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if _, busy := h.downloads.Load(current); busy {
			return nil
		}
		removed, cleanupErr := h.cleanupExpiredObject(ctx, current, "blob", opts)
		if removed {
			deleted++
		}
		return cleanupErr
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (h *handler) cleanupExpiredObject(ctx context.Context, objectPath, objectKind string, opts config.CleanupConfig) (bool, error) {
	info, err := h.store.StatObject(ctx, h.name, objectPath)
	if err != nil {
		return false, err
	}
	fetchedAt, fetchedAtErr := utils.ParseFetchedAt(info.Options["fetched-at"])
	if fetchedAtErr == nil && time.Since(fetchedAt) <= h.expireAfter.Duration() {
		return false, nil
	}
	if opts.DryRun {
		slog.Info("oci cleanup dry-run delete", "instance", h.name, "kind", objectKind, "path", objectPath)
		return true, nil
	}
	if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil {
		return false, fmt.Errorf("delete expired oci %s %s: %w", objectKind, objectPath, err)
	}
	return true, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", http.StatusMethodNotAllowed, 0)
		return
	}
	if !h.beginOperation() {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer h.wait.Done()

	resolved, err := resolveRequest(req, h.policy)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	status, cache, bytes, err := h.serve(req.Context(), w, req, resolved)
	if err != nil {
		slog.Info("oci proxy failed", "instance", h.name, "method", req.Method, "path", req.URL.Path, "err", err)
		status := http.StatusBadGateway
		var limited *httpcache.UpstreamRateLimitError
		if errors.As(err, &limited) {
			status = http.StatusTooManyRequests
		}
		if retryAfter, admissionError := httpcache.AdmissionRetryAfterSeconds(err); admissionError {
			if !errors.As(err, &limited) {
				status = http.StatusServiceUnavailable
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		http.Error(w, http.StatusText(status), status)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", status, 0)
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
	if err == nil && h.manifestFresh(resolved, state) {
		if status, bytes, cacheErr := h.serveManifestState(ctx, w, req, state, "HIT"); cacheErr == nil {
			slog.Debug("oci manifest cache hit", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref)
			return status, "HIT", bytes, nil
		}
	}

	staleState := state
	lock := h.refLocks.Get(statePath)
	if !lock.TryLock() {
		if resolved.match.busyPolicy == config.BusyPolicyStale && staleState.Repo != "" {
			if status, bytes, cacheErr := h.serveManifestState(ctx, w, req, staleState, "STALE"); cacheErr == nil {
				return status, "STALE", bytes, nil
			}
		}
		if resolved.match.busyPolicy == config.BusyPolicyBypass {
			return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", map[string]string{"Accept": manifestAccept})
		}
		lock.Lock()
	}
	defer lock.Unlock()

	state, err = h.readState(ctx, statePath)
	if err == nil && h.manifestFresh(resolved, state) {
		if status, bytes, cacheErr := h.serveManifestState(ctx, w, req, state, "HIT"); cacheErr == nil {
			return status, "HIT", bytes, nil
		}
	}
	if state.Repo != "" {
		staleState = state
	}
	status, cache, bytes, fetchErr := h.fetchManifest(ctx, w, req, resolved)
	if fetchErr == nil {
		slog.Debug("oci manifest fetched", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref)
		return status, cache, bytes, nil
	}
	if resolved.match.busyPolicy == config.BusyPolicyStale && staleState.Repo != "" {
		slog.Debug("oci manifest fetch failed, serving stale", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "err", fetchErr)
		if staleStatus, staleBytes, cacheErr := h.serveManifestState(ctx, w, req, staleState, "STALE"); cacheErr == nil {
			return staleStatus, "STALE", staleBytes, nil
		}
	}
	return 0, "", 0, fetchErr
}

func (h *handler) serveManifestState(ctx context.Context, w http.ResponseWriter, req *http.Request, state refState, cache string) (int, uint64, error) {
	objectPath := h.manifestPath(state.ManifestDigest)
	h.manifestMu.Lock()
	h.manifestReaders[objectPath]++
	h.manifestMu.Unlock()
	defer func() {
		h.manifestMu.Lock()
		h.manifestReaders[objectPath]--
		if h.manifestReaders[objectPath] == 0 {
			delete(h.manifestReaders, objectPath)
		}
		h.manifestMu.Unlock()
	}()

	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = reader.Close() }()
	info := reader.Info()
	if info.Options["docker-content-digest"] != state.ManifestDigest ||
		!httpcache.CacheSupportsRequestUserAgent(h.client, req, info.Options) {
		return 0, 0, errors.New("cached OCI manifest does not match committed state")
	}
	headers := map[string]string{
		"Content-Length":        strconv.FormatInt(state.ContentLength, 10),
		"Content-Type":          state.ContentType,
		"ETag":                  state.ETag,
		"Last-Modified":         state.LastModified,
		"Vary":                  state.Vary,
		"X-Cache":               cache,
		"Docker-Content-Digest": state.ManifestDigest,
	}
	return h.writeResponse(w, req.Method, http.StatusOK, headers, reader)
}

func (h *handler) manifestFresh(resolved request, state refState) bool {
	if h.stateExpired(state) {
		return false
	}
	if resolved.match.policy == config.PolicyImmutable || isSHA256Digest(resolved.ref) {
		return true
	}
	if h.policy.FreshFor.IsForever() {
		return true
	}
	return !h.policy.FreshFor.IsUnset() && time.Since(state.FetchedAt) <= h.policy.FreshFor.Duration()
}

func (h *handler) serveBlob(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	objectPath := h.blobPath(resolved.digest)
	if status, bytes, cacheErr := h.serveCachedObject(ctx, w, req, objectPath, "HIT"); cacheErr == nil {
		slog.Debug("oci blob cache hit", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
		return status, "HIT", bytes, nil
	}
	ready := make(chan struct{})
	if active, downloading := h.downloads.LoadOrStore(objectPath, ready); downloading {
		slog.Debug("oci blob already downloading, waiting", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
		select {
		case <-ctx.Done():
			return 0, "", 0, ctx.Err()
		case <-active.(chan struct{}):
		}
		if status, bytes, cacheErr := h.serveCachedObject(ctx, w, req, objectPath, "COALESCED"); cacheErr == nil {
			return status, "COALESCED", bytes, nil
		}
		return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
	slog.Debug("oci blob miss, fetching", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
	return h.fetchBlob(w, req, resolved, ready)
}

func (h *handler) serveCachedObject(ctx context.Context, w http.ResponseWriter, req *http.Request, objectPath, cache string) (int, uint64, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = reader.Close() }()
	info := reader.Info()
	if !httpcache.CacheSupportsRequestUserAgent(h.client, req, info.Options) {
		return 0, 0, errors.New("cached OCI object has unknown or incompatible User-Agent variance")
	}
	headers := map[string]string{
		"Content-Length": info.Options["content-length"],
		"Content-Type":   info.Options["content-type"],
		"ETag":           info.Options["etag"],
		"Last-Modified":  info.Options["last-modified"],
		"Vary":           info.Options["vary"],
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
	userAgent, _ := h.client.RequestUserAgent(req)
	response, err := h.remoteRequest(ctx, ctx, req.Method, upstreamPath, userAgent, headers)
	if err != nil {
		return 0, "", 0, err
	}
	defer func() { _ = response.Body.Close() }()
	status, bytes, err := h.copyRemote(w, req, response, cache)
	return status, cache, bytes, err
}

func (h *handler) putObjectFromReader(ctx context.Context, objectPath string, body io.Reader, size int64, headers http.Header, extra map[string]string) error {
	meta := map[string]string{
		"content-type":                    headers.Get("Content-Type"),
		"content-length":                  strconv.FormatInt(size, 10),
		"fetched-at":                      time.Now().UTC().Format(time.RFC3339Nano),
		httpcache.UserAgentReviewedOption: "true",
	}
	for _, key := range []string{"ETag", "Last-Modified", "Vary", "Docker-Content-Digest"} {
		if value := headers.Get(key); value != "" {
			meta[strings.ToLower(key)] = value
		}
	}
	for key, value := range extra {
		if value != "" {
			meta[key] = value
		}
	}
	return h.storeObject(ctx, objectPath, body, meta)
}

func (h *handler) storeObject(ctx context.Context, objectPath string, body io.Reader, meta map[string]string) error {
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			return err
		}
	}
	_, err := h.store.Put(ctx, h.name, objectPath, body, meta)
	return err
}

func (h *handler) refStatePath(repo, ref string) string {
	return path.Join(h.refDir(repo, ref), "state.yaml")
}

func (h *handler) manifestPath(digest string) string {
	algorithm, encoded, _ := strings.Cut(digest, ":")
	return path.Join("oci/manifests", algorithm, encoded)
}

func (h *handler) blobPath(digest string) string {
	return path.Join("oci/blobs", strings.ReplaceAll(digest, ":", "/"))
}

func (h *handler) refDir(repo, ref string) string {
	return path.Join("oci/refs", repo, httpcache.HashKey(ref))
}
