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
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const maxTokenCacheEntries = 2048

type tokenCacheEntry struct {
	key    string
	expire time.Time
}

func newHandler(name string, block Block, expireAfter config.Expiration, store *blobfs.Store, stats *metrics.Stats, upstreamGate *transport.UpstreamGate) *handler {
	if block.MetadataTTL <= 0 {
		block.MetadataTTL = 2 * time.Minute
	}
	if block.Spooler == nil {
		block.Spooler = storeio.NewSpooler(block.WorkDir, 2<<30, nil)
	}
	client := transport.NewUpstreamHTTPClient()
	transport.ConfigureHTTPClient(client, name, block.Transport)
	transport.ConfigureAdmission(client.Client, upstreamGate)
	return &handler{
		name:            name,
		upstream:        strings.TrimRight(block.Upstream, "/"),
		expireAfter:     expireAfter,
		metadataTTL:     block.MetadataTTL,
		workDir:         block.WorkDir,
		spooler:         block.Spooler,
		options:         &block.Options,
		store:           store,
		stats:           stats,
		client:          client,
		upstreamGate:    upstreamGate,
		lifecycle:       storeio.NewLifecycle(),
		auth:            authHandler{tokens: map[string]ociToken{}},
		refLocks:        &referenceLocks{},
		manifestReaders: map[string]int{},
	}
}

func (h *handler) Start(context.Context) error { return nil }

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
	return h.lifecycle.Close(ctx)
}

func (h *handler) Cleanup(ctx context.Context, opts config.CleanupConfig) (bool, error) {
	h.cleanupMu.Lock()
	defer h.cleanupMu.Unlock()
	if h.cleanupPhase == "" {
		h.cleanupPhase = "refs"
		h.cleanupRefs = make(map[string]struct{})
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 500
	}

	for {
		switch h.cleanupPhase {
		case "refs":
			complete, err := h.cleanupWalk(ctx, "oci/refs", batch, func(current string) (bool, error) {
				baseName := path.Base(current)
				if strings.HasPrefix(baseName, "state.yaml.tmp.") {
					statePath := path.Join(path.Dir(current), "state.yaml")
					lock := h.refLocks.Get(statePath)
					if !lock.TryLock() {
						return true, nil
					}
					defer lock.Unlock()
					if opts.DryRun {
						slog.Info("oci cleanup dry-run delete abandoned state", "instance", h.name, "path", current)
						return false, nil
					}
					return false, h.store.DeleteObject(ctx, h.name, current)
				}
				if baseName != "state.yaml" {
					return false, nil
				}
				lock := h.refLocks.Get(current)
				if !lock.TryLock() {
					return true, nil
				}
				defer lock.Unlock()
				state, readErr := h.readState(ctx, current)
				if readErr == nil && (!h.stateExpired(state) || opts.DryRun) {
					h.cleanupRefs[h.manifestPath(state.ManifestDigest)] = struct{}{}
				}
				if readErr == nil && !h.stateExpired(state) {
					return false, nil
				}
				if opts.DryRun {
					slog.Info("oci cleanup dry-run delete", "instance", h.name, "prefix", path.Dir(current))
					return false, nil
				}
				return false, h.store.DeleteObject(ctx, h.name, current)
			})
			if err != nil || !complete {
				return !complete, err
			}
			h.cleanupPhase, h.cleanupCursor = "manifests", ""
			if h.expireAfter.IsNever() || h.expireAfter.IsUnset() {
				h.cleanupPhase, h.cleanupRefs = "refs", nil
				return false, nil
			}
		case "manifests":
			complete, err := h.cleanupWalk(ctx, "oci/manifests", batch, func(current string) (bool, error) {
				if _, referenced := h.cleanupRefs[current]; referenced {
					return false, nil
				}
				h.manifestMu.Lock()
				active := h.manifestReaders[current] > 0
				h.manifestMu.Unlock()
				if active {
					return true, nil
				}
				_, err := h.cleanupExpiredObject(ctx, current, "manifest", opts)
				return false, err
			})
			if err != nil || !complete {
				return !complete, err
			}
			h.cleanupPhase, h.cleanupCursor = "blobs", ""
		case "blobs":
			complete, err := h.cleanupWalk(ctx, "oci/blobs", batch, func(current string) (bool, error) {
				if _, active := h.downloads.Load(current); active {
					return true, nil
				}
				_, err := h.cleanupExpiredObject(ctx, current, "blob", opts)
				return false, err
			})
			if err != nil || !complete {
				return !complete, err
			}
			h.cleanupPhase, h.cleanupCursor, h.cleanupRefs = "refs", "", nil
			return false, nil
		}
	}
}

func (h *handler) cleanupWalk(ctx context.Context, root string, batch int, visit func(string) (retry bool, err error)) (bool, error) {
	inspected := 0
	complete := true
	err := fs.WalkDir(h.store.TenantFS(h.name), root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || current <= h.cleanupCursor {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if inspected >= batch {
			complete = false
			return fs.SkipAll
		}
		retry, err := visit(current)
		if err != nil {
			return err
		}
		if retry {
			complete = false
			return fs.SkipAll
		}
		h.cleanupCursor = current
		inspected++
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	return complete, err
}

func (h *handler) cleanupExpiredObject(ctx context.Context, objectPath, objectKind string, opts config.CleanupConfig) (bool, error) {
	info, err := h.store.StatObject(ctx, h.name, objectPath)
	if err != nil {
		return false, err
	}
	fetchedAt, fetchedAtErr := time.Parse(time.RFC3339Nano, info.Options["fetched-at"])
	if fetchedAtErr != nil {
		fetchedAt, fetchedAtErr = time.Parse(http.TimeFormat, info.Options["fetched-at"])
	}
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
	if !proxyruntime.RequireReadMethod(w, req.Method) {
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "REJECTED", http.StatusMethodNotAllowed, 0)
		return
	}
	_, done, err := h.lifecycle.Begin()
	if err != nil {
		transport.WriteError(w, http.StatusServiceUnavailable)
		return
	}
	defer done()

	resolved, err := resolveRequest(req, h.options)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", http.StatusNotFound, 0)
		return
	}
	status, cache, bytes, err := h.serve(req.Context(), w, req, resolved)
	if err != nil {
		slog.Info("oci proxy failed", "instance", h.name, "method", req.Method, "path", req.URL.Path, "err", err)
		status := http.StatusBadGateway
		var limited *proxyruntime.UpstreamRateLimitError
		if errors.As(err, &limited) {
			status = http.StatusTooManyRequests
		}
		if retryAfter, admissionError := proxyruntime.AdmissionRetryAfterSeconds(err); admissionError {
			if !errors.As(err, &limited) {
				status = http.StatusServiceUnavailable
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		transport.WriteError(w, status)
		h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, "ERROR", status, 0)
		return
	}
	h.stats.RecordRequest(h.name, config.ModeOCI, req.Method, cache, status, bytes)
}

func (h *handler) serve(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	if resolved.kind == requestPing {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.Header().Set("X-Cache", "LOCAL")
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, "LOCAL", 0, nil
	}
	if resolved.kind == requestTags || resolved.kind == requestBypass {
		return h.serveUpstream(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
	switch resolved.kind {
	case requestManifest:
		return h.serveManifest(ctx, w, req, resolved)
	case requestBlob:
		return h.serveBlob(ctx, w, req, resolved)
	default:
		return h.serveUpstream(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
}

func (h *handler) serveManifest(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	if isSHA256Digest(resolved.ref) {
		objectPath := h.manifestPath(resolved.ref)
		h.manifestMu.Lock()
		h.manifestReaders[objectPath]++
		h.manifestMu.Unlock()
		status, bytes, cacheErr := h.serveCachedObject(ctx, w, req, objectPath, "HIT")
		h.manifestMu.Lock()
		h.manifestReaders[objectPath]--
		if h.manifestReaders[objectPath] == 0 {
			delete(h.manifestReaders, objectPath)
		}
		h.manifestMu.Unlock()
		if cacheErr == nil {
			return status, "HIT", bytes, nil
		}
	}
	statePath := h.refStatePath(resolved.repo, resolved.ref, req.Header.Get("Accept"))
	state, err := h.readState(ctx, statePath)
	if err == nil && h.manifestFresh(resolved, state) && !transport.RequestForcesRevalidation(req) {
		if status, bytes, cacheErr := h.serveManifestState(ctx, w, req, state, "HIT"); cacheErr == nil {
			slog.Debug("oci manifest cache hit", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref)
			return status, "HIT", bytes, nil
		}
	}

	staleState := state
	lock := h.refLocks.Get(statePath)
	if !lock.TryLock() {
		if staleState.Repo != "" {
			if status, bytes, cacheErr := h.serveManifestState(ctx, w, req, staleState, "STALE"); cacheErr == nil {
				return status, "STALE", bytes, nil
			}
		}
		lock.Lock()
	}
	defer lock.Unlock()

	state, err = h.readState(ctx, statePath)
	if err == nil && h.manifestFresh(resolved, state) && !transport.RequestForcesRevalidation(req) {
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
	if staleState.Repo != "" {
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
		!cacheSupportsRequestUserAgent(h.client, req, info.Options) {
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
	if isSHA256Digest(resolved.ref) {
		return true
	}
	return h.metadataTTL > 0 && time.Since(state.FetchedAt) <= h.metadataTTL
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
		return h.serveUpstream(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
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
	if !cacheSupportsRequestUserAgent(h.client, req, info.Options) {
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

func (h *handler) serveUpstream(ctx context.Context, w http.ResponseWriter, req *http.Request, upstreamPath, cache string, headers map[string]string) (int, string, uint64, error) {
	userAgent, _ := h.client.RequestUserAgent(req)
	response, err := h.readUpstream(ctx, ctx, req.Method, upstreamPath, req.URL.RawQuery, userAgent, headers)
	if err != nil {
		return 0, "", 0, err
	}
	defer func() { _ = response.Body.Close() }()
	status, bytes, err := h.copyRemote(w, req, response, cache)
	return status, cache, bytes, err
}

func (h *handler) putObjectFromReader(ctx context.Context, objectPath string, body io.Reader, size int64, headers http.Header, extra map[string]string) error {
	meta := map[string]string{
		"content-type":          headers.Get("Content-Type"),
		"fetched-at":            time.Now().UTC().Format(time.RFC3339Nano),
		userAgentReviewedOption: "true",
	}
	if size >= 0 {
		meta["content-length"] = strconv.FormatInt(size, 10)
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

func (h *handler) refStatePath(repo, ref string, representation ...string) string {
	accept := manifestAccept
	if len(representation) > 0 && representation[0] != "" {
		accept = representation[0]
	}
	return path.Join("oci/refs", repo, hashKey(ref+"\x00"+accept), "state.yaml")
}

func (h *handler) manifestPath(digest string) string {
	algorithm, encoded, _ := strings.Cut(digest, ":")
	return path.Join("oci/manifests", algorithm, encoded)
}

func (h *handler) blobPath(digest string) string {
	return path.Join("oci/blobs", strings.ReplaceAll(digest, ":", "/"))
}
