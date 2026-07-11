package oci

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func newHandler(name string, block Block, expireAfter config.Expiration, store *blobfs.Store, stats *httpcache.Stats, downloads *httpcache.DownloadLimiter) *handler {
	client := utils.DefaultHttpClientWrapper()
	httpcache.ConfigureClientTransport(client, name, block.Transport)
	return &handler{
		name:             name,
		upstream:         strings.TrimRight(block.Upstream, "/"),
		expireAfter:      expireAfter,
		policy:           &block.Policy,
		store:            store,
		stats:            stats,
		client:           client,
		downloadsLimiter: downloads,
		auth:             authHandler{tokens: map[string]ociToken{}},
		blobIndex:        map[string]blobIndexEntry{},
	}
}

func (h *handler) Start(ctx context.Context) error {
	return nil
}

func (h *handler) purgeExpiredTokens() {
	h.auth.tokenMu.Lock()
	defer h.auth.tokenMu.Unlock()
	now := time.Now()
	for key, token := range h.auth.tokens {
		if !now.Before(token.expire) {
			delete(h.auth.tokens, key)
		}
	}
	h.purgeBlobIndex()
}

func (h *handler) Stop(ctx context.Context) error {
	return utils.WaitGroupContext(ctx, &h.wait)
}

func (h *handler) Cleanup(ctx context.Context, opts config.CleanupConfig) error {
	deleted := 0
	return fs.WalkDir(h.store.TenantFS(h.name), "oci/refs", func(current string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.BatchSize > 0 && deleted >= opts.BatchSize {
			return fs.SkipAll
		}
		if err != nil || entry.IsDir() || path.Base(current) != "state.yaml" {
			return nil
		}
		state, readErr := h.readState(ctx, current)
		if readErr != nil || h.stateExpired(state) {
			if opts.DryRun {
				deleted++
				slog.Info("oci cleanup dry-run delete", "instance", h.name, "prefix", path.Dir(current))
				return nil
			}
			if removeErr := h.deleteTree(ctx, path.Dir(current)); removeErr != nil && !errors.Is(removeErr, context.Canceled) {
				slog.Info("oci cleanup delete failed", "instance", h.name, "prefix", path.Dir(current), "err", removeErr)
			} else {
				deleted++
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
		slog.Info("oci proxy failed", "instance", h.name, "method", req.Method, "path", req.URL.Path, "err", err)
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
			slog.Debug("oci manifest cache hit", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref)
			return status, "HIT", bytes, nil
		}
	}

	staleState := state
	status, bytes, fetchErr := h.fetchManifest(ctx, w, req, resolved)
	if fetchErr == nil {
		slog.Debug("oci manifest fetched", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref)
		return status, "MISS", bytes, nil
	}
	if resolved.match.busyPolicy == config.BusyPolicyStale && staleState.Repo != "" {
		slog.Debug("oci manifest fetch failed, serving stale", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "err", fetchErr)
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
			slog.Debug("oci blob cache hit", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
			return status, "HIT", bytes, nil
		}
	}
	if err != nil {
		slog.Debug("oci blob not found in refs, bypass", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
		return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
	objectPath := h.refBlobPath(state.Repo, state.Ref, resolved.digest)
	if _, downloading := h.downloads.LoadOrStore(objectPath, struct{}{}); downloading {
		slog.Debug("oci blob already downloading, bypass", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
		return h.serveRemote(ctx, w, req, resolved.upstreamPath, "BYPASS", nil)
	}
	slog.Debug("oci blob miss, fetching", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest)
	return h.fetchBlob(ctx, w, req, resolved, state)
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

func (h *handler) putObjectFromReader(ctx context.Context, objectPath string, body io.Reader, size int64, headers http.Header, extra map[string]string) error {
	meta := map[string]string{
		"content-type":   headers.Get("Content-Type"),
		"content-length": strconv.FormatInt(size, 10),
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

func (h *handler) refManifestPath(repo, ref string) string {
	return path.Join(h.refDir(repo, ref), "manifest")
}

func (h *handler) refBlobPath(repo, ref, digest string) string {
	return path.Join(h.refDir(repo, ref), "blobs", strings.ReplaceAll(digest, ":", "/"))
}

func (h *handler) refDir(repo, ref string) string {
	return path.Join("oci/refs", repo, httpcache.HashKey(ref))
}
