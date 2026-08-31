package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const (
	maxManifestSize          = 50 << 20
	ociRefStateSchemaVersion = 1
)

var ociStateTempSequence atomic.Uint64

func (h *handler) fetchManifest(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	h.stats.AddActiveDownload(h.name, config.ModeOCI, 1)
	defer h.stats.AddActiveDownload(h.name, config.ModeOCI, -1)

	slog.Debug("oci fetch manifest", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "upstream", h.upstream)
	userAgent, _ := h.client.RequestUserAgent(req)
	requestHeaders := map[string]string{"Accept": manifestAccept}
	statePath := h.refStatePath(resolved.repo, resolved.ref)
	previousState, previousStateErr := h.readState(ctx, statePath)
	if previousStateErr == nil {
		if previousState.ETag != "" {
			requestHeaders["If-None-Match"] = previousState.ETag
		}
		if previousState.LastModified != "" {
			requestHeaders["If-Modified-Since"] = previousState.LastModified
		}
	}
	response, err := h.remoteRequest(req.Context(), h.lifecycleCtx, http.MethodGet, resolved.upstreamPath, userAgent, requestHeaders)
	if err != nil {
		return 0, "", 0, err
	}
	defer func() { _ = response.Body.Close() }()
	cacheCtx := h.lifecycleCtx
	if response.StatusCode == http.StatusNotModified {
		if previousStateErr != nil {
			return 0, "", 0, previousStateErr
		}
		previousState.FetchedAt = time.Now().UTC()
		if stateErr := h.writeState(cacheCtx, previousState); stateErr != nil {
			slog.Warn("oci revalidation state update failed; serving committed manifest", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "err", stateErr)
			status, bytes, serveErr := h.serveManifestState(ctx, w, req, previousState, "STALE")
			return status, "STALE", bytes, serveErr
		}
		status, bytes, serveErr := h.serveManifestState(ctx, w, req, previousState, "REVALIDATED")
		return status, "REVALIDATED", bytes, serveErr
	}
	if response.StatusCode != http.StatusOK {
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}
	if !h.client.UserAgentConfigured && utils.VariesByUserAgent(response.Header.Values("Vary")...) {
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}

	tempFile, size, err := utils.TempFileFromReader(io.LimitReader(response.Body, maxManifestSize+1))
	if err != nil {
		return 0, "", 0, err
	}
	_ = response.Body.Close()
	defer func() { _ = tempFile.Close() }()
	defer func() { _ = os.Remove(tempFile.Name()) }()
	if size > maxManifestSize {
		return 0, "", 0, errors.New("oci manifest exceeds size limit")
	}

	manifestDigest := response.Header.Get("Docker-Content-Digest")
	if manifestDigest != "" {
		if err := verifyDigestReader(manifestDigest, tempFile); err != nil {
			return 0, "", 0, err
		}
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return 0, "", 0, err
		}
	}
	if manifestDigest == "" {
		sum := sha256.New()
		if _, err := io.Copy(sum, tempFile); err != nil {
			return 0, "", 0, err
		}
		manifestDigest = "sha256:" + hex.EncodeToString(sum.Sum(nil))
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return 0, "", 0, err
		}
	}

	expireAfter := effectiveExpire(resolved.match.expireAfter, h.expireAfter)
	if isSHA256Digest(resolved.ref) {
		expireAfter = config.ExpirationNever
	}
	fetchedAt := time.Now().UTC()
	state := refState{
		Version:        ociRefStateSchemaVersion,
		SourceUpstream: h.upstream,
		Repo:           resolved.repo,
		Ref:            resolved.ref,
		FetchedAt:      fetchedAt,
		ExpireAfter:    expireAfter,
		ManifestDigest: manifestDigest,
		ContentType:    response.Header.Get("Content-Type"),
		ContentLength:  size,
		ETag:           response.Header.Get("ETag"),
		LastModified:   response.Header.Get("Last-Modified"),
		Vary:           strings.Join(response.Header.Values("Vary"), ", "),
	}

	meta := map[string]string{
		"content-type":                    state.ContentType,
		"content-length":                  strconv.FormatInt(size, 10),
		"fetched-at":                      fetchedAt.Format(time.RFC3339Nano),
		"docker-content-digest":           manifestDigest,
		httpcache.UserAgentReviewedOption: "true",
	}
	manifestPath := h.manifestPath(manifestDigest)
	info, statErr := h.store.StatObject(cacheCtx, h.name, manifestPath)
	if statErr != nil || info.Options["docker-content-digest"] != manifestDigest {
		if storeErr := h.storeObject(cacheCtx, manifestPath, tempFile, meta); storeErr != nil {
			slog.Warn("oci manifest cache write failed; serving verified upstream response", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "err", storeErr)
			if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr != nil {
				return 0, "", 0, errors.Join(storeErr, seekErr)
			}
			return h.writeManifestTemp(w, req, state, tempFile, "BYPASS")
		}
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return 0, "", 0, err
	}

	if err := h.writeState(cacheCtx, state); err != nil {
		slog.Warn("oci manifest state commit failed; serving verified upstream response", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "err", err)
		return h.writeManifestTemp(w, req, state, tempFile, "BYPASS")
	}
	return h.writeManifestTemp(w, req, state, tempFile, "MISS")
}

func (h *handler) writeManifestTemp(w http.ResponseWriter, req *http.Request, state refState, tempFile io.Reader, cache string) (int, string, uint64, error) {
	headers := map[string]string{
		"Content-Type":          state.ContentType,
		"Content-Length":        strconv.FormatInt(state.ContentLength, 10),
		"ETag":                  state.ETag,
		"Last-Modified":         state.LastModified,
		"Vary":                  state.Vary,
		"X-Cache":               cache,
		"Docker-Content-Digest": state.ManifestDigest,
	}
	status, bytes, err := h.writeResponse(w, req.Method, http.StatusOK, headers, tempFile)
	return status, cache, bytes, err
}

func (h *handler) fetchBlob(w http.ResponseWriter, req *http.Request, resolved request, ready chan struct{}) (int, string, uint64, error) {
	slog.Debug("oci fetch blob", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest, "upstream", h.upstream)
	objectPath := h.blobPath(resolved.digest)
	cleanupDownload := true
	defer func() {
		if cleanupDownload {
			close(ready)
			h.downloads.Delete(objectPath)
		}
	}()

	userAgent, _ := h.client.RequestUserAgent(req)
	response, err := h.remoteRequest(req.Context(), h.lifecycleCtx, http.MethodGet, resolved.upstreamPath, userAgent, nil)
	if err != nil {
		return 0, "", 0, err
	}
	if response.StatusCode != http.StatusOK {
		defer func() { _ = response.Body.Close() }()
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}
	if !h.client.UserAgentConfigured && utils.VariesByUserAgent(response.Header.Values("Vary")...) {
		defer func() { _ = response.Body.Close() }()
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}

	contentLen := response.ContentLength
	respHeader := response.Header

	pr, err := httpcache.StreamToCache(h.lifecycleCtx, httpcache.StreamConfig{
		Body:       response.Body,
		ObjectPath: objectPath,
		Wait:       &h.wait,
		StatsStart: func() { h.stats.AddActiveDownload(h.name, config.ModeOCI, 1) },
		StatsDone:  func() { h.stats.AddActiveDownload(h.name, config.ModeOCI, -1) },
		Done: func(error) {
			close(ready)
			h.downloads.Delete(objectPath)
		},
		VerifyFn: func(r io.ReadSeeker) error {
			return verifyDigestReader(resolved.digest, r)
		},
		StoreFn: func(ctx context.Context, r io.Reader) error {
			return h.putObjectFromReader(ctx, objectPath, r, contentLen, respHeader, nil)
		},
	})
	if err != nil {
		return 0, "", 0, err
	}
	defer func() { _ = pr.Close() }()
	cleanupDownload = false

	headers := objectHeaders(respHeader, int(contentLen), "MISS")
	status, bytes, err := h.writeResponse(w, req.Method, http.StatusOK, headers, pr)
	return status, "MISS", bytes, err
}

func verifyDigestReader(digest string, reader io.Reader) error {
	if !isSHA256Digest(digest) {
		return fmt.Errorf("invalid SHA256 digest %q", digest)
	}
	_, expected, _ := strings.Cut(digest, ":")
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return err
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("digest mismatch: expected %s got sha256:%s", digest, actual)
	}
	return nil
}

func isSHA256Digest(value string) bool {
	algorithm, encoded, ok := strings.Cut(value, ":")
	if !ok || algorithm != "sha256" || len(encoded) != sha256.Size*2 || encoded != strings.ToLower(encoded) {
		return false
	}
	_, err := hex.DecodeString(encoded)
	return err == nil
}

func (h *handler) readState(ctx context.Context, objectPath string) (refState, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return refState{}, err
	}
	defer func() { _ = reader.Close() }()
	var state refState
	if err := yaml.NewDecoder(reader).Decode(&state); err != nil {
		return refState{}, err
	}
	if !h.validRefState(state) {
		return refState{}, errors.New("invalid oci ref state")
	}
	return state, nil
}

func (h *handler) writeState(ctx context.Context, state refState) error {
	state.Version = ociRefStateSchemaVersion
	state.SourceUpstream = h.upstream
	if !h.validRefState(state) {
		return errors.New("invalid oci ref state")
	}
	data, err := yaml.Marshal(&state)
	if err != nil {
		return err
	}
	statePath := h.refStatePath(state.Repo, state.Ref)
	tempPath := statePath + ".tmp." + strconv.FormatUint(ociStateTempSequence.Add(1), 36)
	if err := h.storeObject(ctx, tempPath, bytes.NewReader(data), map[string]string{"content-type": "application/yaml"}); err != nil {
		return fmt.Errorf("write oci ref state staging object: %w", err)
	}
	if err := h.store.Rename(path.Join(h.name, tempPath), path.Join(h.name, statePath)); err != nil {
		_ = h.store.DeleteObject(context.Background(), h.name, tempPath)
		return fmt.Errorf("publish oci ref state: %w", err)
	}
	return nil
}

func (h *handler) validRefState(state refState) bool {
	return state.Version == ociRefStateSchemaVersion && state.SourceUpstream == h.upstream &&
		state.Repo != "" && state.Ref != "" && !state.FetchedAt.IsZero() &&
		isSHA256Digest(state.ManifestDigest) && state.ContentLength >= 0
}

func (h *handler) stateExpired(state refState) bool {
	expireAfter := effectiveExpire(state.ExpireAfter, h.expireAfter)
	return !expireAfter.IsNever() && !expireAfter.IsUnset() && time.Now().After(state.FetchedAt.Add(expireAfter.Duration()))
}

func (h *handler) deleteTree(ctx context.Context, prefix string) error {
	var objects []string
	if err := fs.WalkDir(h.store.TenantFS(h.name), prefix, func(current string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
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
		"Vary":           strings.Join(headers.Values("Vary"), ", "),
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
