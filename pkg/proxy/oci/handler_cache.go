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
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxManifestSize = 50 << 20

func (h *handler) fetchManifest(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, string, uint64, error) {
	h.stats.AddActiveDownload(h.name, config.ModeOCI, 1)
	defer h.stats.AddActiveDownload(h.name, config.ModeOCI, -1)

	slog.Debug("oci fetch manifest", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "upstream", h.upstream)
	userAgent, _ := h.client.RequestUserAgent(req)
	requestHeaders := map[string]string{"Accept": manifestAccept}
	statePath := h.refStatePath(resolved.repo, resolved.ref)
	manifestPath := h.refManifestPath(resolved.repo, resolved.ref)
	if info, statErr := h.store.StatObject(ctx, h.name, manifestPath); statErr == nil && info.Options["source-upstream"] == h.upstream {
		if etag := info.Options["etag"]; etag != "" {
			requestHeaders["If-None-Match"] = etag
		}
		if modified := info.Options["last-modified"]; modified != "" {
			requestHeaders["If-Modified-Since"] = modified
		}
	}
	response, err := h.remoteRequest(ctx, http.MethodGet, resolved.upstreamPath, userAgent, requestHeaders)
	if err != nil {
		return 0, "", 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		state, stateErr := h.readState(ctx, statePath)
		if stateErr != nil {
			return 0, "", 0, stateErr
		}
		state.FetchedAt = time.Now().UTC()
		if stateErr = h.writeState(ctx, state); stateErr != nil {
			return 0, "", 0, stateErr
		}
		if info, statErr := h.store.StatObject(ctx, h.name, manifestPath); statErr == nil {
			options := make(map[string]string, len(info.Options))
			for key, value := range info.Options {
				options[key] = value
			}
			options["fetched-at"] = state.FetchedAt.Format(time.RFC3339Nano)
			if _, updateErr := h.store.UpdateMetadata(ctx, h.name, manifestPath, options); updateErr != nil {
				return 0, "", 0, updateErr
			}
		}
		status, bytes, serveErr := h.serveCachedObject(ctx, w, req, manifestPath, "REVALIDATED")
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
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())
	if size > maxManifestSize {
		return 0, "", 0, fmt.Errorf("oci manifest exceeds size limit")
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
	state := refState{
		Repo:           resolved.repo,
		Ref:            resolved.ref,
		FetchedAt:      time.Now().UTC(),
		ExpireAfter:    expireAfter,
		ManifestDigest: manifestDigest,
	}

	meta := map[string]string{
		"content-type":                    response.Header.Get("Content-Type"),
		"content-length":                  strconv.FormatInt(size, 10),
		"fetched-at":                      time.Now().UTC().Format(time.RFC3339Nano),
		"docker-content-digest":           manifestDigest,
		"source-upstream":                 h.upstream,
		httpcache.UserAgentReviewedOption: "true",
	}
	if v := response.Header.Get("ETag"); v != "" {
		meta["etag"] = v
	}
	if v := response.Header.Get("Last-Modified"); v != "" {
		meta["last-modified"] = v
	}
	if v := strings.Join(response.Header.Values("Vary"), ", "); v != "" {
		meta["vary"] = v
	}
	if err := h.storeObject(ctx, h.refManifestPath(resolved.repo, resolved.ref), tempFile, meta); err != nil {
		return 0, "", 0, err
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return 0, "", 0, err
	}

	if err := h.writeState(ctx, state); err != nil {
		return 0, "", 0, err
	}

	headers := map[string]string{
		"Content-Type":          response.Header.Get("Content-Type"),
		"Content-Length":        strconv.FormatInt(size, 10),
		"ETag":                  response.Header.Get("ETag"),
		"Last-Modified":         response.Header.Get("Last-Modified"),
		"X-Cache":               "MISS",
		"Docker-Content-Digest": manifestDigest,
	}
	status, bytes, err := h.writeResponse(w, req.Method, http.StatusOK, headers, tempFile)
	return status, "MISS", bytes, err
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
	response, err := h.remoteRequest(h.lifecycleCtx, http.MethodGet, resolved.upstreamPath, userAgent, nil)
	if err != nil {
		return 0, "", 0, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}
	if !h.client.UserAgentConfigured && utils.VariesByUserAgent(response.Header.Values("Vary")...) {
		defer response.Body.Close()
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
	return h.storeObject(ctx, h.refStatePath(state.Repo, state.Ref), bytes.NewReader(data), map[string]string{"content-type": "application/yaml"})
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
