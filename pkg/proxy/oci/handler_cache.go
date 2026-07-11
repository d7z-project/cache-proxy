package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *handler) fetchManifest(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request) (int, uint64, error) {
	h.stats.AddActiveDownload(h.name, config.ModeOCI, 1)
	defer h.stats.AddActiveDownload(h.name, config.ModeOCI, -1)

	slog.Debug("oci fetch manifest", "instance", h.name, "repo", resolved.repo, "ref", resolved.ref, "upstream", h.upstream)
	response, err := h.remoteRequest(ctx, http.MethodGet, resolved.upstreamPath, map[string]string{"Accept": manifestAccept})
	if err != nil {
		return 0, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return h.copyRemote(w, req, response, "BYPASS")
	}

	tempFile, size, err := utils.TempFileFromReader(io.LimitReader(response.Body, 50<<20))
	if err != nil {
		return 0, 0, err
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())
	if size > 50<<20 {
		return 0, 0, fmt.Errorf("oci manifest exceeds size limit")
	}

	manifestDigest := response.Header.Get("Docker-Content-Digest")
	if manifestDigest != "" {
		if err := verifyDigestReader(manifestDigest, tempFile); err != nil {
			return 0, 0, err
		}
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return 0, 0, err
		}
	}
	if manifestDigest == "" {
		sum := sha256.New()
		if _, err := io.Copy(sum, tempFile); err != nil {
			return 0, 0, err
		}
		manifestDigest = "sha256:" + hex.EncodeToString(sum.Sum(nil))
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return 0, 0, err
		}
	}

	blobDigests := collectBlobDigests(tempFile)
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}

	state := refState{
		Repo:           resolved.repo,
		Ref:            resolved.ref,
		FetchedAt:      time.Now().UTC(),
		ExpireAfter:    effectiveExpire(resolved.match.expireAfter, h.expireAfter),
		ManifestDigest: manifestDigest,
		BlobDigests:    blobDigests,
	}

	meta := map[string]string{
		"content-type":          response.Header.Get("Content-Type"),
		"content-length":        strconv.FormatInt(size, 10),
		"fetched-at":            time.Now().UTC().Format(time.RFC3339Nano),
		"docker-content-digest": manifestDigest,
	}
	if v := response.Header.Get("ETag"); v != "" {
		meta["etag"] = v
	}
	if v := response.Header.Get("Last-Modified"); v != "" {
		meta["last-modified"] = v
	}
	if err := h.storeObject(ctx, h.refManifestPath(resolved.repo, resolved.ref), tempFile, meta); err != nil {
		return 0, 0, err
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}

	if err := h.writeState(ctx, state); err != nil {
		return 0, 0, err
	}

	for _, d := range blobDigests {
		h.rememberBlob(d, state)
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
	return status, bytes, err
}

func (h *handler) fetchBlob(ctx context.Context, w http.ResponseWriter, req *http.Request, resolved request, state refState) (int, string, uint64, error) {
	slog.Debug("oci fetch blob", "instance", h.name, "repo", resolved.repo, "digest", resolved.digest, "upstream", h.upstream)
	objectPath := h.refBlobPath(state.Repo, state.Ref, resolved.digest)
	cleanupDownload := true
	defer func() {
		if cleanupDownload {
			h.downloads.Delete(objectPath)
		}
	}()

	response, err := h.remoteRequest(ctx, http.MethodGet, resolved.upstreamPath, nil)
	if err != nil {
		return 0, "", 0, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}

	contentLen := response.ContentLength
	respHeader := response.Header

	pr, err := httpcache.StreamToPipe(ctx, httpcache.StreamConfig{
		Body:       response.Body,
		Instance:   h.name,
		ObjectPath: objectPath,
		Downloads:  &h.downloads,
		Wait:       &h.wait,
		Limiter:    h.downloadsLimiter,
		StatsStart: func() { h.stats.AddActiveDownload(h.name, config.ModeOCI, 1) },
		StatsDone:  func() { h.stats.AddActiveDownload(h.name, config.ModeOCI, -1) },
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

func verifyDigestBytes(digest string, body []byte) error {
	return verifyDigestReader(digest, bytes.NewReader(body))
}

func verifyDigestReader(digest string, reader io.Reader) error {
	algo, expected, ok := strings.Cut(digest, ":")
	if !ok || algo != "sha256" || len(expected) != 64 {
		return nil
	}
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

func (h *handler) findBlobState(ctx context.Context, repo, digest string) (refState, error) {
	if ref, ok := h.lookupBlob(digest); ok {
		state, err := h.readState(ctx, h.refStatePath(ref.repo, ref.ref))
		if err == nil && !h.stateExpired(state) {
			return state, nil
		}
		h.forgetBlob(digest)
	}
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
				h.rememberBlob(digest, state)
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

func (h *handler) purgeBlobIndex() {
	h.blobIndexMu.Lock()
	defer h.blobIndexMu.Unlock()
	now := time.Now()
	for digest, entry := range h.blobIndex {
		if !entry.expires.IsZero() && now.After(entry.expires) {
			delete(h.blobIndex, digest)
		}
	}
	for len(h.blobIndex) > maxBlobIndexEntries {
		for digest := range h.blobIndex {
			delete(h.blobIndex, digest)
			break
		}
	}
}

const maxBlobIndexEntries = 8192

func (h *handler) rememberBlob(digest string, state refState) {
	if digest == "" {
		return
	}
	expireAfter := effectiveExpire(state.ExpireAfter, h.expireAfter)
	var expires time.Time
	if !expireAfter.IsNever() && !expireAfter.IsUnset() {
		expires = state.FetchedAt.Add(expireAfter.Duration())
	}
	h.blobIndexMu.Lock()
	h.blobIndex[digest] = blobIndexEntry{ref: blobRef{repo: state.Repo, ref: state.Ref}, expires: expires}
	over := len(h.blobIndex) - maxBlobIndexEntries
	for digest := range h.blobIndex {
		if over <= 0 {
			break
		}
		delete(h.blobIndex, digest)
		over--
	}
	h.blobIndexMu.Unlock()
}

func (h *handler) lookupBlob(digest string) (blobRef, bool) {
	h.blobIndexMu.Lock()
	defer h.blobIndexMu.Unlock()
	entry, ok := h.blobIndex[digest]
	if !ok {
		return blobRef{}, false
	}
	if !entry.expires.IsZero() && time.Now().After(entry.expires) {
		delete(h.blobIndex, digest)
		return blobRef{}, false
	}
	return entry.ref, true
}

func (h *handler) forgetBlob(digest string) {
	h.blobIndexMu.Lock()
	delete(h.blobIndex, digest)
	h.blobIndexMu.Unlock()
}

func collectBlobDigests(r io.Reader) []string {
	var doc struct {
		Config descriptor   `json:"config"`
		Layers []descriptor `json:"layers"`
		Blobs  []descriptor `json:"blobs"`
	}
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
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
