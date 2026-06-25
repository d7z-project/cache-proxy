package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

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
	response, err := h.remoteRequest(ctx, http.MethodGet, resolved.upstreamPath, nil)
	if err != nil {
		return 0, "", 0, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		status, bytes, copyErr := h.copyRemote(w, req, response, "BYPASS")
		return status, "BYPASS", bytes, copyErr
	}

	tempFile, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		response.Body.Close()
		return 0, "", 0, err
	}

	objectPath := h.refBlobPath(state.Repo, state.Ref, resolved.digest)
	contentLen := response.ContentLength
	respHeader := response.Header

	done := make(chan struct{})
	h.downloads.Store(objectPath, done)

	pr, pw := io.Pipe()
	h.wait.Add(1)
	go func() {
		h.stats.AddActiveDownload(h.name, config.ModeOCI, 1)
		defer h.stats.AddActiveDownload(h.name, config.ModeOCI, -1)
		defer h.wait.Done()
		defer close(done)
		defer h.downloads.Delete(objectPath)
		defer response.Body.Close()
		defer pw.Close()

		tee := io.TeeReader(response.Body, tempFile)
		_, copyErr := io.Copy(pw, tee)
		if copyErr == nil {
			if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr == nil {
				h.putObjectFromReader(context.Background(), objectPath, tempFile, contentLen, respHeader, nil)
			}
		}
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	headers := objectHeaders(respHeader, int(contentLen), "MISS")
	status, bytes, err := h.writeResponse(w, req.Method, http.StatusOK, headers, pr)
	return status, "MISS", bytes, err
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
