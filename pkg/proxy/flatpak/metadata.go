package flatpak

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const currentMetadataObject = "flatpak/metadata/current.yaml"

var errMetadataUnavailable = errors.New("flatpak metadata unavailable")

type generationEntry struct {
	name string
	mod  time.Time
}

type metadataDownload struct {
	temp    string
	size    int64
	digest  string
	headers map[string]string
}

func (h *Handler) Refresh(ctx context.Context) error {
	_, err := h.RefreshTask(ctx)
	return err
}

func (h *Handler) RefreshTask(ctx context.Context) (*scheduler.TaskOutcome, error) {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()

	var firstErr error
	for _, upstream := range h.upstreams {
		next, changed, err := h.refreshFromUpstream(ctx, upstream)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !changed {
			return flatpakRefreshOutcome("unchanged", "same_as_current", next.Generation, upstream), nil
		}
		h.mu.Lock()
		h.current = next
		h.mu.Unlock()
		return flatpakRefreshOutcome("updated", "published", next.Generation, upstream), nil
	}
	if firstErr == nil {
		firstErr = errMetadataUnavailable
	}
	return nil, firstErr
}

func (h *Handler) CleanupMetadata(ctx context.Context) error {
	entries, err := fs.ReadDir(h.store.TenantFS(h.name), metadataRoot)
	if err != nil {
		return nil
	}
	var generations []generationEntry
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		generations = append(generations, generationEntry{name: entry.Name(), mod: info.ModTime()})
	}
	if len(generations) <= metadataGenerations {
		return nil
	}
	sortGenerations(generations)
	current := h.currentSnapshot().Generation
	for _, generation := range generations[:len(generations)-metadataGenerations] {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if generation.name == current {
			continue
		}
		if err := h.store.RemoveAll(path.Join(h.name, metadataRoot, generation.name)); err != nil {
			return fmt.Errorf("remove flatpak metadata generation %s: %w", generation.name, err)
		}
	}
	return nil
}

func (h *Handler) serveSummary(w http.ResponseWriter, req *http.Request) {
	current := h.currentSnapshot()
	if current.Generation == "" || time.Since(current.Published) >= h.refreshInterval {
		if err := h.Refresh(req.Context()); err != nil && current.Generation == "" {
			h.base.ProxyPassthrough(w, req, "summary", "")
			return
		}
		current = h.currentSnapshot()
	}
	if current.Generation == "" {
		h.base.ProxyPassthrough(w, req, "summary", "")
		return
	}
	h.serveMetadataObject(w, req, path.Join(metadataRoot, current.Generation, "summary"))
}

func (h *Handler) serveCompanionMetadata(w http.ResponseWriter, req *http.Request, cleanPath string) {
	current := h.currentSnapshot()
	if current.Generation == "" {
		h.base.ProxyPassthrough(w, req, cleanPath, "")
		return
	}
	objectPath := path.Join(metadataRoot, current.Generation, cleanPath)
	if _, err := h.store.StatObject(req.Context(), h.name, objectPath); err != nil {
		h.base.ProxyPassthrough(w, req, cleanPath, current.Upstream)
		return
	}
	h.serveMetadataObject(w, req, objectPath)
}

func (h *Handler) serveMetadataObject(w http.ResponseWriter, req *http.Request, objectPath string) {
	reader, err := h.store.OpenObject(req.Context(), h.name, objectPath)
	if err != nil {
		httpcache.ErrorResponse(http.StatusInternalServerError, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", http.StatusInternalServerError, 0)
		return
	}
	info := reader.Info()
	headers := map[string]string{
		"Content-Length": strconv.FormatInt(info.Size, 10),
		"X-Cache":        "GENERATION",
	}
	for key, value := range info.Options {
		headers[httpcache.HeaderName(key)] = value
	}
	httpcache.StripInternal(headers)
	response := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}
	if err := response.FlushClose(req, w); err != nil {
		return
	}
	h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "GENERATION", http.StatusOK, uint64(info.Size))
}

func (h *Handler) refreshFromUpstream(ctx context.Context, upstream string) (currentMetadata, bool, error) {
	generation := strconv.FormatInt(time.Now().UnixNano(), 36)
	summary, err := h.fetchMetadata(ctx, upstream, "summary", true)
	if err != nil {
		return currentMetadata{}, false, err
	}
	defer summary.Close()
	if err := validateSummary(summary); err != nil {
		return currentMetadata{}, false, err
	}

	objects := map[string]*metadataDownload{"summary": summary}
	for _, companion := range []string{"summary.sig", "config"} {
		item, err := h.fetchMetadata(ctx, upstream, companion, false)
		if err != nil {
			return currentMetadata{}, false, err
		}
		if item != nil {
			defer item.Close()
			objects[companion] = item
		}
	}
	fingerprint := metadataFingerprint(objects)
	current := h.currentSnapshot()
	if current.Fingerprint != "" && current.Fingerprint == fingerprint {
		return current, false, nil
	}
	for name, item := range objects {
		if err := h.putMetadata(ctx, generation, name, item); err != nil {
			return currentMetadata{}, false, err
		}
	}
	next := currentMetadata{
		Generation:  generation,
		Upstream:    upstream,
		Published:   time.Now().UTC(),
		Fingerprint: fingerprint,
	}
	if err := h.putCurrent(ctx, next); err != nil {
		return currentMetadata{}, false, err
	}
	return next, true, nil
}

func (d *metadataDownload) Close() {
	if d != nil && d.temp != "" {
		_ = os.Remove(d.temp)
	}
}

func flatpakContentLength(response *http.Response) uint64 {
	if response == nil || response.ContentLength <= 0 {
		return 0
	}
	return uint64(response.ContentLength)
}

func (h *Handler) fetchMetadata(
	ctx context.Context,
	upstream, cleanPath string,
	required bool,
) (*metadataDownload, error) {
	targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(cleanPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create flatpak metadata request %s: %w", cleanPath, err)
	}
	request.Header.Set("User-Agent", h.client.UserAgent)

	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		h.stats.RecordUpstreamRequest(h.name, config.ModeFlatpak, upstream, http.MethodGet, 0, latency, 0)
		return nil, fmt.Errorf("fetch flatpak metadata %s: %w", cleanPath, err)
	}
	defer response.Body.Close()
	h.stats.RecordUpstreamRequest(
		h.name,
		config.ModeFlatpak,
		upstream,
		http.MethodGet,
		response.StatusCode,
		latency,
		flatpakContentLength(response),
	)
	if response.StatusCode != http.StatusOK {
		if !required && (response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusForbidden) {
			return nil, nil
		}
		return nil, fmt.Errorf("fetch flatpak metadata %s: HTTP %d", cleanPath, response.StatusCode)
	}

	tempFile, size, err := utils.TempFileFromReader(io.LimitReader(
		utils.NewRateLimitReader(h.client.WrapBody(response.Body)),
		maxMetadataSize+1,
	))
	if err != nil {
		return nil, fmt.Errorf("download flatpak metadata %s: %w", cleanPath, err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("close flatpak metadata temp %s: %w", cleanPath, err)
	}
	if size > maxMetadataSize {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("flatpak metadata %s exceeds %d bytes", cleanPath, maxMetadataSize)
	}
	digest, err := fileDigest(tempPath)
	if err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("hash flatpak metadata %s: %w", cleanPath, err)
	}
	headers := map[string]string{}
	for key, value := range response.Header {
		if len(value) > 0 {
			headers[http.CanonicalHeaderKey(key)] = value[0]
		}
	}
	return &metadataDownload{temp: tempPath, size: size, digest: digest, headers: headers}, nil
}

func fileDigest(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

func metadataFingerprint(objects map[string]*metadataDownload) string {
	names := make([]string, 0, len(objects))
	for name := range objects {
		names = append(names, name)
	}
	sort.Strings(names)
	sum := sha256.New()
	for _, name := range names {
		_, _ = io.WriteString(sum, name)
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, objects[name].digest)
		_, _ = io.WriteString(sum, "\x00")
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil))
}

func flatpakRefreshOutcome(result, reasonCode, generation, upstream string) *scheduler.TaskOutcome {
	return &scheduler.TaskOutcome{
		Result:     result,
		ReasonCode: reasonCode,
		Detail:     fmt.Sprintf("generation=%s upstream=%s", generation, upstream),
	}
}

func validateSummary(item *metadataDownload) error {
	if item == nil || item.size == 0 {
		return errors.New("flatpak summary is empty")
	}
	file, err := os.Open(item.temp)
	if err != nil {
		return fmt.Errorf("open flatpak summary: %w", err)
	}
	defer file.Close()
	var first [1]byte
	if _, err := file.Read(first[:]); err != nil {
		return fmt.Errorf("read flatpak summary: %w", err)
	}
	return nil
}

func (h *Handler) putMetadata(ctx context.Context, generation, name string, item *metadataDownload) error {
	file, err := os.Open(item.temp)
	if err != nil {
		return fmt.Errorf("open flatpak metadata %s: %w", name, err)
	}
	defer file.Close()
	objectPath := path.Join(metadataRoot, generation, name)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(objectPath)), 0o755); err != nil {
		return fmt.Errorf("create flatpak metadata directory %s: %w", name, err)
	}
	meta := map[string]string{
		"content-type":   item.headers["Content-Type"],
		"content-length": strconv.FormatInt(item.size, 10),
		"last-modified":  item.headers["Last-Modified"],
		"etag":           item.headers["Etag"],
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           config.ModeFlatpak,
		"cache":          "GENERATION",
	}
	if _, err := h.store.Put(ctx, h.name, objectPath, file, meta); err != nil {
		return fmt.Errorf("store flatpak metadata %s: %w", name, err)
	}
	return nil
}

func (h *Handler) putCurrent(ctx context.Context, current currentMetadata) error {
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(currentMetadataObject)), 0o755); err != nil {
		return fmt.Errorf("create flatpak current directory: %w", err)
	}
	data, err := yaml.Marshal(current)
	if err != nil {
		return fmt.Errorf("marshal flatpak current metadata: %w", err)
	}
	tmpPath := currentMetadataObject + ".tmp." + current.Generation
	if _, err := h.store.Put(ctx, h.name, tmpPath, bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         config.ModeFlatpak,
		"fetched-at":   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return fmt.Errorf("store flatpak current metadata: %w", err)
	}
	if err := h.store.Rename(path.Join(h.name, tmpPath), path.Join(h.name, currentMetadataObject)); err != nil {
		return fmt.Errorf("publish flatpak current metadata: %w", err)
	}
	return nil
}

func (h *Handler) restoreCurrent(ctx context.Context) error {
	reader, err := h.store.OpenObject(ctx, h.name, currentMetadataObject)
	if err != nil {
		return nil
	}
	defer reader.Close()
	var current currentMetadata
	if err := yaml.NewDecoder(reader).Decode(&current); err != nil {
		return fmt.Errorf("decode flatpak current metadata: %w", err)
	}
	h.mu.Lock()
	h.current = current
	h.mu.Unlock()
	return nil
}

func (h *Handler) currentSnapshot() currentMetadata {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.current
}

func sortGenerations(items []generationEntry) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].mod.Equal(items[j].mod) {
			return items[i].name < items[j].name
		}
		return items[i].mod.Before(items[j].mod)
	})
}
