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
	"log/slog"
	"net/http"
	"os"
	"path"
	"slices"
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

var (
	errMetadataUnavailable   = errors.New("flatpak metadata unavailable")
	errMetadataMirrorRetry   = errors.New("flatpak metadata upstream allows mirror retry")
	errMetadataStateTooLarge = errors.New("flatpak metadata state exceeds size limit")
)

type generationEntry struct {
	name string
	mod  time.Time
}

type metadataDownload struct {
	temp    string
	size    int64
	digest  string
	state   metadataObjectState
	headers map[string]string
}

type metadataReadCloser struct {
	io.ReadCloser
	release func()
}

func (r *metadataReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.release()
	return err
}

func (h *Handler) Refresh(ctx context.Context) error {
	_, err := h.RefreshTask(ctx)
	return err
}

func (h *Handler) RefreshTask(ctx context.Context) (*scheduler.TaskOutcome, error) {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()
	h.mu.Lock()
	h.refreshing = true
	h.refreshQueued = false
	h.lastError = ""
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.refreshing = false
		h.mu.Unlock()
	}()
	var firstErr error
	for _, upstream := range h.upstreams {
		next, changed, err := h.refreshFromUpstream(ctx, upstream)
		if err != nil {
			var limited *httpcache.UpstreamRateLimitError
			if errors.As(err, &limited) {
				if !limited.RetryAfter.IsZero() {
					return nil, scheduler.RetryAt(limited.RetryAfter)
				}
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !errors.Is(err, errMetadataMirrorRetry) {
				h.setRefreshError(err)
				return nil, err
			}
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
	h.setRefreshError(firstErr)
	return nil, firstErr
}

func (h *Handler) setRefreshError(err error) {
	h.mu.Lock()
	if err != nil {
		h.lastError = err.Error()
	}
	h.mu.Unlock()
}

func (h *Handler) requestRefresh() {
	h.mu.Lock()
	if h.stopping || h.refreshing || h.refreshQueued {
		h.mu.Unlock()
		return
	}
	if h.triggerRefresh != nil {
		trigger := h.triggerRefresh
		h.mu.Unlock()
		trigger()
		return
	}
	if h.lifecycleCtx == nil {
		h.mu.Unlock()
		return
	}
	h.refreshQueued = true
	ctx := h.lifecycleCtx
	h.wait.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.wait.Done()
		_, _ = h.RefreshTask(ctx)
	}()
}

func (h *Handler) CleanupMetadata(ctx context.Context) error {
	entries, err := fs.ReadDir(h.store.TenantFS(h.name), metadataRoot)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read flatpak metadata generations: %w", err)
	}
	var generations []generationEntry
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat flatpak metadata generation %s: %w", entry.Name(), err)
		}
		generations = append(generations, generationEntry{name: entry.Name(), mod: info.ModTime()})
	}
	if len(generations) <= 1 {
		return nil
	}
	slices.SortFunc(generations, func(left, right generationEntry) int {
		if comparison := left.mod.Compare(right.mod); comparison != 0 {
			return comparison
		}
		return strings.Compare(left.name, right.name)
	})
	h.mu.RLock()
	current := h.current.Generation
	retained := make(map[string]struct{}, len(h.readers)+2)
	for generation, readers := range h.readers {
		if readers > 0 {
			retained[generation] = struct{}{}
		}
	}
	h.mu.RUnlock()
	if current != "" {
		retained[current] = struct{}{}
	}
	cutoff := time.Now().Add(-15 * time.Minute)
	previous := ""
	for i := len(generations) - 1; i >= 0; i-- {
		generation := generations[i]
		if generation.mod.After(cutoff) {
			retained[generation.name] = struct{}{}
		}
		if generation.name != current && previous == "" {
			previous = generation.name
		}
	}
	if previous != "" {
		retained[previous] = struct{}{}
	}
	for _, generation := range generations {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, keep := retained[generation.name]; keep {
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
	if current.Generation == "" {
		refreshCtx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
		err := h.Refresh(refreshCtx)
		cancel()
		if err != nil {
			h.serveMetadataUnavailable(w, req, err)
			return
		}
		current = h.currentSnapshot()
	} else if time.Since(current.Published) >= h.refreshInterval {
		h.requestRefresh()
	}
	if current.Generation == "" {
		h.serveMetadataUnavailable(w, req, errMetadataUnavailable)
		return
	}
	h.serveCommittedMetadata(w, req, current, "summary")
}

func (h *Handler) serveCompanionMetadata(w http.ResponseWriter, req *http.Request, cleanPath string) {
	current := h.currentSnapshot()
	if current.Generation == "" {
		h.serveMetadataUnavailable(w, req, errMetadataUnavailable)
		return
	}
	h.serveCommittedMetadata(w, req, current, cleanPath)
}

func (h *Handler) serveCommittedMetadata(w http.ResponseWriter, req *http.Request, current currentMetadata, cleanPath string) {
	object, ok := current.Manifest.Objects[cleanPath]
	if !ok {
		h.serveMetadataUnavailable(w, req, fmt.Errorf("flatpak metadata %s is absent from current generation", cleanPath))
		return
	}
	switch object.State {
	case metadataNotFound:
		w.WriteHeader(http.StatusNotFound)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "GENERATION", http.StatusNotFound, 0)
		return
	case metadataForbidden:
		w.WriteHeader(http.StatusForbidden)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "GENERATION", http.StatusForbidden, 0)
		return
	case metadataPresent:
		h.serveMetadataObject(w, req, path.Join(metadataRoot, current.Generation, cleanPath), current.Generation)
	default:
		h.serveMetadataUnavailable(w, req, errors.New("flatpak metadata manifest has invalid state"))
	}
}

func (h *Handler) serveMetadataUnavailable(w http.ResponseWriter, req *http.Request, err error) {
	w.Header().Set("Retry-After", "1")
	_ = httpcache.ErrorResponse(http.StatusServiceUnavailable, err).FlushClose(req, w)
	h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", http.StatusServiceUnavailable, 0)
}

func (h *Handler) serveMetadataObject(w http.ResponseWriter, req *http.Request, objectPath, generation string) {
	reader, err := h.store.OpenObject(req.Context(), h.name, objectPath)
	if err != nil {
		_ = httpcache.ErrorResponse(http.StatusInternalServerError, err).FlushClose(req, w)
		h.stats.RecordRequest(h.name, config.ModeFlatpak, req.Method, "ERROR", http.StatusInternalServerError, 0)
		return
	}
	h.mu.Lock()
	h.readers[generation]++
	h.mu.Unlock()
	release := func() {
		h.mu.Lock()
		h.readers[generation]--
		if h.readers[generation] == 0 {
			delete(h.readers, generation)
		}
		h.mu.Unlock()
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
	response := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: &metadataReadCloser{ReadCloser: reader, release: release}}
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
		defer item.Close()
		objects[companion] = item
	}
	for _, name := range []string{"summary", "summary.sig", "config"} {
		expected := objects[name]
		confirmed, err := h.fetchMetadata(ctx, upstream, name, name == "summary")
		if err != nil {
			return currentMetadata{}, false, err
		}
		defer confirmed.Close()
		if !sameMetadataDownload(expected, confirmed) {
			return currentMetadata{}, false, errors.New("flatpak metadata anchor changed during refresh")
		}
	}
	fingerprint := metadataFingerprint(objects)
	current := h.currentSnapshot()
	if current.Fingerprint != "" && current.Fingerprint == fingerprint {
		return current, false, nil
	}
	for name, item := range objects {
		if item.state == metadataPresent {
			if err := h.putMetadata(ctx, generation, name, item); err != nil {
				return currentMetadata{}, false, err
			}
		}
	}
	manifest := metadataManifest{
		Version: 1, Generation: generation, Upstream: upstream, Published: time.Now().UTC(),
		AnchorSetDigest: fingerprint, Objects: make(map[string]metadataManifestObject, len(objects)),
	}
	for name, item := range objects {
		manifest.Objects[name] = metadataManifestObject{State: item.state, Size: item.size, Digest: item.digest}
	}
	manifestData, err := yaml.Marshal(manifest)
	if err != nil {
		return currentMetadata{}, false, err
	}
	manifestPath := path.Join(metadataRoot, generation, "snapshot.yaml")
	if _, err := h.store.Put(ctx, h.name, manifestPath, bytes.NewReader(manifestData), map[string]string{
		"content-type": "application/yaml", "mode": config.ModeFlatpak,
	}); err != nil {
		return currentMetadata{}, false, err
	}
	next := currentMetadata{
		Generation: generation, Upstream: upstream, Published: manifest.Published,
		Fingerprint: fingerprint, SnapshotDigest: digestData(manifestData), Manifest: manifest,
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
	release, err := h.upstreamGate.Acquire(ctx, upstream, httpcache.AdmissionRefresh)
	if err != nil {
		return nil, err
	}
	targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(cleanPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		release()
		return nil, fmt.Errorf("create flatpak metadata request %s: %w", cleanPath, err)
	}
	request.Header.Set("User-Agent", h.client.UserAgent)

	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		release()
		h.stats.RecordUpstreamRequest(h.name, config.ModeFlatpak, upstream, http.MethodGet, 0, latency, 0)
		if h.serviceHealth != nil {
			h.serviceHealth.RecordFailure(upstream, err)
		}
		return nil, fmt.Errorf("%w: fetch flatpak metadata %s: %v", errMetadataMirrorRetry, cleanPath, err)
	}
	released := false
	releaseTransport := func() {
		if released {
			return
		}
		released = true
		_ = response.Body.Close()
		release()
	}
	defer releaseTransport()
	h.stats.RecordUpstreamRequest(
		h.name,
		config.ModeFlatpak,
		upstream,
		http.MethodGet,
		response.StatusCode,
		latency,
		flatpakContentLength(response),
	)
	if h.serviceHealth != nil {
		h.serviceHealth.RecordResult(upstream, response.StatusCode, latency)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return nil, h.upstreamGate.RateLimited(upstream, response.Header.Get("Retry-After"))
	}
	if response.StatusCode != http.StatusOK {
		if !required && (response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusForbidden) {
			state := metadataNotFound
			if response.StatusCode == http.StatusForbidden {
				state = metadataForbidden
			}
			return &metadataDownload{state: state}, nil
		}
		switch response.StatusCode {
		case http.StatusNotFound:
			return nil, fmt.Errorf("%w: fetch flatpak metadata %s", errMetadataUnavailable, cleanPath)
		case http.StatusForbidden:
			return nil, fmt.Errorf("%w: fetch flatpak metadata %s", errMetadataUnavailable, cleanPath)
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return nil, fmt.Errorf("%w: fetch flatpak metadata %s: HTTP %d", errMetadataMirrorRetry, cleanPath, response.StatusCode)
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
	releaseTransport()
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
	return &metadataDownload{temp: tempPath, size: size, digest: digest, state: metadataPresent, headers: headers}, nil
}

func fileDigest(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
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
	slices.Sort(names)
	sum := sha256.New()
	for _, name := range names {
		_, _ = io.WriteString(sum, name)
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, string(objects[name].state))
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, strconv.FormatInt(objects[name].size, 10))
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, objects[name].digest)
		_, _ = io.WriteString(sum, "\x00")
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil))
}

func sameMetadataDownload(left, right *metadataDownload) bool {
	return left != nil && right != nil && left.state == right.state && left.size == right.size && left.digest == right.digest
}

func digestData(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
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
	defer func() { _ = file.Close() }()
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
	defer func() { _ = file.Close() }()
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
	data, err := yaml.Marshal(metadataCurrentReference{
		Version: 1, Generation: current.Generation, SnapshotDigest: current.SnapshotDigest,
	})
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

func (h *Handler) cleanCurrentTemp(ctx context.Context) {
	err := fs.WalkDir(
		h.store.TenantFS(h.name),
		path.Dir(currentMetadataObject),
		func(objectPath string, entry fs.DirEntry, walkErr error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasPrefix(path.Base(objectPath), "current.yaml.tmp.") {
				return nil
			}
			if err := h.store.DeleteObject(ctx, h.name, objectPath); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("delete flatpak current temp %s: %w", objectPath, err)
			}
			return nil
		},
	)
	if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, context.Canceled) {
		slog.Debug("flatpak current temp cleanup failed", "instance", h.name, "err", err)
	}
}

func (h *Handler) restoreCurrent(ctx context.Context) error {
	reader, err := h.store.OpenObject(ctx, h.name, currentMetadataObject)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open flatpak current metadata: %w", err)
	}
	currentData, err := readBoundedMetadataState(reader, 1<<20)
	err = errors.Join(err, reader.Close())
	if err != nil {
		if errors.Is(err, errMetadataStateTooLarge) {
			return h.discardCurrent(ctx)
		}
		return fmt.Errorf("read flatpak current metadata: %w", err)
	}
	var ref metadataCurrentReference
	if err := decodeStrictYAML(currentData, &ref); err != nil || ref.Version != 1 || ref.Generation == "" || ref.SnapshotDigest == "" {
		return h.discardCurrent(ctx)
	}
	manifestPath := path.Join(metadataRoot, ref.Generation, "snapshot.yaml")
	manifestReader, err := h.store.OpenObject(ctx, h.name, manifestPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return h.discardCurrent(ctx)
		}
		return fmt.Errorf("open flatpak metadata manifest: %w", err)
	}
	manifestData, readErr := readBoundedMetadataState(manifestReader, 4<<20)
	readErr = errors.Join(readErr, manifestReader.Close())
	if readErr != nil {
		if errors.Is(readErr, errMetadataStateTooLarge) {
			return h.discardCurrent(ctx)
		}
		return fmt.Errorf("read flatpak metadata manifest: %w", readErr)
	}
	var manifest metadataManifest
	if digestData(manifestData) != ref.SnapshotDigest || decodeStrictYAML(manifestData, &manifest) != nil ||
		manifest.Version != 1 || manifest.Generation != ref.Generation || manifest.Upstream == "" ||
		manifest.AnchorSetDigest == "" || len(manifest.Objects) == 0 {
		return h.discardCurrent(ctx)
	}
	for name, object := range manifest.Objects {
		if name != "summary" && name != "summary.sig" && name != "config" {
			return h.discardCurrent(ctx)
		}
		if object.State == metadataPresent {
			objectPath := path.Join(metadataRoot, ref.Generation, name)
			info, statErr := h.store.StatObject(ctx, h.name, objectPath)
			if statErr != nil {
				if errors.Is(statErr, fs.ErrNotExist) {
					return h.discardCurrent(ctx)
				}
				return fmt.Errorf("stat flatpak metadata object %s: %w", name, statErr)
			}
			if info.Size != object.Size {
				return h.discardCurrent(ctx)
			}
			digest, digestErr := h.storeObjectDigest(ctx, objectPath)
			if digestErr != nil {
				if errors.Is(digestErr, fs.ErrNotExist) {
					return h.discardCurrent(ctx)
				}
				return fmt.Errorf("hash flatpak metadata object %s: %w", name, digestErr)
			}
			if digest != object.Digest {
				return h.discardCurrent(ctx)
			}
		} else if object.State != metadataNotFound && object.State != metadataForbidden {
			return h.discardCurrent(ctx)
		}
	}
	for _, name := range []string{"summary", "summary.sig", "config"} {
		if _, ok := manifest.Objects[name]; !ok {
			return h.discardCurrent(ctx)
		}
	}
	if manifestFingerprint(manifest.Objects) != manifest.AnchorSetDigest {
		return h.discardCurrent(ctx)
	}
	current := currentMetadata{Generation: manifest.Generation, Upstream: manifest.Upstream, Published: manifest.Published,
		Fingerprint: manifest.AnchorSetDigest, SnapshotDigest: ref.SnapshotDigest, Manifest: manifest}
	h.mu.Lock()
	h.current = current
	h.mu.Unlock()
	return nil
}

func (h *Handler) discardCurrent(ctx context.Context) error {
	if err := h.store.DeleteObject(ctx, h.name, currentMetadataObject); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("discard invalid flatpak current metadata: %w", err)
	}
	return nil
}

func manifestFingerprint(objects map[string]metadataManifestObject) string {
	names := make([]string, 0, len(objects))
	for name := range objects {
		names = append(names, name)
	}
	slices.Sort(names)
	sum := sha256.New()
	for _, name := range names {
		object := objects[name]
		_, _ = io.WriteString(sum, name)
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, string(object.State))
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, strconv.FormatInt(object.Size, 10))
		_, _ = io.WriteString(sum, "\x00")
		_, _ = io.WriteString(sum, object.Digest)
		_, _ = io.WriteString(sum, "\x00")
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil))
}

func decodeStrictYAML(data []byte, value any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple YAML documents")
		}
		return err
	}
	return nil
}

func readBoundedMetadataState(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: limit %d bytes", errMetadataStateTooLarge, limit)
	}
	return data, nil
}

func (h *Handler) storeObjectDigest(ctx context.Context, objectPath string) (string, error) {
	reader, err := h.store.OpenObject(ctx, h.name, objectPath)
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

func (h *Handler) currentSnapshot() currentMetadata {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.current
}
