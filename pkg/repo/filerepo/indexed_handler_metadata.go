package filerepo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *IndexedHandler) tryServeMetadata(w http.ResponseWriter, req *http.Request, cleanPath string) bool {
	snapshot := h.currentSnapshot()
	if snapshot == nil {
		return false
	}
	obj, ok := snapshot.Metadata[cleanPath]
	if !ok {
		return false
	}
	if obj.Path != cleanPath {
		http.Redirect(w, req, req.Header.Get("X-Cache-Proxy-Prefix")+"/"+obj.Path, http.StatusFound)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusFound, 0)
		return true
	}
	reader, err := h.store.OpenObject(req.Context(), h.name, obj.StorePath)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		h.stats.RecordRequest(h.name, h.mode, req.Method, "ERROR", http.StatusInternalServerError, 0)
		return true
	}
	size := reader.Info().Size
	headers := map[string]string{
		"Content-Length": strconv.FormatInt(size, 10),
		"X-Cache":        "GENERATION",
	}
	for key, value := range reader.Info().Options {
		headers[httpcache.HeaderName(key)] = value
	}
	httpcache.StripInternal(headers)
	result := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}
	result.FlushClose(req, w)
	h.stats.RecordRequest(h.name, h.mode, req.Method, "GENERATION", http.StatusOK, uint64(size))
	return true
}

func (h *IndexedHandler) fetchMetadataObject(ctx context.Context, rootKey, generation, upstream, cleanPath string) (MetadataBlob, error) {
	targetURL := strings.TrimRight(upstream, "/") + "/" + httpcache.EscapePath(cleanPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return MetadataBlob{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)

	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, 0)
		if h.sh != nil {
			h.sh.RecordFailure(upstream, err)
		}
		return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: fmt.Errorf("fetch %s: %w", targetURL, err)}
	}
	defer response.Body.Close()
	response.Body = utils.NewContextReadCloser(ctx, h.client.WrapBody(response.Body))
	h.stats.RecordUpstream(h.name, h.mode, http.MethodGet, response.StatusCode)
	if h.sh != nil {
		h.sh.RecordResult(upstream, response.StatusCode, latency)
	}
	if response.StatusCode != http.StatusOK {
		switch response.StatusCode {
		case http.StatusNotFound, http.StatusGone:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataNotFound}
		case http.StatusUnauthorized, http.StatusForbidden:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: errMetadataForbidden}
		default:
			return MetadataBlob{}, MetadataFetchError{Path: cleanPath, Err: fmt.Errorf("HTTP %d from upstream: %w", response.StatusCode, errMetadataTransient)}
		}
	}
	tempFile, size, err := utils.TempFileFromReader(io.LimitReader(utils.NewRateLimitReader(response.Body), maxMetadataObjectSize+1))
	if err != nil {
		return MetadataBlob{}, err
	}
	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if size > maxMetadataObjectSize {
		return MetadataBlob{}, fmt.Errorf("%s: metadata object exceeds %d bytes", cleanPath, maxMetadataObjectSize)
	}
	headers := map[string]string{}
	for key, value := range response.Header {
		if len(value) > 0 {
			headers[http.CanonicalHeaderKey(key)] = value[0]
		}
	}
	if err := h.putMetadataObject(ctx, rootKey, generation, cleanPath, tempFile, size, headers); err != nil {
		return MetadataBlob{}, err
	}
	if err := tempFile.Close(); err != nil {
		return MetadataBlob{}, err
	}
	cleanupTemp = false
	return MetadataBlob{Path: cleanPath, temp: tempPath, Headers: headers}, nil
}

func (h *IndexedHandler) putMetadataObject(ctx context.Context, rootKey, generation, cleanPath string, body io.ReadSeeker, size int64, headers map[string]string) error {
	objectPath := h.generationMetadataPath(rootKey, generation, cleanPath)
	meta := map[string]string{
		"content-type":   headers["Content-Type"],
		"content-length": headers["Content-Length"],
		"last-modified":  headers["Last-Modified"],
		"etag":           headers["Etag"],
		"fetched-at":     time.Now().UTC().Format(time.RFC3339Nano),
		"mode":           h.mode,
		"cache":          "GENERATION",
	}
	if meta["content-length"] == "" {
		meta["content-length"] = strconv.FormatInt(size, 10)
	}
	if parent := path.Dir(objectPath); parent != "." {
		if err := h.store.MkdirAll(path.Join(h.name, parent), 0o755); err != nil {
			return err
		}
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := h.store.Put(ctx, h.name, objectPath, body, meta)
	if _, seekErr := body.Seek(0, io.SeekStart); seekErr != nil && err == nil {
		err = seekErr
	}
	return err
}

func (h *IndexedHandler) publishSnapshot(ctx context.Context, snapshot *LiveSnapshot) error {
	data, err := yaml.Marshal(snapshot)
	if err != nil {
		return err
	}
	snapshotPath := h.snapshotPath(snapshot.RootKey, snapshot.Generation)
	currentPath := h.currentPath(snapshot.RootKey)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(snapshotPath)), 0o755); err != nil {
		return err
	}
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(currentPath)), 0o755); err != nil {
		return err
	}
	if _, err = h.store.Put(ctx, h.name, snapshotPath, bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		return err
	}
	refData, err := yaml.Marshal(struct {
		RootKey    string `yaml:"root_key"`
		Generation string `yaml:"generation"`
	}{RootKey: snapshot.RootKey, Generation: snapshot.Generation})
	if err != nil {
		return err
	}
	tmpPath := currentPath + ".tmp." + snapshot.Generation
	if _, err = h.store.Put(ctx, h.name, tmpPath, bytes.NewReader(refData), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		return err
	}
	return h.store.Rename(path.Join(h.name, tmpPath), path.Join(h.name, currentPath))
}
