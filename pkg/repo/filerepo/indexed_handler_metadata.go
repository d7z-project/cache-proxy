package filerepo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type metadataFetchRequest struct {
	rootID         string
	generation     string
	upstream       string
	requestPath    string
	objectPath     string
	verify         bool
	expectedSize   int64
	expectedSHA256 string
}

func (h *IndexedHandler) fetchMetadataObject(ctx context.Context, spec metadataFetchRequest) (MetadataBlob, error) {
	targetURL := strings.TrimRight(spec.upstream, "/") + "/" + httpcache.EscapePath(spec.requestPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return MetadataBlob{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)

	release, err := h.upstreamGate.Acquire(ctx, spec.upstream, httpcache.AdmissionRefresh)
	if err != nil {
		return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: err}
	}
	start := time.Now()
	response, err := h.client.Do(request)
	latency := time.Since(start)
	if err != nil {
		release()
		h.stats.RecordUpstreamRequest(h.name, h.mode, spec.upstream, http.MethodGet, 0, latency, 0)
		if ctx.Err() != nil {
			return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: ctx.Err()}
		}
		if h.serviceHealth != nil {
			h.serviceHealth.RecordFailure(spec.upstream, err)
		}
		return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: fmt.Errorf("%w: fetch %s: %v", errMetadataMirrorRetry, targetURL, err)}
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
	response.Body = utils.NewContextReadCloser(ctx, h.client.WrapBody(response.Body))
	h.stats.RecordUpstreamRequest(
		h.name,
		h.mode,
		spec.upstream,
		http.MethodGet,
		response.StatusCode,
		latency,
		metadataContentLength(response),
	)
	if h.serviceHealth != nil {
		h.serviceHealth.RecordResult(spec.upstream, response.StatusCode, latency)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: h.upstreamGate.RateLimited(spec.upstream, response.Header.Get("Retry-After"))}
	}
	if response.StatusCode != http.StatusOK {
		switch response.StatusCode {
		case http.StatusNotFound:
			return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: errMetadataNotFound}
		case http.StatusForbidden:
			return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: errMetadataForbidden}
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: fmt.Errorf("%w: HTTP %d", errMetadataMirrorRetry, response.StatusCode)}
		default:
			return MetadataBlob{}, MetadataFetchError{Path: spec.requestPath, Err: fmt.Errorf("http %d from upstream: %w", response.StatusCode, errMetadataTransient)}
		}
	}
	tempFile, size, err := utils.TempFileFromReader(io.LimitReader(utils.NewRateLimitReader(response.Body), maxMetadataObjectSize+1))
	releaseTransport()
	if err != nil {
		return MetadataBlob{}, err
	}
	_ = response.Body.Close()
	release()
	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if size > maxMetadataObjectSize {
		return MetadataBlob{}, fmt.Errorf("%s: metadata object exceeds %d bytes", spec.requestPath, maxMetadataObjectSize)
	}
	if spec.verify && size != spec.expectedSize {
		return MetadataBlob{}, fmt.Errorf("%s: metadata size mismatch: got %d, want %d", spec.requestPath, size, spec.expectedSize)
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return MetadataBlob{}, err
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, tempFile); err != nil {
		return MetadataBlob{}, err
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if spec.expectedSHA256 != "" {
		if !strings.EqualFold(actual, spec.expectedSHA256) {
			return MetadataBlob{}, fmt.Errorf("%s: metadata SHA256 mismatch", spec.requestPath)
		}
	}
	headers := map[string]string{}
	for key, value := range response.Header {
		if len(value) > 0 {
			headers[http.CanonicalHeaderKey(key)] = value[0]
		}
	}
	if err := h.putMetadataObject(ctx, spec.rootID, spec.generation, spec.objectPath, tempFile, size, headers); err != nil {
		return MetadataBlob{}, err
	}
	if err := tempFile.Close(); err != nil {
		return MetadataBlob{}, err
	}
	cleanupTemp = false
	return MetadataBlob{Path: spec.objectPath, temp: tempPath, Headers: headers, Size: size, Digest: actual}, nil
}

func (h *IndexedHandler) openVerifiedStagingObject(
	ctx context.Context,
	rootID, generation, objectPath string,
	expectedSize int64,
	expectedSHA256 string,
) (MetadataBlob, bool) {
	storePath := h.generationMetadataPath(rootID, generation, objectPath)
	reader, err := h.store.OpenObject(ctx, h.name, storePath)
	if err != nil {
		return MetadataBlob{}, false
	}
	info := reader.Info()
	if info.Size != expectedSize {
		_ = reader.Close()
		_ = h.store.DeleteObject(ctx, h.name, storePath)
		return MetadataBlob{}, false
	}
	tempFile, size, err := utils.TempFileFromReader(reader)
	_ = reader.Close()
	if err != nil {
		return MetadataBlob{}, false
	}
	tempPath := tempFile.Name()
	valid := false
	defer func() {
		_ = tempFile.Close()
		if !valid {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return MetadataBlob{}, false
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, tempFile); err != nil {
		return MetadataBlob{}, false
	}
	digest := hex.EncodeToString(sum.Sum(nil))
	if !strings.EqualFold(digest, expectedSHA256) {
		_ = h.store.DeleteObject(ctx, h.name, storePath)
		return MetadataBlob{}, false
	}
	valid = true
	return MetadataBlob{Path: objectPath, temp: tempPath, Size: size, Digest: digest}, true
}

func metadataContentLength(response *http.Response) uint64 {
	if response == nil || response.ContentLength <= 0 {
		return 0
	}
	return uint64(response.ContentLength)
}

func (h *IndexedHandler) putMetadataObject(ctx context.Context, rootID, generation, cleanPath string, body io.ReadSeeker, size int64, headers map[string]string) error {
	objectPath := h.generationMetadataPath(rootID, generation, cleanPath)
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

func (h *IndexedHandler) publishSnapshot(ctx context.Context, snapshot *LiveSnapshot, cleanupPaths []string) error {
	data, err := yaml.Marshal(snapshot)
	if err != nil {
		return err
	}
	snapshotPath := h.snapshotPath(snapshot.RootID, snapshot.Generation)
	cleanupPath := h.cleanupIndexPath(snapshot.RootID, snapshot.Generation)
	currentPath := h.currentPath(snapshot.RootID)
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(snapshotPath)), 0o755); err != nil {
		return err
	}
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(cleanupPath)), 0o755); err != nil {
		return err
	}
	if err := h.store.MkdirAll(path.Join(h.name, path.Dir(currentPath)), 0o755); err != nil {
		return err
	}
	cleanupData := bytes.Buffer{}
	writer := bufio.NewWriter(&cleanupData)
	for _, item := range cleanupPaths {
		item = strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(item)), "/")
		if item == "." || item == "" || !httpcache.SafePath(item) {
			continue
		}
		if _, err := writer.WriteString(item + "\n"); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if _, err = h.store.Put(ctx, h.name, cleanupPath, bytes.NewReader(cleanupData.Bytes()), map[string]string{
		"content-type": "text/plain; charset=utf-8",
		"mode":         h.mode,
	}); err != nil {
		return err
	}
	if _, err = h.store.Put(ctx, h.name, snapshotPath, bytes.NewReader(data), map[string]string{
		"content-type": "application/yaml",
		"mode":         h.mode,
	}); err != nil {
		return err
	}
	refData, err := yaml.Marshal(struct {
		RootID     string `yaml:"root_id"`
		Generation string `yaml:"generation"`
	}{RootID: snapshot.RootID, Generation: snapshot.Generation})
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
