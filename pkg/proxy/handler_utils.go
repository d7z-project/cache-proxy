package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func redactedURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	parsed.User = nil
	return parsed.String()
}

func (h *Handler) expired(route Route, options map[string]string) bool {
	expireAfter := route.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = h.config.ExpireAfter
	}
	if expireAfter.IsNever() || expireAfter.IsUnset() {
		return false
	}
	fetchedAt, err := utils.ParseFetchedAt(options["fetched-at"])
	return err == nil && time.Since(fetchedAt) > expireAfter.Duration()
}

func (h *Handler) fresh(route Route, headers map[string]string) bool {
	freshFor := route.FreshFor
	if freshFor.IsUnset() {
		freshFor = h.config.DefaultFreshFor
	}
	if freshFor.IsUnset() {
		return false
	}
	if freshFor.IsForever() {
		return true
	}
	fetchedAt, err := utils.ParseFetchedAt(headers["fetched-at"])
	return err == nil && time.Since(fetchedAt) <= freshFor.Duration()
}

func (h *Handler) busyPolicy(route Route) string {
	if route.BusyPolicy != "" {
		return route.BusyPolicy
	}
	return h.config.BusyPolicy
}

func setContentType(headers map[string]string, objectPath string) {
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = mime.TypeByExtension(path.Ext(objectPath))
	}
	if headers["Content-Type"] == "" {
		headers["Content-Type"] = "application/octet-stream"
	}
}

func errorResponse(status int, err error) *utils.ResponseWrapper {
	return &utils.ResponseWrapper{StatusCode: status, Headers: map[string]string{"Content-Type": "text/plain; charset=utf-8", "X-Cache": "ERROR"}, Body: io.NopCloser(strings.NewReader(err.Error()))}
}

func responseFromHTTP(response *http.Response) *utils.ResponseWrapper {
	return &utils.ResponseWrapper{StatusCode: response.StatusCode, Headers: copyHeaders(response.Header), Body: response.Body}
}

func copyHeaders(headers http.Header) map[string]string {
	result := map[string]string{}
	for _, key := range []string{"Content-Type", "Content-Length", "Last-Modified", "Content-Range", "Accept-Ranges", "ETag", "Docker-Content-Digest", "Docker-Distribution-API-Version"} {
		if value := headers.Get(key); value != "" {
			result[key] = value
		}
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

func metadata(headers map[string]string, mode, status string) map[string]string {
	result := map[string]string{"mode": mode, "cache": status, "fetched-at": time.Now().UTC().Format(time.RFC3339Nano)}
	for _, key := range []string{"Content-Type", "Content-Length", "Last-Modified", "ETag", "Docker-Content-Digest"} {
		if value := headers[key]; value != "" {
			result[strings.ToLower(key)] = value
		}
	}
	return result
}

func headerName(key string) string {
	switch key {
	case "content-type":
		return "Content-Type"
	case "content-length":
		return "Content-Length"
	case "last-modified":
		return "Last-Modified"
	case "etag":
		return "ETag"
	case "docker-content-digest":
		return "Docker-Content-Digest"
	default:
		return key
	}
}

func SafePath(value string) bool {
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func EscapePath(value string) string {
	parts := strings.Split(value, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func HashKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
