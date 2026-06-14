package gomod

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

type upstreamProxyFetcher struct {
	client    *http.Client
	upstreams []*url.URL
}

type moduleInfo struct {
	Version string    `json:"Version"`
	Time    time.Time `json:"Time"`
}

func newUpstreamProxyFetcher(upstreams []string, transport http.RoundTripper) (*upstreamProxyFetcher, error) {
	if len(upstreams) == 0 {
		return nil, fmt.Errorf("go proxy requires at least one GOPROXY upstream: %w", fs.ErrNotExist)
	}
	parsed := make([]*url.URL, 0, len(upstreams))
	for i, raw := range upstreams {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("invalid GOPROXY upstream %d: %w", i, fs.ErrNotExist)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("unsupported GOPROXY upstream scheme %q: %w", u.Scheme, fs.ErrNotExist)
		}
		parsed = append(parsed, u)
	}
	client := &http.Client{Transport: transport}
	return &upstreamProxyFetcher{client: client, upstreams: parsed}, nil
}

func (f *upstreamProxyFetcher) Query(ctx context.Context, modulePath, query string) (string, time.Time, error) {
	query = strings.TrimSpace(query)
	switch {
	case query == "latest":
		return f.queryLatest(ctx, modulePath)
	case module.CanonicalVersion(query) == query:
		return f.queryExact(ctx, modulePath, query)
	case isSemanticPrefixQuery(query):
		return f.queryVersionPrefix(ctx, modulePath, query)
	default:
		return "", time.Time{}, unsupportedQueryError(query)
	}
}

func (f *upstreamProxyFetcher) List(ctx context.Context, modulePath string) ([]string, error) {
	escapedPath, err := module.EscapePath(modulePath)
	if err != nil {
		return nil, fmt.Errorf("invalid module path: %w", fs.ErrNotExist)
	}
	var lastErr error
	for _, upstream := range f.upstreams {
		body, status, err := f.getBytes(ctx, upstream.JoinPath(path.Join(escapedPath, "@v", "list")).String())
		if err != nil {
			if shouldFallback(status, err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		versions := parseVersionList(body)
		semver.Sort(versions)
		return versions, nil
	}
	if lastErr == nil {
		lastErr = fs.ErrNotExist
	}
	return nil, lastErr
}

func (f *upstreamProxyFetcher) Download(ctx context.Context, modulePath, version string) (io.ReadSeekCloser, io.ReadSeekCloser, io.ReadSeekCloser, error) {
	escapedPath, err := module.EscapePath(modulePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid module path: %w", fs.ErrNotExist)
	}
	escapedVersion, err := module.EscapeVersion(version)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid module version: %w", fs.ErrNotExist)
	}
	var lastErr error
	for _, upstream := range f.upstreams {
		base := upstream.JoinPath(path.Join(escapedPath, "@v"))
		info, status, err := f.getBytes(ctx, base.JoinPath(escapedVersion+".info").String())
		if err != nil {
			if shouldFallback(status, err) {
				lastErr = err
				continue
			}
			return nil, nil, nil, err
		}
		mod, status, err := f.getBytes(ctx, base.JoinPath(escapedVersion+".mod").String())
		if err != nil {
			if shouldFallback(status, err) {
				lastErr = err
				continue
			}
			return nil, nil, nil, err
		}
		zip, status, err := f.getBytes(ctx, base.JoinPath(escapedVersion+".zip").String())
		if err != nil {
			if shouldFallback(status, err) {
				lastErr = err
				continue
			}
			return nil, nil, nil, err
		}
		return newReadSeekCloser(info), newReadSeekCloser(mod), newReadSeekCloser(zip), nil
	}
	if lastErr == nil {
		lastErr = fs.ErrNotExist
	}
	return nil, nil, nil, lastErr
}

func (f *upstreamProxyFetcher) queryLatest(ctx context.Context, modulePath string) (string, time.Time, error) {
	escapedPath, err := module.EscapePath(modulePath)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("invalid module path: %w", fs.ErrNotExist)
	}
	var lastErr error
	for _, upstream := range f.upstreams {
		body, status, err := f.getBytes(ctx, upstream.JoinPath(path.Join(escapedPath, "@latest")).String())
		if err != nil {
			if shouldFallback(status, err) {
				lastErr = err
				continue
			}
			return "", time.Time{}, err
		}
		info, err := parseModuleInfo(body)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("invalid latest response: %w", fs.ErrNotExist)
		}
		return info.Version, info.Time, nil
	}
	if lastErr == nil {
		lastErr = fs.ErrNotExist
	}
	return "", time.Time{}, lastErr
}

func (f *upstreamProxyFetcher) queryExact(ctx context.Context, modulePath, version string) (string, time.Time, error) {
	info, err := f.fetchInfo(ctx, modulePath, version)
	if err != nil {
		return "", time.Time{}, err
	}
	return info.Version, info.Time, nil
}

func (f *upstreamProxyFetcher) queryVersionPrefix(ctx context.Context, modulePath, prefix string) (string, time.Time, error) {
	versions, err := f.List(ctx, modulePath)
	if err != nil {
		return "", time.Time{}, err
	}
	for i := len(versions) - 1; i >= 0; i-- {
		if hasVersionPrefix(versions[i], prefix) {
			return f.queryExact(ctx, modulePath, versions[i])
		}
	}
	return "", time.Time{}, fmt.Errorf("version query %q not found: %w", prefix, fs.ErrNotExist)
}

func (f *upstreamProxyFetcher) fetchInfo(ctx context.Context, modulePath, version string) (*moduleInfo, error) {
	escapedPath, err := module.EscapePath(modulePath)
	if err != nil {
		return nil, fmt.Errorf("invalid module path: %w", fs.ErrNotExist)
	}
	escapedVersion, err := module.EscapeVersion(version)
	if err != nil {
		return nil, fmt.Errorf("invalid module version: %w", fs.ErrNotExist)
	}
	var lastErr error
	for _, upstream := range f.upstreams {
		body, status, err := f.getBytes(ctx, upstream.JoinPath(path.Join(escapedPath, "@v", escapedVersion+".info")).String())
		if err != nil {
			if shouldFallback(status, err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		info, err := parseModuleInfo(body)
		if err != nil {
			return nil, fmt.Errorf("invalid info response: %w", fs.ErrNotExist)
		}
		return info, nil
	}
	if lastErr == nil {
		lastErr = fs.ErrNotExist
	}
	return nil, lastErr
}

func (f *upstreamProxyFetcher) getBytes(ctx context.Context, rawURL string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return nil, resp.StatusCode, fmt.Errorf("upstream object not found: %w", fs.ErrNotExist)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, resp.StatusCode, fmt.Errorf("upstream GOPROXY %s returned %d: %s", rawURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func parseVersionList(body []byte) []string {
	lines := strings.Split(string(body), "\n")
	versions := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		versions = append(versions, line)
	}
	return versions
}

func parseModuleInfo(body []byte) (*moduleInfo, error) {
	var info moduleInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	if info.Version == "" {
		return nil, fmt.Errorf("missing version")
	}
	return &info, nil
}

func shouldFallback(status int, err error) bool {
	return errors.Is(err, fs.ErrNotExist) && (status == http.StatusNotFound || status == http.StatusGone)
}

func isSemanticPrefixQuery(query string) bool {
	if !strings.HasPrefix(query, "v") || strings.ContainsAny(query, "/@") {
		return false
	}
	for _, ch := range query[1:] {
		if (ch < '0' || ch > '9') && ch != '.' {
			return false
		}
	}
	return true
}

func hasVersionPrefix(version, prefix string) bool {
	if version == prefix {
		return true
	}
	if !strings.HasPrefix(version, prefix) {
		return false
	}
	if len(version) == len(prefix) {
		return true
	}
	switch version[len(prefix)] {
	case '.', '-', '+':
		return true
	default:
		return false
	}
}

func unsupportedQueryError(query string) error {
	return fmt.Errorf("go module query %q requires direct source resolution, which this proxy disables: %w", query, fs.ErrNotExist)
}

type readSeekCloser struct {
	*bytes.Reader
}

func newReadSeekCloser(data []byte) io.ReadSeekCloser {
	return &readSeekCloser{Reader: bytes.NewReader(data)}
}

func (r *readSeekCloser) Close() error {
	return nil
}
