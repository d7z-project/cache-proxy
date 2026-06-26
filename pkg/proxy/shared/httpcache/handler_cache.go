package httpcache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strconv"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *Handler) handle(ctx context.Context, req *http.Request) (*utils.ResponseWrapper, error) {
	h.wait.Add(1)
	defer h.wait.Done()
	route, err := h.resolver.Resolve(req)
	if err != nil {
		return nil, err
	}
	slog.Debug("proxy route resolved", "instance", h.name, "mode", h.config.Mode, "method", req.Method, "path", req.URL.Path, "object", route.ObjectPath, "upstream_path", route.UpstreamPath, "policy", route.Policy)
	if route.Policy == config.PolicyBypass {
		return h.bypass(ctx, req, route)
	}

	lock := h.locks.Get(route.ObjectPath)
	if !lock.TryLock() {
		return h.lockBusy(ctx, req, route)
	}
	defer lock.Unlock()

	if req.Header.Get("Range") != "" {
		cached, err := h.openValidCached(ctx, route)
		if err == nil {
			cached.Headers["X-Cache"] = "HIT"
			return h.rewriteResponse(req, route, cached), nil
		}
		return h.bypass(ctx, req, route)
	}

	if _, downloading := h.downloads.Load(route.ObjectPath); downloading {
		return h.lockBusy(ctx, req, route)
	}

	cached, err := h.openCached(ctx, route)
	if err != nil {
		slog.Debug("cache miss", "instance", h.name, "object", route.ObjectPath, "err", err)
		return h.streamDownload(ctx, req, route, "MISS")
	}
	if route.Policy == config.PolicyImmutable {
		cached.Headers["X-Cache"] = "HIT"
		return h.rewriteResponse(req, route, cached), nil
	}
	if h.fresh(route, cached.Headers) {
		cached.Headers["X-Cache"] = "FRESH"
		return h.rewriteResponse(req, route, cached), nil
	}
	valid, err := h.validateCached(ctx, route, cached.Headers)
	if err != nil {
		_ = cached.Close()
		return ErrorResponse(http.StatusBadGateway, err), nil
	}
	if valid {
		cached.Headers["X-Cache"] = "HIT"
		return h.rewriteResponse(req, route, cached), nil
	}
	_ = cached.Close()
	slog.Debug("cache stale", "instance", h.name, "object", route.ObjectPath)
	return h.streamDownload(ctx, req, route, "REFRESH")
}

func (h *Handler) lockBusy(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	if h.busyPolicy(route) == config.BusyPolicyStale && req.Header.Get("Range") == "" {
		cached, err := h.openCached(ctx, route)
		if err == nil {
			cached.Headers["X-Cache"] = "STALE"
			if route.Policy == config.PolicyImmutable {
				cached.Headers["X-Cache"] = "HIT"
			}
			if h.fresh(route, cached.Headers) {
				cached.Headers["X-Cache"] = "FRESH"
			}
			return h.rewriteResponse(req, route, cached), nil
		}
	}
	slog.Debug("cache lock busy, bypass upstream", "instance", h.name, "object", route.ObjectPath)
	return h.bypass(ctx, req, route)
}

func (h *Handler) bypass(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	response, err := h.openRemote(ctx, req.Method, route.UpstreamPath, remoteOptions{AcceptErrors: true, Record: true, TargetURL: route.TargetURL}, h.remoteHeaders(req, route, nil))
	if response != nil {
		response.Headers["X-Cache"] = "BYPASS"
		response = h.rewriteResponse(req, route, response)
	}
	return response, err
}

func (h *Handler) openCached(ctx context.Context, route Route) (*utils.ResponseWrapper, error) {
	reader, err := h.store.OpenObject(ctx, h.name, route.ObjectPath)
	if err != nil {
		return nil, err
	}
	info := reader.Info()
	headers := map[string]string{"Content-Length": strconv.FormatInt(info.Size, 10)}
	for key, value := range info.Options {
		headers[headerName(key)] = value
	}
	if h.expired(route, info.Options) {
		_ = reader.Close()
		_ = h.store.DeleteObject(ctx, h.name, route.ObjectPath)
		return nil, errors.New("cached object expired")
	}
	setContentType(headers, route.ObjectPath)
	h.addCacheDebugHeaders(headers, route, info.Options["fetched-at"])
	return &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}, nil
}

func (h *Handler) openValidCached(ctx context.Context, route Route) (*utils.ResponseWrapper, error) {
	cached, err := h.openCached(ctx, route)
	if err != nil {
		return nil, err
	}
	if route.Policy == config.PolicyImmutable {
		return cached, nil
	}
	if h.fresh(route, cached.Headers) {
		return cached, nil
	}
	valid, err := h.validateCached(ctx, route, cached.Headers)
	if err != nil || !valid {
		_ = cached.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("cached object is invalid")
	}
	return cached, nil
}

func (h *Handler) validateCached(ctx context.Context, route Route, cached map[string]string) (bool, error) {
	headers := map[string]string{}
	if etag := cached["ETag"]; etag != "" {
		headers["If-None-Match"] = etag
	}
	if lastModified := cached["Last-Modified"]; lastModified != "" {
		headers["If-Modified-Since"] = lastModified
	}
	resp, err := h.openRemote(ctx, http.MethodHead, route.UpstreamPath, remoteOptions{AcceptErrors: true, TargetURL: route.TargetURL}, h.remoteHeaders(nil, route, headers))
	if err != nil {
		return false, err
	}
	defer resp.Close()
	switch resp.StatusCode {
	case http.StatusNotModified:
		return true, nil
	case http.StatusOK:
		if cached["ETag"] != "" && resp.Headers["ETag"] == cached["ETag"] {
			return true, nil
		}
		return cached["ETag"] == "" && resp.Headers["Last-Modified"] == cached["Last-Modified"] && resp.Headers["Content-Length"] == cached["Content-Length"], nil
	case http.StatusNotFound, http.StatusGone:
		_ = h.store.DeleteObject(ctx, h.name, route.ObjectPath)
		return false, nil
	default:
		if resp.StatusCode >= 500 {
			return false, fmt.Errorf("upstream HEAD failed with %d", resp.StatusCode)
		}
		return false, nil
	}
}

func (h *Handler) streamDownload(ctx context.Context, req *http.Request, route Route, status string) (*utils.ResponseWrapper, error) {
	resp, err := h.openRemote(ctx, http.MethodGet, route.UpstreamPath, remoteOptions{AcceptErrors: true, Record: true, TargetURL: route.TargetURL}, h.remoteHeaders(req, route, nil))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Headers["X-Cache"] = "BYPASS"
		return h.rewriteResponse(req, route, resp), nil
	}

	if parent := path.Dir(route.ObjectPath); parent != "." {
		if err = h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			resp.Close()
			return nil, err
		}
	}

	meta := metadata(resp.Headers, h.config.Mode, status)
	if h.config.MetadataFunc != nil {
		for key, value := range h.config.MetadataFunc(req, route, copyHeadersMap(resp.Headers), status) {
			if value != "" {
				meta[key] = value
			}
		}
	}

	_, pr, err := StreamToPipe(ctx, StreamConfig{
		Body:       resp.Body,
		ObjectPath: route.ObjectPath,
		Downloads:  &h.downloads,
		Wait:       &h.wait,
		StatsStart: func() { h.stats.AddActiveDownload(h.name, h.config.Mode, 1) },
		StatsDone:  func() { h.stats.AddActiveDownload(h.name, h.config.Mode, -1) },
		StoreFn: func(ctx context.Context, r io.Reader) error {
			_, err := h.store.Put(ctx, h.name, route.ObjectPath, r, meta)
			return err
		},
	})
	if err != nil {
		return nil, err
	}

	headers := map[string]string{"X-Cache": status}
	for key, value := range meta {
		headers[headerName(key)] = value
	}
	setContentType(headers, route.ObjectPath)
	h.addCacheDebugHeaders(headers, route, meta["fetched-at"])
	return &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: pr}, nil
}

func (h *Handler) rewriteResponse(req *http.Request, route Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
	if route.RewriteKind == "" || req.Method == http.MethodHead || response.Body == nil {
		return response
	}
	const maxRewriteBody = 50 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRewriteBody+1))
	_ = response.Body.Close()
	if err != nil {
		return ErrorResponse(http.StatusBadGateway, err)
	}
	if len(body) > maxRewriteBody {
		return ErrorResponse(http.StatusBadGateway, errors.New("response body too large to rewrite"))
	}
	switch route.RewriteKind {
	case "npm-metadata":
		var document any
		if err := json.Unmarshal(body, &document); err != nil {
			response.Body = io.NopCloser(bytes.NewReader(body))
			return response
		}
		if RewriteNPMTarballs(document, h.config.Upstreams, publicBaseURL(req)) {
			body, err = json.Marshal(document)
			if err != nil {
				return ErrorResponse(http.StatusBadGateway, err)
			}
			response.Headers["Content-Length"] = strconv.Itoa(len(body))
		}
	case "cargo-config":
		body, err = rewriteCargoConfig(req, body, route.AuthRequired)
		if err != nil {
			return ErrorResponse(http.StatusBadGateway, err)
		}
		response.Headers["Content-Type"] = "application/json"
		response.Headers["Content-Length"] = strconv.Itoa(len(body))
	case "pypi-simple":
		body, response.Headers, err = rewritePyPISimple(req, h.config.Upstreams, route, response.Headers, body)
		if err != nil {
			return ErrorResponse(http.StatusBadGateway, err)
		}
		response.Headers["Content-Length"] = strconv.Itoa(len(body))
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response
}
