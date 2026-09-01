package httpcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func (h *Handler) handleRequest(ctx context.Context, req *http.Request) (*utils.ResponseWrapper, error) {
	if !h.beginOperation() {
		return nil, errors.New("cache handler is closing")
	}
	defer h.wait.Done()
	route, err := h.resolver.Resolve(req)
	if err != nil {
		return nil, err
	}
	if route.Class == ClassUnspecified {
		return nil, errors.New("resolver did not classify request")
	}
	slog.Debug("proxy route resolved", "instance", h.name, "mode", h.config.Mode, "method", req.Method, "path", req.URL.Path, "object", route.ObjectPath, "upstream_path", route.UpstreamPath, "class", route.Class)
	if route.Class == ClassPassthrough {
		return h.bypass(ctx, req, route)
	}

	if req.Header.Get("Range") != "" {
		cached, err := h.openValidCached(ctx, req, route)
		if err == nil {
			cached.Headers["X-Cache"] = "HIT"
			return h.rewriteResponse(req, route, cached), nil
		}
		return h.bypass(ctx, req, route)
	}

	flight, leader := h.flights.begin(route.ObjectPath)
	if !leader {
		return h.followFlight(ctx, req, route, flight)
	}
	return h.handleFlightLeader(ctx, req, route, flight)
}

func (h *Handler) handleFlightLeader(ctx context.Context, req *http.Request, route Route, flight *objectFlight) (*utils.ResponseWrapper, error) {
	cached, err := h.openCached(ctx, req, route)
	if err != nil {
		slog.Debug("cache miss", "instance", h.name, "object", route.ObjectPath, "err", err)
		return h.streamDownload(req, route, "MISS", flight)
	}
	if route.Class == ClassContent {
		cached.Headers["X-Cache"] = "HIT"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, cached), nil)
	}
	if h.fresh(route, cached.Headers) {
		cached.Headers["X-Cache"] = "FRESH"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, cached), nil)
	}
	response, err := h.revalidateCached(ctx, req, route, flight, cached)
	if err != nil {
		slog.Debug("cache validation error, serving stale", "instance", h.name, "object", route.ObjectPath, "err", err)
		cached.Headers["X-Cache"] = "STALE"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, cached), nil)
	}
	return response, nil
}

func (h *Handler) followFlight(ctx context.Context, req *http.Request, route Route, flight *objectFlight) (*utils.ResponseWrapper, error) {
	if route.Class == ClassMetadata && req.Header.Get("Range") == "" {
		cached, err := h.openCached(ctx, req, route)
		if err == nil {
			cached.Headers["X-Cache"] = "STALE"
			if h.fresh(route, cached.Headers) {
				cached.Headers["X-Cache"] = "FRESH"
			}
			return h.rewriteResponse(req, route, cached), nil
		}
	}
	if response, streamed, err := flight.subscribe(ctx); streamed || err != nil {
		if response != nil {
			response.Headers["X-Cache"] = "COALESCED"
			response = h.rewriteResponse(req, route, response)
		}
		return response, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
	}
	if err := flight.resultError(); err != nil {
		return nil, err
	}
	cached, err := h.openCached(ctx, req, route)
	if err != nil {
		slog.Debug("completed cache flight did not publish object, bypass upstream", "instance", h.name, "object", route.ObjectPath, "err", err)
		return h.bypass(ctx, req, route)
	}
	cached.Headers["X-Cache"] = "HIT"
	return h.rewriteResponse(req, route, cached), nil
}

func (h *Handler) finishFlight(key string, flight *objectFlight, result *utils.ResponseWrapper, err error) (*utils.ResponseWrapper, error) {
	h.flights.finish(key, flight, err)
	return result, err
}

func (h *Handler) bypass(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	response, err := h.openRemote(
		ctx,
		req.Method,
		route.UpstreamPath,
		h.remoteOptionsForRoute(route, true, req),
		h.remoteHeaders(req, route, nil),
	)
	if response != nil {
		response.Headers["X-Cache"] = "BYPASS"
		response = h.rewriteResponse(req, route, response)
	}
	return response, err
}

func (h *Handler) openCached(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	reader, err := h.store.OpenObject(ctx, h.name, route.ObjectPath)
	if err != nil {
		return nil, err
	}
	info := reader.Info()
	if !CacheSupportsRequestUserAgent(h.client, req, info.Options) {
		_ = reader.Close()
		return nil, errors.New("cached object has unknown or incompatible User-Agent variance")
	}
	headers := map[string]string{"Content-Length": strconv.FormatInt(info.Size, 10)}
	for key, value := range info.Options {
		headers[HeaderName(key)] = value
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

func (h *Handler) openValidCached(ctx context.Context, req *http.Request, route Route) (*utils.ResponseWrapper, error) {
	cached, err := h.openCached(ctx, req, route)
	if err != nil {
		return nil, err
	}
	if route.Class == ClassContent {
		return cached, nil
	}
	if h.fresh(route, cached.Headers) {
		return cached, nil
	}
	_ = cached.Close()
	return nil, errors.New("cached object requires revalidation")
}

func (h *Handler) revalidateCached(ctx context.Context, req *http.Request, route Route, flight *objectFlight, cached *utils.ResponseWrapper) (*utils.ResponseWrapper, error) {
	headers := map[string]string{}
	if etag := cached.Headers["ETag"]; etag != "" {
		headers["If-None-Match"] = etag
	}
	if lastModified := cached.Headers["Last-Modified"]; lastModified != "" {
		headers["If-Modified-Since"] = lastModified
	}
	options := h.remoteOptionsForRoute(route, true, req)
	options.DisableFailover = true
	options.ValidatorOrigin = cached.Headers[responseSourceUpstreamHeader]
	resp, err := h.openRemote(
		h.lifecycleCtx,
		http.MethodGet,
		route.UpstreamPath,
		options,
		h.remoteHeaders(req, route, headers),
	)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusNotModified:
		_ = resp.Close()
		if err := h.markRevalidated(h.lifecycleCtx, route, resp.Headers); err != nil {
			return nil, err
		}
		cached.Headers["X-Cache"] = "REVALIDATED"
		h.addCacheDebugHeaders(cached.Headers, route, time.Now().UTC().Format(time.RFC3339Nano))
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, cached), nil)
	case http.StatusOK:
		if !h.client.UserAgentConfigured && utils.VariesByUserAgent(resp.Headers["Vary"]) {
			_ = cached.Close()
			resp.Headers["X-Cache"] = "BYPASS"
			return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, resp), nil)
		}
		response, streamErr := h.streamResponse(req, route, "REFRESH", flight, resp)
		if streamErr == nil {
			_ = cached.Close()
		}
		return response, streamErr
	case http.StatusNotFound, http.StatusGone:
		_ = cached.Close()
		_ = h.store.DeleteObject(h.lifecycleCtx, h.name, route.ObjectPath)
		resp.Headers["X-Cache"] = "BYPASS"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, resp), nil)
	default:
		_ = resp.Close()
		return nil, fmt.Errorf("%w: conditional GET failed with %d", ErrUpstreamUnavailable, resp.StatusCode)
	}
}

func (h *Handler) markRevalidated(ctx context.Context, route Route, responseHeaders map[string]string) error {
	info, err := h.store.StatObject(ctx, h.name, route.ObjectPath)
	if err != nil {
		return err
	}
	options := copyHeadersMap(info.Options)
	options["fetched-at"] = time.Now().UTC().Format(time.RFC3339Nano)
	for _, key := range []string{"ETag", "Last-Modified", "Vary"} {
		if value := responseHeaders[key]; value != "" {
			options[strings.ToLower(key)] = value
		}
	}
	_, err = h.store.UpdateMetadata(ctx, h.name, route.ObjectPath, options)
	return err
}

func (h *Handler) streamDownload(req *http.Request, route Route, status string, flight *objectFlight) (*utils.ResponseWrapper, error) {
	resp, err := h.openRemote(
		h.lifecycleCtx,
		http.MethodGet,
		route.UpstreamPath,
		h.remoteOptionsForRoute(route, true, req),
		h.remoteHeaders(req, route, nil),
	)
	if err != nil {
		return h.finishFlight(route.ObjectPath, flight, nil, err)
	}
	return h.streamResponse(req, route, status, flight, resp)
}

func (h *Handler) streamResponse(req *http.Request, route Route, status string, flight *objectFlight, resp *utils.ResponseWrapper) (*utils.ResponseWrapper, error) {
	fillCtx := h.lifecycleCtx
	if resp.StatusCode != http.StatusOK {
		resp.Headers["X-Cache"] = "BYPASS"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, resp), nil)
	}
	if !h.client.UserAgentConfigured && utils.VariesByUserAgent(resp.Headers["Vary"]) {
		resp.Headers["X-Cache"] = "BYPASS"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, resp), nil)
	}
	slog.Debug("downloading from upstream", "instance", h.name, "mode", h.config.Mode, "object", route.ObjectPath, "status", status)

	if parent := path.Dir(route.ObjectPath); parent != "." {
		if err := h.store.MkdirAll(h.name+"/"+parent, 0o755); err != nil {
			slog.Warn("cache path preparation failed; serving upstream response", "instance", h.name, "object", route.ObjectPath, "err", err)
			resp.Headers["X-Cache"] = "BYPASS"
			return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, resp), nil)
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

	spool, err := startCacheStream(fillCtx, StreamConfig{
		Body:       resp.Body,
		ObjectPath: route.ObjectPath,
		Wait:       &h.wait,
		StatsStart: func() { h.stats.AddActiveDownload(h.name, h.config.Mode, 1) },
		StatsDone:  func() { h.stats.AddActiveDownload(h.name, h.config.Mode, -1) },
		Done: func(fillErr error) {
			h.flights.finish(route.ObjectPath, flight, fillErr)
		},
		VerifyFn: func(r io.ReadSeeker) error {
			if h.config.VerifyFunc == nil {
				return nil
			}
			return h.config.VerifyFunc(req, route, r)
		},
		StoreFn: func(ctx context.Context, r io.Reader) error {
			_, err := h.store.Put(ctx, h.name, route.ObjectPath, r, meta)
			return err
		},
	})
	if err != nil {
		slog.Warn("cache stream preparation failed; serving upstream response", "instance", h.name, "object", route.ObjectPath, "err", err)
		resp.Headers["X-Cache"] = "BYPASS"
		return h.finishFlight(route.ObjectPath, flight, h.rewriteResponse(req, route, resp), nil)
	}
	reader, err := spool.Reader()
	if err != nil {
		return h.finishFlight(route.ObjectPath, flight, nil, err)
	}

	headers := map[string]string{"X-Cache": status}
	for key, value := range meta {
		headers[HeaderName(key)] = value
	}
	setContentType(headers, route.ObjectPath)
	h.addCacheDebugHeaders(headers, route, meta["fetched-at"])
	response := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: headers, Body: reader}
	if flight != nil {
		flight.publish(spool, response)
	}
	return h.rewriteResponse(req, route, response), nil
}

func (h *Handler) remoteOptionsForRoute(route Route, record bool, req *http.Request) remoteOptions {
	userAgent, _ := h.client.RequestUserAgent(req)
	return remoteOptions{
		AcceptErrors:       true,
		Record:             record,
		UserAgent:          userAgent,
		TargetURL:          route.TargetURL,
		AllowedTargetHosts: route.AllowedTargetHosts,
		PreferredUpstream:  route.PreferredUpstream,
		AdmissionContext:   req.Context(),
	}
}

func (h *Handler) rewriteResponse(req *http.Request, route Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
	if h.config.ResponseTransform != nil {
		return h.config.ResponseTransform(req, route, response)
	}
	return response
}
