package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const objectTenant = "objects"

type handlerConfig struct {
	name        string
	upstream    string
	passHeaders []string
	rules       []Rule
	workDir     string
	store       *blobfs.Store
	client      *transport.Client
	stats       *metrics.Stats
}

type handler struct {
	name        string
	origin      *url.URL
	passHeaders []string
	rules       []Rule
	spooler     *storeio.Spooler
	store       *blobfs.Store
	client      *transport.Client
	stats       *metrics.Stats
	lifecycle   *storeio.Lifecycle
	flights     storeio.FlightGroup
}

func newHandler(cfg handlerConfig) (*handler, error) {
	origin, err := url.Parse(cfg.upstream)
	if err != nil {
		return nil, fmt.Errorf("parse file upstream: %w", err)
	}
	return &handler{
		name:        cfg.name,
		origin:      origin,
		passHeaders: cfg.passHeaders,
		rules:       cfg.rules,
		spooler:     cfg.client.EnsureSpooler(cfg.workDir),
		store:       cfg.store,
		client:      cfg.client,
		stats:       cfg.stats,
		lifecycle:   storeio.NewLifecycle(),
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		h.stats.RecordRequest(h.name, "file", request.Method, "REJECTED", http.StatusMethodNotAllowed, 0)
		return
	}
	status, cacheResult := h.serve(w, request)
	h.stats.RecordRequest(h.name, "file", request.Method, cacheResult, status, 0)
}

func (h *handler) serve(w http.ResponseWriter, request *http.Request) (int, string) {
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid file path", http.StatusBadRequest)
		return http.StatusBadRequest, "ERROR"
	}
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}
	policy := h.policy(cleaned)
	if policy == "passthrough" || request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}

	key := cacheKey(h.origin, cleaned, request)
	if cached, err := storeio.OpenResponse(request.Context(), h.store, objectTenant, key); err == nil {
		fresh := policy == "immutable" || responseFresh(cached.Header, cached.Fetched)
		if transport.RequestForcesRevalidation(request) {
			fresh = false
		}
		if fresh {
			status := serveStored(w, request, cached, "HIT")
			return status, "HIT"
		}
		status, result, handled := h.revalidate(w, request, cleaned, key, cached)
		if handled {
			return status, result
		}
	}
	if request.Method == http.MethodHead {
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}
	return h.fill(w, request, cleaned)
}

func (h *handler) policy(cleaned string) string {
	selected := "passthrough"
	for _, rule := range h.rules {
		if doublestar.MatchUnvalidated(rule.Match, cleaned) {
			selected = rule.Policy
		}
	}
	return selected
}

func (h *handler) revalidate(w http.ResponseWriter, request *http.Request, cleaned, key string, cached *storeio.ResponseObject) (int, string, bool) {
	defer func() { _ = cached.Reader.Close() }()
	origin, err := url.Parse(cached.Origin)
	if err != nil {
		return 0, "", false
	}
	target, err := transport.JoinURL(origin, transport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		return 0, "", false
	}
	flight, leader := h.flights.Begin(key)
	if !leader {
		if err := h.flights.Wait(request.Context(), flight); err != nil && request.Context().Err() != nil {
			return localError(w, http.StatusGatewayTimeout), "ERROR", true
		}
		if updated, openErr := storeio.OpenResponse(request.Context(), h.store, objectTenant, key); openErr == nil {
			return serveStored(w, request, updated, "COALESCED"), "COALESCED", true
		}
		return serveStored(w, request, cached, "STALE"), "STALE", true
	}
	upstreamRequest, err := http.NewRequestWithContext(h.lifecycle.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		h.flights.Finish(key, flight, err)
		return 0, "", false
	}
	h.copyRequestHeaders(upstreamRequest.Header, request.Header)
	transport.SanitizeReadRequestHeaders(upstreamRequest.Header)
	if etag := cached.Header.Get("ETag"); etag != "" {
		upstreamRequest.Header.Set("If-None-Match", etag)
	}
	if modified := cached.Header.Get("Last-Modified"); modified != "" {
		upstreamRequest.Header.Set("If-Modified-Since", modified)
	}
	response, err := h.client.DoRead(h.lifecycle.Context(), upstreamRequest, transport.AdmissionForeground)
	if err != nil {
		h.flights.Finish(key, flight, err)
		status := serveStored(w, request, cached, "STALE")
		return status, "STALE", true
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		if err := storeio.TouchResponse(h.lifecycle.Context(), h.store, objectTenant, key, response.Header); err == nil {
			for _, name := range []string{"Cache-Control", "Expires", "ETag", "Last-Modified", "Vary"} {
				if values := response.Header.Values(name); len(values) > 0 {
					cached.Header[name] = append([]string(nil), values...)
				}
			}
		}
		h.flights.Finish(key, flight, nil)
		status := serveStored(w, request, cached, "REVALIDATED")
		return status, "REVALIDATED", true
	}
	if response.StatusCode >= 500 {
		_ = response.Body.Close()
		h.flights.Finish(key, flight, fmt.Errorf("upstream returned %d", response.StatusCode))
		status := serveStored(w, request, cached, "STALE")
		return status, "STALE", true
	}
	_ = cached.Reader.Close()
	if response.StatusCode != http.StatusOK || !cacheable(request, response) {
		h.flights.Finish(key, flight, nil)
		status := transport.WriteResponse(w, request, response, "BYPASS")
		return status, "BYPASS", true
	}
	return h.streamAndStoreFlight(w, request, key, origin.String(), response, "REFRESH", flight), "REFRESH", true
}

func (h *handler) fill(w http.ResponseWriter, request *http.Request, cleaned string) (int, string) {
	key := cacheKey(h.origin, cleaned, request)
	flight, leader := h.flights.Begin(key)
	if !leader {
		waitErr := h.flights.Wait(request.Context(), flight)
		if request.Context().Err() != nil {
			return localError(w, http.StatusGatewayTimeout), "ERROR"
		}
		if waitErr == nil {
			if cached, err := storeio.OpenResponse(request.Context(), h.store, objectTenant, key); err == nil {
				return serveStored(w, request, cached, "COALESCED"), "COALESCED"
			}
		}
		return localError(w, http.StatusBadGateway), "ERROR"
	}
	if cached, err := storeio.OpenResponse(request.Context(), h.store, objectTenant, key); err == nil {
		h.flights.Finish(key, flight, nil)
		return serveStored(w, request, cached, "HIT"), "HIT"
	}
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		h.flights.Finish(key, flight, err)
		return localError(w, http.StatusBadGateway), "ERROR"
	}
	upstreamRequest, err := http.NewRequestWithContext(h.lifecycle.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		h.flights.Finish(key, flight, err)
		return localError(w, http.StatusBadGateway), "ERROR"
	}
	h.copyRequestHeaders(upstreamRequest.Header, request.Header)
	transport.SanitizeReadRequestHeaders(upstreamRequest.Header)
	response, err := h.client.DoRead(h.lifecycle.Context(), upstreamRequest, transport.AdmissionForeground)
	if err != nil {
		h.flights.Finish(key, flight, err)
		return localError(w, http.StatusBadGateway), "ERROR"
	}
	if response.StatusCode != http.StatusOK || !cacheable(request, response) {
		h.flights.Finish(key, flight, nil)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	status := h.streamAndStoreFlight(w, request, key, h.origin.String(), response, "MISS", flight)
	return status, "MISS"
}

func (h *handler) streamAndStoreFlight(w http.ResponseWriter, request *http.Request, key, origin string, response *http.Response, result string, flight *storeio.Flight) int {
	headers := response.Header.Clone()
	headers.Del("Content-Length")
	reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.spooler, Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StatsStart: func() { h.stats.AddActiveDownload(h.name, "file", 1) },
		StatsDone:  func() { h.stats.AddActiveDownload(h.name, "file", -1) },
		StoreFn: func(ctx context.Context, reader io.Reader) error {
			return storeio.PutResponse(ctx, h.store, objectTenant, key, origin, http.StatusOK, response.Header, "", reader)
		},
		Done: func(err error) { h.flights.Finish(key, flight, err) },
	})
	if err != nil {
		h.flights.Finish(key, flight, err)
		return transport.WriteResponse(w, request, response, "BYPASS")
	}
	transport.CopyEndToEndHeaders(w.Header(), headers)
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
	_ = reader.Close()
	return http.StatusOK
}

func (h *handler) forwardUpstream(w http.ResponseWriter, request *http.Request, cleaned string) int {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		return localError(w, http.StatusBadGateway)
	}
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target.String(), nil)
	if err != nil {
		return localError(w, http.StatusBadGateway)
	}
	h.copyRequestHeaders(upstreamRequest.Header, request.Header)
	transport.SanitizeReadRequestHeaders(upstreamRequest.Header)
	response, err := h.client.DoRead(request.Context(), upstreamRequest, transport.AdmissionForeground)
	if err != nil {
		return localError(w, http.StatusBadGateway)
	}
	return transport.WriteResponse(w, request, response, "BYPASS")
}

func (h *handler) copyRequestHeaders(destination, source http.Header) {
	for _, name := range []string{"Accept", "Accept-Encoding", "Authorization", "Cache-Control", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "Range", "Content-Type"} {
		for _, value := range source.Values(name) {
			destination.Add(name, value)
		}
	}
	for _, name := range h.passHeaders {
		destination.Del(name)
		for _, value := range source.Values(name) {
			destination.Add(name, value)
		}
	}
	if destination.Get("User-Agent") == "" {
		destination.Set("User-Agent", transport.DefaultUserAgent)
	}
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func cacheKey(origin *url.URL, cleaned string, request *http.Request) string {
	hash := sha256.New()
	for _, value := range []string{origin.String(), cleaned, request.URL.RawQuery} {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	names := make([]string, 0, len(request.Header))
	for name := range request.Header {
		canonical := http.CanonicalHeaderKey(name)
		switch canonical {
		case "Cache-Control", "Connection", "If-Match", "If-Modified-Since", "If-None-Match", "If-Range", "If-Unmodified-Since", "Pragma", "Range":
			continue
		}
		names = append(names, canonical)
	}
	sort.Strings(names)
	for _, name := range names {
		_, _ = io.WriteString(hash, name)
		_, _ = hash.Write([]byte{0})
		for _, value := range request.Header.Values(name) {
			_, _ = io.WriteString(hash, value)
			_, _ = hash.Write([]byte{0})
		}
	}
	return path.Join("file", hex.EncodeToString(hash.Sum(nil)))
}

func responseFresh(headers http.Header, fetched time.Time) bool {
	cacheControl := strings.ToLower(headers.Get("Cache-Control"))
	if strings.Contains(cacheControl, "no-cache") {
		return false
	}
	for _, directive := range strings.Split(cacheControl, ",") {
		name, value, found := strings.Cut(strings.TrimSpace(directive), "=")
		if !found || name != "s-maxage" && name != "max-age" {
			continue
		}
		seconds, err := strconv.ParseInt(strings.Trim(value, `"`), 10, 64)
		return err == nil && time.Since(fetched) < time.Duration(seconds)*time.Second
	}
	if expires, err := http.ParseTime(headers.Get("Expires")); err == nil {
		return time.Now().Before(expires)
	}
	return false
}

func cacheable(request *http.Request, response *http.Response) bool {
	requestControl := strings.ToLower(request.Header.Get("Cache-Control"))
	return response.StatusCode == http.StatusOK && !strings.Contains(requestControl, "no-store") &&
		transport.ResponseCacheable(response, false)
}

func serveStored(w http.ResponseWriter, request *http.Request, cached *storeio.ResponseObject, cacheResult string) int {
	defer func() { _ = cached.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), cached.Header)
	w.Header().Set("X-Cache", cacheResult)
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", mime.TypeByExtension(path.Ext(request.URL.Path)))
	}
	http.ServeContent(w, request, "", cached.Fetched, cached.Reader)
	return http.StatusOK
}

func localError(w http.ResponseWriter, status int) int {
	transport.WriteError(w, status)
	return status
}
