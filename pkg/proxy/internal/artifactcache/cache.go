// Package artifactcache implements the shared lifecycle for stable package
// artifacts. Protocol handlers remain responsible for classifying requests and
// constructing cache keys.
package artifactcache

import (
	"context"
	"io"
	"net/http"
	"path"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

// FetchFunc performs one authorized upstream read for an artifact cache.
type FetchFunc func(ctx context.Context, method, requestPath, rawQuery string, header http.Header) (*http.Response, error)

// KeyFunc constructs the protocol-owned stable cache identity.
type KeyFunc func(requestPath string, request *http.Request) string

// Cache coordinates artifact fills, conditional refreshes, and stale serving.
type Cache struct {
	Tenant    string
	Upstream  string
	Freshness time.Duration
	// FreshnessForPath overrides Freshness when cache lifetime depends on the path.
	FreshnessForPath func(requestPath string) time.Duration
	Store            *blobfs.Store
	Spooler          *storeio.Spooler
	Lifecycle        *storeio.Lifecycle
	Flights          *storeio.FlightGroup
	FetchUpstream    FetchFunc
	CacheKey         KeyFunc
}

func (c *Cache) Serve(w http.ResponseWriter, request *http.Request, requestPath string) (int, string) {
	key := c.CacheKey(requestPath, request)
	object, _ := storeio.OpenResponse(request.Context(), c.Store, c.Tenant, key)
	if object != nil {
		freshness := c.Freshness
		if c.FreshnessForPath != nil {
			freshness = c.FreshnessForPath(requestPath)
		}
		if proxyruntime.ResponseFresh(object.Header, object.ValidatedAt, freshness) && !proxyruntime.RequestForcesRevalidation(request) {
			return serveCached(w, request, object, "HIT"), "HIT"
		}
		if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
			_ = object.Reader.Close()
			return c.forwardUpstream(w, request, requestPath)
		}
		return c.revalidate(w, request, requestPath, key, object)
	}

	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		return c.forwardUpstream(w, request, requestPath)
	}
	flight, leader := c.Flights.Begin(key)
	if !leader {
		if err := c.Flights.Wait(request.Context(), flight); err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		if cached, err := storeio.OpenResponse(request.Context(), c.Store, c.Tenant, key); err == nil {
			return serveCached(w, request, cached, "COALESCED"), "COALESCED"
		}
		return c.forwardUpstream(w, request, requestPath)
	}
	if cached, err := storeio.OpenResponse(request.Context(), c.Store, c.Tenant, key); err == nil {
		c.Flights.Finish(key, flight, nil)
		return serveCached(w, request, cached, "HIT"), "HIT"
	}

	response, err := c.FetchUpstream(c.Lifecycle.Context(), http.MethodGet, requestPath, request.URL.RawQuery, request.Header)
	if err != nil {
		c.Flights.Finish(key, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		c.Flights.Finish(key, flight, nil)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	return c.streamAndCache(w, request, response, key, key, flight, "MISS")
}

func (c *Cache) revalidate(w http.ResponseWriter, request *http.Request, requestPath, key string, object *storeio.ResponseObject) (int, string) {
	flightKey := "revalidate:" + key
	flight, leader := c.Flights.Begin(flightKey)
	if !leader {
		_ = c.Flights.Wait(request.Context(), flight)
		if updated, err := storeio.OpenResponse(request.Context(), c.Store, c.Tenant, key); err == nil {
			if updated.ValidatedAt.After(object.ValidatedAt) {
				_ = object.Reader.Close()
				return serveCached(w, request, updated, "COALESCED"), "COALESCED"
			}
			_ = updated.Reader.Close()
		}
		if proxyruntime.StaleAllowed(request, object.Header) {
			return serveCached(w, request, object, "STALE"), "STALE"
		}
		_ = object.Reader.Close()
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}

	conditional := request.Header.Clone()
	if etag := object.Header.Get("ETag"); etag != "" {
		conditional.Set("If-None-Match", etag)
	}
	if modified := object.Header.Get("Last-Modified"); modified != "" {
		conditional.Set("If-Modified-Since", modified)
	}
	response, err := c.FetchUpstream(c.Lifecycle.Context(), http.MethodGet, requestPath, request.URL.RawQuery, conditional)
	if err != nil || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
		if !proxyruntime.StaleAllowed(request, object.Header) {
			_ = object.Reader.Close()
			c.Flights.Finish(flightKey, flight, err)
			if response != nil {
				return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
			}
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		if response != nil {
			_ = response.Body.Close()
		}
		c.Flights.Finish(flightKey, flight, err)
		return serveCached(w, request, object, "STALE"), "STALE"
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		timingCtx := storeio.WithResponseTiming(c.Lifecycle.Context(), response)
		object.ValidatedAt, response.Header = storeio.ResponseTimingHeader(timingCtx, response.Header)
		object.Header = proxyruntime.MergeRevalidationHeader(object.Header, response.Header)
		err = storeio.TouchResponse(timingCtx, c.Store, c.Tenant, key, response.Header)
		c.Flights.Finish(flightKey, flight, err)
		result := "REVALIDATED"
		if err != nil {
			result = "BYPASS"
		}
		return serveCached(w, request, object, result), result
	}

	_ = object.Reader.Close()
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		c.Flights.Finish(flightKey, flight, nil)
		_ = storeio.DeleteResponse(context.Background(), c.Store, c.Tenant, key)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	return c.streamAndCache(w, request, response, key, flightKey, flight, "REFRESH")
}

func (c *Cache) streamAndCache(w http.ResponseWriter, request *http.Request, response *http.Response, key, flightKey string, flight *storeio.Flight, result string) (int, string) {
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, err := storeio.StartStream(c.Lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: c.Spooler, Lifecycle: c.Lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(storeio.WithResponseTiming(ctx, response), c.Store, c.Tenant, key, c.Upstream, http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { c.Flights.Finish(flightKey, flight, err) },
	})
	if err != nil {
		c.Flights.Finish(flightKey, flight, err)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	defer func() { _ = reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
	return http.StatusOK, result
}

func (c *Cache) forwardUpstream(w http.ResponseWriter, request *http.Request, requestPath string) (int, string) {
	response, err := c.FetchUpstream(request.Context(), request.Method, requestPath, request.URL.RawQuery, request.Header)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
}

func serveCached(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) int {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.ResponseHeader())
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, path.Base(request.URL.Path), object.ValidatedAt, object.Reader)
	return http.StatusOK
}
