package maven

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	mavenTenant       = "maven"
	metadataFreshness = time.Minute
	snapshotFreshness = 5 * time.Minute
	releaseFreshness  = 24 * time.Hour
)

type handler struct {
	origin    *url.URL
	workDir   string
	store     *blobfs.Store
	client    *transport.Client
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
}

func newHandler(origin *url.URL, workDir string, store *blobfs.Store, client *transport.Client) *handler {
	return &handler{origin: origin, workDir: workDir, store: store, client: client, lifecycle: storeio.NewLifecycle()}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		return
	}
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid Maven path", http.StatusBadRequest)
		return
	}
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
		h.forwardUpstream(w, request, cleaned)
		return
	}

	key := mavenKey(h.origin, cleaned, request)
	object, openErr := storeio.OpenResponse(request.Context(), h.store, mavenTenant, key)
	if openErr == nil {
		freshness := releaseFreshness
		base := path.Base(cleaned)
		if strings.HasPrefix(base, "maven-metadata.xml") {
			freshness = metadataFreshness
		} else if strings.Contains(strings.ToUpper(cleaned), "SNAPSHOT") {
			freshness = snapshotFreshness
		}
		if time.Since(object.Fetched) < freshness && !transport.RequestForcesRevalidation(request) {
			h.serveObject(w, request, object, "HIT")
			return
		}
		h.revalidate(w, request, cleaned, key, object)
		return
	}
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		h.forwardUpstream(w, request, cleaned)
		return
	}

	flight, leader := h.flights.Begin(key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if object, err = storeio.OpenResponse(request.Context(), h.store, mavenTenant, key); err == nil {
			h.serveObject(w, request, object, "COALESCED")
		} else {
			h.forwardUpstream(w, request, cleaned)
		}
		return
	}
	if object, err = storeio.OpenResponse(request.Context(), h.store, mavenTenant, key); err == nil {
		h.flights.Finish(key, flight, nil)
		h.serveObject(w, request, object, "HIT")
		return
	}
	h.fill(w, request, cleaned, key, flight)
}

func (h *handler) fill(w http.ResponseWriter, request *http.Request, cleaned, key string, flight *storeio.Flight) {
	response, err := h.fetchUpstream(h.lifecycle.Context(), request, cleaned, nil)
	if err != nil {
		h.flights.Finish(key, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(key, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, mavenTenant, key, h.origin.String(), http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(key, flight, err) },
	})
	if err != nil {
		h.flights.Finish(key, flight, err)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	defer func() { _ = reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func (h *handler) revalidate(w http.ResponseWriter, request *http.Request, cleaned, key string, object *storeio.ResponseObject) {
	defer func() { _ = object.Reader.Close() }()
	flightKey := "revalidate:" + key
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if updated, err := storeio.OpenResponse(request.Context(), h.store, mavenTenant, key); err == nil {
			h.serveObject(w, request, updated, "COALESCED")
		} else {
			h.serveObject(w, request, object, "STALE")
		}
		return
	}
	conditional := http.Header{}
	if value := object.Header.Get("ETag"); value != "" {
		conditional.Set("If-None-Match", value)
	}
	if value := object.Header.Get("Last-Modified"); value != "" {
		conditional.Set("If-Modified-Since", value)
	}
	response, err := h.fetchUpstream(h.lifecycle.Context(), request, cleaned, conditional)
	if err != nil || response.StatusCode >= 500 {
		if response != nil {
			_ = response.Body.Close()
		}
		h.flights.Finish(flightKey, flight, err)
		h.serveObject(w, request, object, "STALE")
		return
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		_ = storeio.TouchResponse(h.lifecycle.Context(), h.store, mavenTenant, key, response.Header)
		h.flights.Finish(flightKey, flight, nil)
		h.serveObject(w, request, object, "REVALIDATED")
		return
	}
	_ = object.Reader.Close()
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(flightKey, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, streamErr := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, mavenTenant, key, h.origin.String(), http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if streamErr != nil {
		h.flights.Finish(flightKey, flight, streamErr)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	defer func() { _ = reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "REFRESH")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func (h *handler) fetchUpstream(ctx context.Context, inbound *http.Request, cleaned string, extra http.Header) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), inbound.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"Accept", "Accept-Encoding", "Cache-Control", "User-Agent"} {
		for _, value := range inbound.Header.Values(name) {
			request.Header.Add(name, value)
		}
	}
	for name, values := range extra {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	return h.client.DoRead(ctx, request, transport.AdmissionForeground)
}

func (h *handler) serveObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, path.Base(request.URL.Path), object.Fetched, object.Reader)
}

func (h *handler) forwardUpstream(w http.ResponseWriter, request *http.Request, cleaned string) int {
	status, err := transport.ForwardRead(request.Context(), h.client, h.origin, w, request, cleaned)
	if err != nil && status == 0 {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	return status
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func mavenKey(origin *url.URL, cleaned string, request *http.Request) string {
	hash := sha256.New()
	for _, value := range []string{origin.String(), cleaned, request.URL.RawQuery, request.Header.Get("Accept"), request.Header.Get("Accept-Encoding")} {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return "refs/" + hex.EncodeToString(hash.Sum(nil))
}
