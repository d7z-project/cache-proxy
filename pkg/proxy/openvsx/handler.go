package openvsx

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/artifactcache"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	maxQueryBody       = 1 << 20
	maxTextResponse    = 64 << 20
	maxRewrittenOutput = 128 << 20
)

type handlerConfig struct {
	name       string
	origin     *url.URL
	publicBase *url.URL
	workDir    string
	store      *blobfs.Store
	client     *transport.Client
	stats      *metrics.Stats
	freshness  time.Duration
}

type handler struct {
	uncached   bool
	name       string
	origin     *url.URL
	publicBase *url.URL
	store      *blobfs.Store
	client     *transport.Client
	stats      *metrics.Stats
	spooler    *storeio.Spooler
	lifecycle  *storeio.Lifecycle
	flights    storeio.FlightGroup
	freshness  time.Duration
	artifacts  artifactcache.Cache
}

type preparedDocument struct {
	spool  *storeio.SpoolResult
	header http.Header
}

type resourceKind uint8

const (
	documentResource resourceKind = iota
	redirectableDocumentResource
	artifactResource
)

func newHandler(cfg handlerConfig) *handler {
	lifecycle := storeio.NewLifecycle()
	h := &handler{
		name: cfg.name, origin: cfg.origin, publicBase: cfg.publicBase, store: cfg.store,
		client: cfg.client, stats: cfg.stats, spooler: cfg.client.EnsureSpooler(cfg.workDir), lifecycle: lifecycle,
		freshness: cfg.freshness,
	}
	h.artifacts = artifactcache.Cache{
		Tenant: responseTenant, Upstream: cfg.origin.String(), Freshness: artifactFreshness,
		Store: cfg.store, Spooler: h.spooler, Lifecycle: lifecycle, Flights: &h.flights,
		FetchUpstream: h.fetchArtifact,
		FreshnessForPath: func(requestPath string) time.Duration {
			for segment := range strings.SplitSeq(requestPath, "/") {
				if segment == "latest" {
					return cfg.freshness
				}
			}
			return artifactFreshness
		},
		CacheKey: func(requestPath string, request *http.Request) string {
			return "artifacts/" + responseKey(cfg.origin.String(), http.MethodGet, requestPath, request.URL.RawQuery, "", request.Header)
		},
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	status, result := h.serve(w, request)
	h.stats.RecordRequest(h.name, "openvsx", request.Method, result, status, 0)
}

func (h *handler) serve(w http.ResponseWriter, request *http.Request) (int, string) {
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid Open VSX path", http.StatusBadRequest)
		return http.StatusBadRequest, "ERROR"
	}
	var body []byte
	switch request.Method {
	case http.MethodGet, http.MethodHead:
		if request.Body != nil && request.Body != http.NoBody {
			http.Error(w, "request body is not allowed", http.StatusBadRequest)
			return http.StatusBadRequest, "REJECTED"
		}
	case http.MethodPost:
		if cleaned != "vscode/gallery/extensionquery" {
			w.Header().Set("Allow", "GET, HEAD")
			transport.WriteError(w, http.StatusMethodNotAllowed)
			return http.StatusMethodNotAllowed, "REJECTED"
		}
		if request.ContentLength > maxQueryBody {
			transport.WriteError(w, http.StatusRequestEntityTooLarge)
			return http.StatusRequestEntityTooLarge, "REJECTED"
		}
		body, err = io.ReadAll(io.LimitReader(request.Body, maxQueryBody+1))
		_ = request.Body.Close()
		if err != nil {
			transport.WriteError(w, http.StatusBadRequest)
			return http.StatusBadRequest, "ERROR"
		}
		if len(body) > maxQueryBody {
			transport.WriteError(w, http.StatusRequestEntityTooLarge)
			return http.StatusRequestEntityTooLarge, "REJECTED"
		}
	default:
		w.Header().Set("Allow", "GET, HEAD")
		transport.WriteError(w, http.StatusMethodNotAllowed)
		return http.StatusMethodNotAllowed, "REJECTED"
	}

	if request.Method != http.MethodPost && classifyResource(cleaned) == artifactResource {
		if h.uncached || request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
			response, err := h.fetchArtifact(request.Context(), request.Method, cleaned, request.URL.RawQuery, request.Header)
			if err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		return h.artifacts.Serve(w, request, cleaned)
	}
	return h.serveDocument(w, request, cleaned, body)
}

func classifyResource(cleaned string) resourceKind {
	segments := strings.Split(cleaned, "/")
	if len(segments) >= 6 && segments[0] == "vscode" && segments[1] == "asset" {
		return artifactResource
	}
	if len(segments) == 8 && segments[0] == "vscode" && segments[1] == "gallery" &&
		segments[2] == "publishers" && segments[4] == "vsextensions" && segments[7] == "vspackage" {
		return artifactResource
	}
	if len(segments) >= 6 && segments[0] == "api" &&
		(segments[4] == "file" || len(segments) >= 7 && segments[5] == "file") {
		return artifactResource
	}
	if strings.HasPrefix(cleaned, "vscode/unpkg/") {
		return redirectableDocumentResource
	}
	return documentResource
}

func (h *handler) serveDocument(w http.ResponseWriter, request *http.Request, cleaned string, body []byte) (int, string) {
	bodyDigest := ""
	if len(body) > 0 {
		digest := sha256.Sum256(body)
		bodyDigest = hex.EncodeToString(digest[:])
	}
	key := "documents/" + responseKey(h.origin.String()+"\x00"+h.publicBase.String(), request.Method, cleaned, request.URL.RawQuery, bodyDigest, request.Header)
	private := request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != ""
	if !h.uncached && !private {
		if cached, err := storeio.OpenResponse(request.Context(), h.store, responseTenant, key); err == nil {
			if proxyruntime.ResponseFresh(cached.Header, cached.ValidatedAt, h.freshness) && !proxyruntime.RequestForcesRevalidation(request) {
				return transport.ServeStoredResponse(w, request, cached, path.Base(cleaned), "HIT"), "HIT"
			}
			if request.Method != http.MethodHead {
				return h.revalidateDocument(w, request, cleaned, body, key, cached)
			}
			_ = cached.Reader.Close()
		}
	}
	if h.uncached || private || request.Method == http.MethodHead {
		return h.forwardDocument(w, request, cleaned, body)
	}

	flight, leader := h.flights.Begin(key)
	if !leader {
		if err := h.flights.Wait(request.Context(), flight); err != nil {
			return h.forwardDocument(w, request, cleaned, body)
		}
		if cached, err := storeio.OpenResponse(request.Context(), h.store, responseTenant, key); err == nil {
			return transport.ServeStoredResponse(w, request, cached, path.Base(cleaned), "COALESCED"), "COALESCED"
		}
		return h.forwardDocument(w, request, cleaned, body)
	}
	response, err := h.fetchDocument(h.lifecycle.Context(), request, cleaned, body, nil)
	if err != nil {
		h.flights.Finish(key, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode != http.StatusOK || !documentCacheable(response) {
		h.flights.Finish(key, flight, nil)
		return h.serveDocumentResponse(w, request, cleaned, response, "BYPASS")
	}
	return h.commitDocument(w, request, cleaned, key, key, flight, response, "MISS")
}

func (h *handler) forwardDocument(w http.ResponseWriter, request *http.Request, cleaned string, body []byte) (int, string) {
	response, err := h.fetchDocument(request.Context(), request, cleaned, body, nil)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	return h.serveDocumentResponse(w, request, cleaned, response, "BYPASS")
}

func (h *handler) revalidateDocument(w http.ResponseWriter, request *http.Request, cleaned string, body []byte, key string, cached *storeio.ResponseObject) (int, string) {
	flightKey := "revalidate:" + key
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		if waitErr := h.flights.Wait(request.Context(), flight); waitErr == nil {
			if updated, err := storeio.OpenResponse(request.Context(), h.store, responseTenant, key); err == nil {
				if updated.ValidatedAt.After(cached.ValidatedAt) {
					_ = cached.Reader.Close()
					return transport.ServeStoredResponse(w, request, updated, path.Base(cleaned), "COALESCED"), "COALESCED"
				}
				_ = updated.Reader.Close()
			}
		}
		if proxyruntime.StaleAllowed(request, cached.Header) {
			return transport.ServeStoredResponse(w, request, cached, path.Base(cleaned), "STALE"), "STALE"
		}
		_ = cached.Reader.Close()
		response, err := h.fetchDocument(request.Context(), request, cleaned, body, nil)
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		return h.serveDocumentResponse(w, request, cleaned, response, "BYPASS")
	}
	response, err := h.fetchDocument(h.lifecycle.Context(), request, cleaned, body, cached)
	if err != nil || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
		h.flights.Finish(flightKey, flight, err)
		if proxyruntime.StaleAllowed(request, cached.Header) {
			if response != nil {
				_ = response.Body.Close()
			}
			return transport.ServeStoredResponse(w, request, cached, path.Base(cleaned), "STALE"), "STALE"
		}
		_ = cached.Reader.Close()
		if response != nil {
			return h.serveDocumentResponse(w, request, cleaned, response, "BYPASS")
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		header := transport.SourceRevalidationHeader(response.Header)
		validated, touchErr := storeio.RevalidateResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, responseTenant, key, header)
		h.flights.Finish(flightKey, flight, touchErr)
		_ = cached.Reader.Close()
		if validated != nil {
			return transport.ServeStoredResponse(w, request, validated, path.Base(cleaned), "REVALIDATED"), "REVALIDATED"
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	_ = cached.Reader.Close()
	if response.StatusCode != http.StatusOK || !documentCacheable(response) {
		h.flights.Finish(flightKey, flight, nil)
		_ = storeio.DeleteResponse(context.Background(), h.store, responseTenant, key)
		return h.serveDocumentResponse(w, request, cleaned, response, "BYPASS")
	}
	return h.commitDocument(w, request, cleaned, key, flightKey, flight, response, "REFRESH")
}

func (h *handler) commitDocument(w http.ResponseWriter, request *http.Request, cleaned, key, flightKey string, flight *storeio.Flight, response *http.Response, result string) (int, string) {
	prepared, err := h.prepareDocumentResponse(h.lifecycle.Context(), cleaned, response)
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	defer prepared.spool.Close()
	err = storeio.PutResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, responseTenant, key, h.origin.String(), prepared.header, prepared.spool.SHA256, prepared.spool.File)
	h.flights.Finish(flightKey, flight, err)
	if err == nil {
		if stored, openErr := storeio.OpenResponse(request.Context(), h.store, responseTenant, key); openErr == nil {
			return transport.ServeStoredResponse(w, request, stored, path.Base(cleaned), result), result
		}
	}
	_, _ = prepared.spool.File.Seek(0, io.SeekStart)
	return servePreparedDocument(w, request, response.StatusCode, prepared, "BYPASS"), "BYPASS"
}

func (h *handler) fetchDocument(ctx context.Context, request *http.Request, cleaned string, body []byte, cached *storeio.ResponseObject) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	var source io.Reader
	if request.Method == http.MethodPost {
		source = bytes.NewReader(body)
	}
	upstream, err := http.NewRequestWithContext(ctx, request.Method, target.String(), source)
	if err != nil {
		return nil, err
	}
	if request.Method == http.MethodPost {
		transport.CopyUpstreamPostHeaders(upstream.Header, request.Header)
	} else {
		transport.CopyUpstreamReadHeaders(upstream.Header, request.Header)
	}
	upstream.Header.Del("Authorization")
	upstream.Header.Del("Cookie")
	upstream.Header.Set("Accept-Encoding", "identity")
	for _, name := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "Range", "If-Range"} {
		upstream.Header.Del(name)
	}
	if cached != nil && cached.Origin == h.origin.String() {
		if value := cached.Header.Get("X-Source-ETag"); value != "" {
			upstream.Header.Set("If-None-Match", value)
		}
		if value := cached.Header.Get("X-Source-Last-Modified"); value != "" {
			upstream.Header.Set("If-Modified-Since", value)
		}
	}
	if request.Method == http.MethodPost {
		return h.client.DoReadOnlyPostNoRedirect(ctx, upstream, transport.AdmissionForeground)
	}
	if classifyResource(cleaned) == redirectableDocumentResource {
		return h.client.DoRead(ctx, upstream, transport.AdmissionForeground)
	}
	return h.client.DoReadNoRedirect(ctx, upstream, transport.AdmissionForeground)
}

func (h *handler) serveDocumentResponse(w http.ResponseWriter, request *http.Request, cleaned string, response *http.Response, result string) (int, string) {
	redirect := response.StatusCode >= 300 && response.StatusCode < 400 && response.StatusCode != http.StatusNotModified
	if redirect || request.Method == http.MethodHead || response.Body == nil || response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotModified {
		if response.Body != nil {
			defer response.Body.Close()
		}
		header, err := h.rewriteResponseHeader(response.Header)
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		kind := documentKind(response.Header.Get("Content-Type"), cleaned)
		if request.Method == http.MethodHead && shouldRewriteDocument(cleaned, kind) {
			for _, name := range []string{"Content-Length", "Content-MD5", "Content-Digest", "Digest", "ETag", "Last-Modified", "Repr-Digest"} {
				header.Del(name)
			}
		}
		transport.CopyDownstreamResponseHeaders(w.Header(), header)
		w.Header().Set("X-Cache", result)
		w.WriteHeader(response.StatusCode)
		return response.StatusCode, result
	}
	prepared, err := h.prepareDocumentResponse(request.Context(), cleaned, response)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	defer prepared.spool.Close()
	return servePreparedDocument(w, request, response.StatusCode, prepared, result), result
}

func (h *handler) prepareDocumentResponse(ctx context.Context, cleaned string, response *http.Response) (*preparedDocument, error) {
	defer response.Body.Close()
	kind := documentKind(response.Header.Get("Content-Type"), cleaned)
	limit := int64(0)
	if kind != "raw" {
		limit = maxTextResponse
	}
	source, err := h.spooler.SpoolWithExpectedSize(ctx, response.Body, limit, response.ContentLength)
	if err != nil {
		return nil, err
	}
	if !shouldRewriteDocument(cleaned, kind) {
		header, err := h.rewriteResponseHeader(response.Header)
		if err != nil {
			source.Close()
			return nil, err
		}
		header.Set("X-Source-ETag", response.Header.Get("ETag"))
		header.Set("X-Source-Last-Modified", response.Header.Get("Last-Modified"))
		return &preparedDocument{spool: source, header: header}, nil
	}

	reader, writer := io.Pipe()
	var rewriteErr error
	var group sync.WaitGroup
	group.Go(func() {
		switch kind {
		case "json":
			rewriteErr = rewriteJSON(source.File, writer, h.origin, h.publicBase)
		case "html":
			rewriteErr = rewriteHTML(source.File, writer, h.origin, h.publicBase)
		default:
			var data []byte
			data, rewriteErr = io.ReadAll(source.File)
			if rewriteErr == nil {
				_, rewriteErr = writer.Write([]byte(rewriteText(string(data), h.origin, h.publicBase)))
			}
		}
		_ = writer.CloseWithError(rewriteErr)
	})
	output, spoolErr := h.spooler.SpoolWithExpectedSize(ctx, reader, maxRewrittenOutput, -1)
	_ = reader.CloseWithError(spoolErr)
	group.Wait()
	_ = source.Close()
	if spoolErr != nil {
		return nil, spoolErr
	}
	if rewriteErr != nil {
		_ = output.Close()
		return nil, rewriteErr
	}
	header, err := h.rewriteResponseHeader(response.Header)
	if err != nil {
		_ = output.Close()
		return nil, err
	}
	header.Del("Content-Length")
	header.Del("Content-Encoding")
	header.Del("Content-MD5")
	header.Del("Content-Digest")
	header.Del("Digest")
	header.Del("Repr-Digest")
	header.Set("X-Source-ETag", response.Header.Get("ETag"))
	header.Set("X-Source-Last-Modified", response.Header.Get("Last-Modified"))
	header.Del("Last-Modified")
	header.Set("ETag", `"sha256-`+output.SHA256+`"`)
	if kind == "html" {
		header.Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; object-src 'none'; frame-src 'none'; base-uri 'self'; form-action 'self'")
	}
	return &preparedDocument{spool: output, header: header}, nil
}

func (h *handler) rewriteResponseHeader(source http.Header) (http.Header, error) {
	header := source.Clone()
	header.Del("Set-Cookie")
	header.Del("Refresh")
	for index, value := range header.Values("Link") {
		header["Link"][index] = rewriteText(value, h.origin, h.publicBase)
	}
	if location := header.Get("Location"); location != "" {
		target, err := url.Parse(location)
		if err != nil {
			return nil, errors.New("invalid open VSX redirect location")
		}
		if !target.IsAbs() {
			target = h.origin.ResolveReference(target)
		}
		if !transport.SameOrigin(target, h.origin) {
			return nil, errors.New("open VSX navigation redirected outside its origin")
		}
		header.Set("Location", rewriteURL(target.String(), h.origin, h.publicBase))
	}
	if location := header.Get("Content-Location"); location != "" {
		rewritten := rewriteURL(location, h.origin, h.publicBase)
		if rewritten == location {
			header.Del("Content-Location")
		} else {
			header.Set("Content-Location", rewritten)
		}
	}
	if strings.EqualFold(header.Get("Access-Control-Allow-Origin"), h.origin.Scheme+"://"+h.origin.Host) {
		header.Set("Access-Control-Allow-Origin", h.publicBase.Scheme+"://"+h.publicBase.Host)
	}
	return header, nil
}

func documentKind(contentType, cleaned string) string {
	mediaType, _, _ := mime.ParseMediaType(contentType)
	mediaType = strings.ToLower(mediaType)
	switch {
	case mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"):
		return "json"
	case mediaType == "text/html" || mediaType == "application/xhtml+xml":
		return "html"
	case mediaType == "text/css", strings.Contains(mediaType, "javascript"), mediaType == "application/manifest+json",
		mediaType == "application/xml", mediaType == "text/xml", strings.HasPrefix(cleaned, "bundle-"), strings.HasPrefix(cleaned, "chunk-"):
		return "text"
	default:
		return "raw"
	}
}

func shouldRewriteDocument(cleaned, kind string) bool {
	return kind != "raw" && !(strings.HasPrefix(cleaned, "vscode/unpkg/") && kind != "json")
}

func (h *handler) fetchArtifact(ctx context.Context, method, requestPath, rawQuery string, header http.Header) (*http.Response, error) {
	clean := header.Clone()
	clean.Del("Authorization")
	clean.Del("Cookie")
	clean.Set("Accept-Encoding", "identity")
	response, err := h.client.DoReadPath(ctx, h.origin, method, requestPath, rawQuery, clean, transport.AdmissionForeground)
	if err != nil {
		return nil, err
	}
	rewritten, err := h.rewriteResponseHeader(response.Header)
	if err != nil {
		_ = response.Body.Close()
		return nil, err
	}
	response.Header = rewritten
	return response, nil
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func documentCacheable(response *http.Response) bool {
	if !transport.ResponseCacheable(response, false) {
		return false
	}
	for _, line := range response.Header.Values("Vary") {
		for field := range strings.SplitSeq(line, ",") {
			switch http.CanonicalHeaderKey(strings.TrimSpace(field)) {
			case "Accept", "Accept-Encoding", "Accept-Language", "Content-Type", "Host", "User-Agent", "Via":
			default:
				return false
			}
		}
	}
	return true
}

func responseKey(origin, method, requestPath, rawQuery, bodyDigest string, header http.Header) string {
	hash := sha256.New()
	for _, value := range []string{origin, method, requestPath, rawQuery, bodyDigest, header.Get("Accept"), header.Get("Accept-Language"), header.Get("Content-Type")} {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func servePreparedDocument(w http.ResponseWriter, request *http.Request, status int, prepared *preparedDocument, result string) int {
	transport.CopyDownstreamResponseHeaders(w.Header(), prepared.header)
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("Content-Length", strconv.FormatInt(prepared.spool.Size, 10))
	w.Header().Set("X-Cache", result)
	if status != http.StatusOK {
		w.WriteHeader(status)
		if request.Method != http.MethodHead {
			_, _ = io.Copy(w, prepared.spool.File)
		}
		return status
	}
	modified, _ := http.ParseTime(prepared.header.Get("Last-Modified"))
	recorder := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	http.ServeContent(recorder, request, path.Base(request.URL.Path), modified, prepared.spool.File)
	return recorder.status
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
