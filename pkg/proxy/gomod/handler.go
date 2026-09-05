package gomod

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const disableModuleFetchHeader = "Disable-Module-Fetch"
const goTenant = "go"

type moduleRequestKind uint8

const (
	moduleRequestInvalid moduleRequestKind = iota
	moduleRequestList
	moduleRequestLatest
	moduleRequestInfo
	moduleRequestMod
	moduleRequestZip
)

type moduleRequest struct {
	kind       moduleRequestKind
	modulePath string
	version    string
	cacheKey   string
}

type handler struct {
	name      string
	options   *Config
	origin    *url.URL
	sumDB     *url.URL
	store     *blobfs.Store
	client    *transport.Client
	stats     *metrics.Stats
	spooler   *storeio.Spooler
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
}

func newHandler(name, upstream string, transportConfig *config.TransportConfig, options *Config, store *blobfs.Store, stats *metrics.Stats, gate *transport.UpstreamGate, spooler *storeio.Spooler) (*handler, error) {
	if options == nil {
		options = &Config{}
	}
	applyDefaults(options)
	origin, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse Go upstream: %w", err)
	}
	var sumDB *url.URL
	if options.SumDB != nil && options.SumDB.Enabled {
		sumDB, _ = url.Parse(options.SumDB.URL)
	}
	client, err := transport.NewClient(name, config.ModeGo, transportConfig, gate, stats)
	if err != nil {
		return nil, err
	}
	return &handler{
		name:      name,
		options:   options,
		origin:    origin,
		sumDB:     sumDB,
		store:     store,
		client:    client,
		stats:     stats,
		spooler:   spooler,
		lifecycle: storeio.NewLifecycle(),
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		h.stats.RecordRequest(h.name, config.ModeGo, request.Method, "REJECTED", http.StatusMethodNotAllowed, 0)
		return
	}
	status, result := h.serve(w, request)
	h.stats.RecordRequest(h.name, config.ModeGo, request.Method, result, status, 0)
}

func (h *handler) serve(w http.ResponseWriter, request *http.Request) (int, string) {
	decodedTarget, pathErr := storeio.DecodeURLPath(request.URL)
	if pathErr != nil {
		http.Error(w, "invalid Go module path", http.StatusBadRequest)
		return http.StatusBadRequest, "ERROR"
	}
	target := strings.TrimPrefix(decodedTarget, "/")
	if target == "" {
		http.NotFound(w, request)
		return http.StatusNotFound, "BYPASS"
	}
	if strings.HasPrefix(target, "sumdb/") {
		return h.serveSumDB(w, request, target)
	}
	parsed, err := parseModuleRequest(target)
	if err != nil || matchesPrivateModule(h.options, parsed.modulePath) {
		http.NotFound(w, request)
		return http.StatusNotFound, "BYPASS"
	}
	key := "objects/" + hashKey(h.origin.String()+"\x00"+parsed.cacheKey+"\x00"+goCredentialScope(request))
	var cached *storeio.ResponseObject
	if object, err := storeio.OpenResponse(request.Context(), h.store, goTenant, key); err == nil {
		cached = object
		freshness := h.client.RefreshInterval(time.Minute)
		if parsed.kind == moduleRequestMod || parsed.kind == moduleRequestZip {
			freshness = 24 * time.Hour
		}
		fresh := proxyruntime.ResponseFresh(cached.Header, cached.ValidatedAt, freshness) && !proxyruntime.RequestForcesRevalidation(request)
		if request.Header.Get(disableModuleFetchHeader) != "" && h.options.DisableModuleFetchHeader || fresh {
			return serveStoredGoResponse(w, request, cached, "HIT"), "HIT"
		}
		_ = cached.Reader.Close()
	} else if request.Header.Get(disableModuleFetchHeader) != "" && h.options.DisableModuleFetchHeader {
		http.NotFound(w, request)
		return http.StatusNotFound, "BYPASS"
	}
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		response, err := h.fetchUpstream(request.Context(), h.origin, request.Method, parsed.cacheKey, request.Header)
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	flightKey := key
	if cached != nil {
		flightKey += "\x00fetched=" + cached.ValidatedAt.UTC().Format(time.RFC3339Nano)
	}
	flight, leader := h.flights.Begin(flightKey)
	if leader {
		if current, openErr := storeio.OpenResponse(request.Context(), h.store, goTenant, key); openErr == nil {
			if cached == nil || current.ValidatedAt.After(cached.ValidatedAt) {
				h.flights.Finish(flightKey, flight, nil)
				return serveStoredGoResponse(w, request, current, "HIT"), "HIT"
			}
			_ = current.Reader.Close()
		}
		return h.fetchModule(w, request, parsed, key, flightKey, flight, cached)
	}
	if err := h.flights.Wait(request.Context(), flight); err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if updated, openErr := storeio.OpenResponse(request.Context(), h.store, goTenant, key); openErr == nil {
		if cached == nil || updated.ValidatedAt.After(cached.ValidatedAt) || proxyruntime.StaleAllowed(request, updated.Header) {
			return serveStoredGoResponse(w, request, updated, "COALESCED"), "COALESCED"
		}
		_ = updated.Reader.Close()
	}
	transport.WriteError(w, http.StatusBadGateway)
	return http.StatusBadGateway, "ERROR"
}

func (h *handler) fetchModule(w http.ResponseWriter, request *http.Request, parsed moduleRequest, objectKey, flightKey string, flight *storeio.Flight, cached *storeio.ResponseObject) (int, string) {
	upstreamHeader := request.Header.Clone()
	if cached != nil && cached.Origin == h.origin.String() {
		if value := cached.Header.Get("ETag"); value != "" {
			upstreamHeader.Set("If-None-Match", value)
		}
		if value := cached.Header.Get("Last-Modified"); value != "" {
			upstreamHeader.Set("If-Modified-Since", value)
		}
	}
	response, err := h.fetchUpstream(h.lifecycle.Context(), h.origin, http.MethodGet, parsed.cacheKey, upstreamHeader)
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		if cached != nil {
			if stale, openErr := storeio.OpenResponse(request.Context(), h.store, goTenant, objectKey); openErr == nil {
				if !proxyruntime.StaleAllowed(request, stale.Header) {
					_ = stale.Reader.Close()
					transport.WriteError(w, http.StatusBadGateway)
					return http.StatusBadGateway, "ERROR"
				}
				return serveStoredGoResponse(w, request, stale, "STALE"), "STALE"
			}
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode == http.StatusNotModified && cached != nil {
		_ = response.Body.Close()
		refreshed, updateErr := storeio.RevalidateResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, goTenant, objectKey, response.Header)
		h.flights.Finish(flightKey, flight, updateErr)
		if refreshed != nil {
			return serveStoredGoResponse(w, request, refreshed, "REVALIDATED"), "REVALIDATED"
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode >= http.StatusInternalServerError && cached != nil && proxyruntime.StaleAllowed(request, cached.Header) {
			_ = response.Body.Close()
			h.flights.Finish(flightKey, flight, nil)
			if stale, openErr := storeio.OpenResponse(request.Context(), h.store, goTenant, objectKey); openErr == nil {
				if !proxyruntime.StaleAllowed(request, stale.Header) {
					_ = stale.Reader.Close()
					transport.WriteError(w, http.StatusBadGateway)
					return http.StatusBadGateway, "ERROR"
				}
				return serveStoredGoResponse(w, request, stale, "STALE"), "STALE"
			}
		}
		h.flights.Finish(flightKey, flight, nil)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	if !goCacheable(request, response) {
		_ = storeio.DeleteResponse(h.lifecycle.Context(), h.store, goTenant, objectKey)
		h.flights.Finish(flightKey, flight, nil)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	maxSize := int64(64 << 20)
	switch parsed.kind {
	case moduleRequestInfo, moduleRequestLatest:
		maxSize = 1 << 20
	case moduleRequestMod:
		maxSize = 16 << 20
	case moduleRequestZip:
		maxSize = 2 << 30
	}
	if response.ContentLength > maxSize {
		h.flights.Finish(flightKey, flight, errors.New("go module response exceeds size limit"))
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: objectKey, Spooler: h.spooler, MaxBytes: maxSize, Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		VerifyFn: func(reader io.ReadSeeker) error {
			size, err := reader.Seek(0, io.SeekEnd)
			if err != nil {
				return err
			}
			if size > maxSize {
				return errors.New("go module response exceeds size limit")
			}
			if _, err = reader.Seek(0, io.SeekStart); err != nil {
				return err
			}
			file, ok := reader.(*os.File)
			if !ok {
				return errors.New("go module verifier requires a file")
			}
			return validateModuleResponse(parsed, file)
		},
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(storeio.WithResponseTiming(ctx, response), h.store, goTenant, objectKey, h.origin.String(), http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	defer func() { _ = reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), response.Header)
	result := "MISS"
	if cached != nil {
		result = "REFRESH"
	}
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = io.Copy(w, reader)
	}
	return http.StatusOK, result
}

func (h *handler) serveSumDB(w http.ResponseWriter, request *http.Request, target string) (int, string) {
	if h.sumDB == nil || h.options.SumDB == nil {
		http.NotFound(w, request)
		return http.StatusNotFound, "BYPASS"
	}
	prefix := "sumdb/" + h.options.SumDB.Name + "/"
	if !strings.HasPrefix(target, prefix) {
		http.NotFound(w, request)
		return http.StatusNotFound, "BYPASS"
	}
	sumTarget := strings.TrimPrefix(target, prefix)
	if _, err := storeio.CleanRelative(sumTarget); err != nil {
		http.NotFound(w, request)
		return http.StatusNotFound, "BYPASS"
	}
	stable := strings.HasPrefix(sumTarget, "lookup/") || strings.HasPrefix(sumTarget, "tile/")
	key := "sumdb/" + hashKey(h.sumDB.String()+"\x00"+sumTarget)
	object, _ := storeio.OpenResponse(request.Context(), h.store, goTenant, key)
	if object != nil {
		if stable || proxyruntime.ResponseFresh(object.Header, object.ValidatedAt, h.client.RefreshInterval(time.Minute)) && !proxyruntime.RequestForcesRevalidation(request) {
			status := serveStoredGoResponse(w, request, object, "HIT")
			return status, "HIT"
		}
		_ = object.Reader.Close()
	}
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		response, err := h.fetchUpstream(request.Context(), h.sumDB, request.Method, sumTarget, request.Header)
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	flight, leader := h.flights.Begin(key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if joined, err := storeio.OpenResponse(request.Context(), h.store, goTenant, key); err == nil {
			if object != nil && !joined.ValidatedAt.After(object.ValidatedAt) && !proxyruntime.StaleAllowed(request, joined.Header) {
				_ = joined.Reader.Close()
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			return serveStoredGoResponse(w, request, joined, "COALESCED"), "COALESCED"
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	defer h.flights.Finish(key, flight, nil)
	if current, err := storeio.OpenResponse(request.Context(), h.store, goTenant, key); err == nil {
		if object == nil || current.ValidatedAt.After(object.ValidatedAt) {
			return serveStoredGoResponse(w, request, current, "HIT"), "HIT"
		}
		_ = current.Reader.Close()
	}
	upstreamHeader := request.Header.Clone()
	if object != nil && object.Origin == h.sumDB.String() {
		if value := object.Header.Get("ETag"); value != "" {
			upstreamHeader.Set("If-None-Match", value)
		}
		if value := object.Header.Get("Last-Modified"); value != "" {
			upstreamHeader.Set("If-Modified-Since", value)
		}
	}
	response, err := h.fetchUpstream(h.lifecycle.Context(), h.sumDB, http.MethodGet, sumTarget, upstreamHeader)
	if err != nil {
		if object != nil {
			if stale, openErr := storeio.OpenResponse(request.Context(), h.store, goTenant, key); openErr == nil {
				if !proxyruntime.StaleAllowed(request, stale.Header) {
					_ = stale.Reader.Close()
					transport.WriteError(w, http.StatusBadGateway)
					return http.StatusBadGateway, "ERROR"
				}
				return serveStoredGoResponse(w, request, stale, "STALE"), "STALE"
			}
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode == http.StatusNotModified && object != nil {
		_ = response.Body.Close()
		if refreshed, _ := storeio.RevalidateResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, goTenant, key, response.Header); refreshed != nil {
			return serveStoredGoResponse(w, request, refreshed, "REVALIDATED"), "REVALIDATED"
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode >= http.StatusInternalServerError && object != nil && proxyruntime.StaleAllowed(request, object.Header) {
		_ = response.Body.Close()
		if stale, openErr := storeio.OpenResponse(request.Context(), h.store, goTenant, key); openErr == nil {
			if !proxyruntime.StaleAllowed(request, stale.Header) {
				_ = stale.Reader.Close()
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			return serveStoredGoResponse(w, request, stale, "STALE"), "STALE"
		}
	}
	if response.StatusCode != http.StatusOK {
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	if !transport.ResponseCacheable(response, false) {
		_ = storeio.DeleteResponse(h.lifecycle.Context(), h.store, goTenant, key)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	defer func() { _ = response.Body.Close() }()
	spool, err := h.spooler.SpoolWithExpectedSize(h.lifecycle.Context(), response.Body, 64<<20, response.ContentLength)
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	defer func() { _ = spool.Close() }()
	_, _ = spool.File.Seek(0, io.SeekStart)
	result := "MISS"
	if object != nil {
		result = "REFRESH"
	}
	if err := storeio.PutResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, goTenant, key, h.sumDB.String(), http.StatusOK, response.Header, spool.SHA256, spool.File); err != nil {
		result = "BYPASS"
	}
	_, _ = spool.File.Seek(0, io.SeekStart)
	transport.CopyEndToEndHeaders(w.Header(), response.Header)
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, spool.File)
	return http.StatusOK, result
}

func (h *handler) fetchUpstream(ctx context.Context, origin *url.URL, method, requestPath string, headers http.Header) (*http.Response, error) {
	targetURL, err := transport.JoinURL(origin, requestPath, "")
	if err != nil {
		return nil, err
	}
	upstream, err := http.NewRequestWithContext(ctx, method, targetURL.String(), nil)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"Accept", "Authorization", "User-Agent", "If-None-Match", "If-Modified-Since", "Cache-Control", "Pragma"} {
		for _, value := range headers.Values(name) {
			upstream.Header.Add(name, value)
		}
	}
	return h.client.DoRead(ctx, upstream, transport.AdmissionForeground)
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func serveStoredGoResponse(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) int {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.ResponseHeader())
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, "", object.ValidatedAt, object.Reader)
	return http.StatusOK
}

func parseModuleRequest(target string) (moduleRequest, error) {
	target = strings.TrimPrefix(target, "/")
	if target == "" || strings.HasPrefix(target, "sumdb/") || strings.Contains(target, "//") {
		return moduleRequest{}, fs.ErrNotExist
	}
	for _, segment := range strings.Split(target, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.ContainsRune(segment, '\x00') || strings.Contains(segment, "\\") {
			return moduleRequest{}, fs.ErrNotExist
		}
	}
	modulePath, suffix, ok := strings.Cut(target, "/@")
	if !ok || modulePath == "" {
		return moduleRequest{}, fs.ErrNotExist
	}
	unescapedModulePath, err := module.UnescapePath(modulePath)
	if err != nil || unescapedModulePath == "" {
		return moduleRequest{}, fs.ErrNotExist
	}
	switch suffix {
	case "v/list":
		return moduleRequest{kind: moduleRequestList, modulePath: unescapedModulePath, cacheKey: target, version: "list"}, nil
	case "latest":
		return moduleRequest{kind: moduleRequestLatest, modulePath: unescapedModulePath, cacheKey: target, version: "latest"}, nil
	}
	if !strings.HasPrefix(suffix, "v/") {
		return moduleRequest{}, fs.ErrNotExist
	}
	versionFile := strings.TrimPrefix(suffix, "v/")
	for _, candidate := range []struct {
		kind   moduleRequestKind
		suffix string
	}{{moduleRequestInfo, ".info"}, {moduleRequestMod, ".mod"}, {moduleRequestZip, ".zip"}} {
		if strings.HasSuffix(versionFile, candidate.suffix) {
			version := strings.TrimSuffix(versionFile, candidate.suffix)
			if version == "" || strings.Contains(version, "/") || module.CanonicalVersion(version) != version {
				return moduleRequest{}, fs.ErrNotExist
			}
			return moduleRequest{kind: candidate.kind, modulePath: unescapedModulePath, version: version, cacheKey: target}, nil
		}
	}
	return moduleRequest{}, fs.ErrNotExist
}

func hashKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func goCredentialScope(request *http.Request) string {
	value := request.Header.Get("Authorization") + "\x00" + request.Header.Get("Cookie")
	if value == "\x00" {
		return "anonymous"
	}
	return hashKey(value)
}

func goCacheable(request *http.Request, response *http.Response) bool {
	return transport.ResponseCacheable(response, goCredentialScope(request) != "anonymous")
}

func validateModuleResponse(parsed moduleRequest, file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	switch parsed.kind {
	case moduleRequestList:
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			version := strings.TrimSpace(scanner.Text())
			if version != "" && module.CanonicalVersion(version) != version {
				return errors.New("invalid module version list")
			}
		}
		return scanner.Err()
	case moduleRequestLatest, moduleRequestInfo:
		var info struct {
			Version string `json:"Version"`
			Time    string `json:"Time"`
		}
		if err := json.NewDecoder(file).Decode(&info); err != nil || module.CanonicalVersion(info.Version) != info.Version {
			return errors.New("invalid module info response")
		}
		if parsed.kind == moduleRequestInfo && info.Version != parsed.version {
			return errors.New("module info version mismatch")
		}
		return nil
	case moduleRequestMod:
		body, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		_, err = modfile.Parse("go.mod", body, nil)
		return err
	case moduleRequestZip:
		_, err := modzip.CheckZip(module.Version{Path: parsed.modulePath, Version: parsed.version}, file.Name())
		return err
	default:
		return errors.New("invalid module response kind")
	}
}
