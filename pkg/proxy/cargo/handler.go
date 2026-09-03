package cargo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	cargoTenant        = "cargo"
	cargoRefFreshness  = time.Minute
	maxCargoConfigSize = int64(1 << 20)
	maxSparseIndexSize = int64(64 << 20)
	maxSparseLineSize  = 2 << 20
)

type handler struct {
	origin    *url.URL
	stateDir  string
	workDir   string
	store     *blobfs.Store
	client    *transport.Client
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
}

type sparseRecord struct {
	Name     string `json:"name"`
	Version  string `json:"vers"`
	Checksum string `json:"cksum"`
}

func newHandler(origin *url.URL, stateDir, workDir string, store *blobfs.Store, client *transport.Client) (*handler, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, err
	}
	return &handler{origin: origin, stateDir: stateDir, workDir: workDir, store: store, client: client, lifecycle: storeio.NewLifecycle()}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	cleaned := ""
	if request.URL.Path == "" || request.URL.Path == "/" {
		cleaned = "config.json"
	} else {
		var err error
		cleaned, err = storeio.CleanURLPath(request.URL)
		if err != nil {
			http.Error(w, "invalid Cargo path", http.StatusBadRequest)
			return
		}
	}
	if isCargoGitUploadPack(request.Method, cleaned) {
		h.forwardGitUploadPack(w, request)
		return
	}
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		return
	}
	if services, set := request.URL.Query()["service"]; cleaned == "info/refs" && set &&
		(len(services) != 1 || services[0] != "") && !isCargoGitReadRequest(cleaned, request) {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	if strings.HasPrefix(cleaned, "-/crate/") {
		h.serveCrate(w, request, strings.TrimPrefix(cleaned, "-/crate/"))
		return
	}
	if cleaned == "config.json" {
		h.serveConfig(w, request)
		return
	}
	if isCargoGitReadRequest(cleaned, request) || !isSparseIndexPath(cleaned) {
		h.forwardRead(w, request, cleaned)
		return
	}
	h.serveSparseIndex(w, request, cleaned)
}

func (h *handler) serveConfig(w http.ResponseWriter, request *http.Request) {
	scope := cargoCredentialScope(request)
	externalBase := proxyruntime.ExternalBaseURL(request)
	key := cargoRefKey("config", h.origin.String(), scope, request.Header.Get("Accept"), externalBase)
	object, _ := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key)
	if object != nil {
		if time.Since(object.Fetched) < cargoRefFreshness && !transport.RequestForcesRevalidation(request) {
			serveCargoObject(w, request, object, "HIT")
			return
		}
		_ = object.Reader.Close()
	}
	if request.Method == http.MethodHead {
		if stale, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
			serveCargoObject(w, request, stale, "STALE")
		} else {
			h.forwardRead(w, request, "config.json")
		}
		return
	}
	flight, leader := h.flights.Begin("ref:" + key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if joined, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
			serveCargoObject(w, request, joined, "COALESCED")
			return
		}
		h.forwardRead(w, request, "config.json")
		return
	}
	defer h.flights.Finish("ref:"+key, flight, nil)
	if current, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
		if object == nil || current.Fetched.After(object.Fetched) {
			serveCargoObject(w, request, current, "HIT")
			return
		}
		_ = current.Reader.Close()
	}
	upstreamHeader := request.Header.Clone()
	if object != nil && object.Origin == h.origin.String() {
		if value := object.Header.Get("X-Source-ETag"); value != "" {
			upstreamHeader.Set("If-None-Match", value)
		}
		if value := object.Header.Get("X-Source-Last-Modified"); value != "" {
			upstreamHeader.Set("If-Modified-Since", value)
		}
	}
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, "config.json", upstreamHeader)
	if err != nil {
		if stale, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
			serveCargoObject(w, request, stale, "STALE")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotModified && object != nil {
		_ = storeio.TouchResponse(h.lifecycle.Context(), h.store, cargoTenant, key, nil)
		if refreshed, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
			serveCargoObject(w, request, refreshed, "REVALIDATED")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode >= http.StatusInternalServerError && object != nil {
		if stale, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
			serveCargoObject(w, request, stale, "STALE")
			return
		}
	}
	if response.StatusCode != http.StatusOK {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	if !cargoCacheable(request, response) {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	spool, err := h.client.EnsureSpooler(h.workDir).SpoolWithExpectedSize(h.lifecycle.Context(), response.Body, maxCargoConfigSize, response.ContentLength)
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = spool.Close() }()
	body, err := io.ReadAll(spool.File)
	if err != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	var configDocument map[string]json.RawMessage
	if err := json.Unmarshal(body, &configDocument); err != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	var download string
	if err := json.Unmarshal(configDocument["dl"], &download); err != nil || download == "" {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	authRequired := false
	_ = json.Unmarshal(configDocument["auth-required"], &authRequired)
	state := registryState{Download: download, AuthRequired: authRequired}
	if err := storeio.WriteJSON(h.stateDir, stateName(scope), state); err != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	configDocument["dl"], _ = json.Marshal(strings.TrimRight(externalBase, "/") + "/-/crate/{crate}/{version}/download")
	rewritten, err := json.Marshal(configDocument)
	if err != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	digest := sha256.Sum256(rewritten)
	header := response.Header.Clone()
	header.Del("Content-Encoding")
	header.Del("Content-Length")
	header.Del("Content-MD5")
	header.Del("Digest")
	header.Set("X-Source-ETag", response.Header.Get("ETag"))
	header.Set("X-Source-Last-Modified", response.Header.Get("Last-Modified"))
	header.Set("Content-Type", "application/json")
	header.Set("ETag", `"sha256-`+hex.EncodeToString(digest[:])+`"`)
	if err := storeio.PutResponse(h.lifecycle.Context(), h.store, cargoTenant, key, h.origin.String(), http.StatusOK, header, hex.EncodeToString(digest[:]), bytes.NewReader(rewritten)); err != nil {
		writeCargoBytes(w, request, header, rewritten, "BYPASS")
		return
	}
	result := "MISS"
	if object != nil {
		result = "REFRESH"
	}
	writeCargoBytes(w, request, header, rewritten, result)
}

func (h *handler) serveSparseIndex(w http.ResponseWriter, request *http.Request, cleaned string) {
	scope := cargoCredentialScope(request)
	key := cargoRefKey("index", h.origin.String(), scope, cleaned)
	object, _ := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key)
	if object != nil {
		if time.Since(object.Fetched) < cargoRefFreshness && !transport.RequestForcesRevalidation(request) {
			serveCargoObject(w, request, object, "HIT")
			return
		}
		_ = object.Reader.Close()
	}
	if request.Method == http.MethodHead {
		if stale, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
			serveCargoObject(w, request, stale, "STALE")
		} else {
			h.forwardRead(w, request, cleaned)
		}
		return
	}
	flight, leader := h.flights.Begin("ref:" + key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if joined, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
			serveCargoObject(w, request, joined, "COALESCED")
			return
		}
		h.forwardRead(w, request, cleaned)
		return
	}
	defer h.flights.Finish("ref:"+key, flight, nil)
	if current, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
		if object == nil || current.Fetched.After(object.Fetched) {
			serveCargoObject(w, request, current, "HIT")
			return
		}
		_ = current.Reader.Close()
	}
	upstreamHeader := request.Header.Clone()
	if object != nil && object.Origin == h.origin.String() {
		if value := object.Header.Get("ETag"); value != "" {
			upstreamHeader.Set("If-None-Match", value)
		}
		if value := object.Header.Get("Last-Modified"); value != "" {
			upstreamHeader.Set("If-Modified-Since", value)
		}
	}
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, upstreamHeader)
	if err != nil {
		if stale, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
			serveCargoObject(w, request, stale, "STALE")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotModified && object != nil {
		_ = storeio.TouchResponse(h.lifecycle.Context(), h.store, cargoTenant, key, response.Header)
		if refreshed, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
			serveCargoObject(w, request, refreshed, "REVALIDATED")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode >= http.StatusInternalServerError && object != nil {
		if stale, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
			serveCargoObject(w, request, stale, "STALE")
			return
		}
	}
	if response.StatusCode != http.StatusOK {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	if !cargoCacheable(request, response) {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	spool, err := h.client.EnsureSpooler(h.workDir).SpoolWithExpectedSize(h.lifecycle.Context(), response.Body, maxSparseIndexSize, response.ContentLength)
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = spool.Close() }()
	crate, parseErr := parseSparseIndex(spool.File, cleaned)
	if parseErr != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	if err := storeio.WriteJSON(h.stateDir, crateStateName(scope, crate.Name), crate); err != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	_, _ = spool.File.Seek(0, io.SeekStart)
	if err := storeio.PutResponse(h.lifecycle.Context(), h.store, cargoTenant, key, h.origin.String(), http.StatusOK, response.Header, spool.SHA256, spool.File); err != nil {
		h.writeSpooled(w, response.Header, spool.File, "BYPASS")
		return
	}
	result := "MISS"
	if object != nil {
		result = "REFRESH"
	}
	h.writeSpooled(w, response.Header, spool.File, result)
}

func (h *handler) serveCrate(w http.ResponseWriter, request *http.Request, route string) {
	parts := strings.Split(route, "/")
	if len(parts) != 3 || parts[2] != "download" || parts[0] == "" || parts[1] == "" {
		http.NotFound(w, request)
		return
	}
	name, version := strings.ToLower(parts[0]), parts[1]
	scope := cargoCredentialScope(request)
	registry, err := loadRegistryState(h.stateDir, scope)
	if err != nil {
		http.NotFound(w, request)
		return
	}
	crate, err := loadCrateState(h.stateDir, scope, name)
	if err != nil {
		http.NotFound(w, request)
		return
	}
	checksum := strings.ToLower(crate.Checksums[version])
	if len(checksum) != sha256.Size*2 {
		http.NotFound(w, request)
		return
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		http.NotFound(w, request)
		return
	}
	key := "crates/sha256/" + checksum
	if object, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
		serveCargoObject(w, request, object, "HIT")
		return
	}
	target, err := cargoDownloadURL(registry.Download, name, version, checksum)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		header := http.Header{}
		for _, name := range []string{"Accept", "If-Range", "Range", "User-Agent"} {
			for _, value := range request.Header.Values(name) {
				header.Add(name, value)
			}
		}
		if strings.EqualFold(target.Host, h.origin.Host) {
			copyCargoCredential(header, request.Header)
		}
		status, err := transport.ForwardReadTarget(request.Context(), h.client, target, w, request, header)
		if err != nil && status == 0 {
			transport.WriteError(w, http.StatusBadGateway)
		}
		return
	}
	flight, leader := h.flights.Begin(key)
	if leader {
		if object, err := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); err == nil {
			h.flights.Finish(key, flight, nil)
			serveCargoObject(w, request, object, "HIT")
			return
		}
		upstreamRequest, err := http.NewRequestWithContext(h.lifecycle.Context(), http.MethodGet, target.String(), nil)
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteError(w, http.StatusBadGateway)
			return
		}
		if strings.EqualFold(target.Host, h.origin.Host) {
			copyCargoCredential(upstreamRequest.Header, request.Header)
		}
		response, err := h.client.DoRead(h.lifecycle.Context(), upstreamRequest, transport.AdmissionForeground)
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteError(w, http.StatusBadGateway)
			return
		}
		if response.StatusCode != http.StatusOK {
			h.flights.Finish(key, flight, nil)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		if !cargoCacheable(request, response) {
			h.flights.Finish(key, flight, nil)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		header := response.Header.Clone()
		header.Del("Content-Length")
		reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
			Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
			VerifyFn: func(reader io.ReadSeeker) error {
				digest := sha256.New()
				if _, err := io.Copy(digest, reader); err != nil {
					return err
				}
				if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), checksum) {
					return errors.New("cargo crate checksum mismatch")
				}
				return nil
			},
			StoreFn: func(ctx context.Context, body io.Reader) error {
				return storeio.PutResponse(ctx, h.store, cargoTenant, key, target.Scheme+"://"+target.Host, http.StatusOK, response.Header, checksum, body)
			}, Done: func(err error) { h.flights.Finish(key, flight, err) },
		})
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		transport.CopyEndToEndHeaders(w.Header(), header)
		w.Header().Set("X-Cache", "MISS")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, reader)
		_ = reader.Close()
		return
	}
	if err := h.flights.Wait(request.Context(), flight); err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if object, openErr := storeio.OpenResponse(request.Context(), h.store, cargoTenant, key); openErr == nil {
		serveCargoObject(w, request, object, "COALESCED")
		return
	}
	transport.WriteError(w, http.StatusBadGateway)
}

func parseSparseIndex(reader io.Reader, cleaned string) (crateState, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, maxSparseLineSize)
	state := crateState{Checksums: make(map[string]string)}
	for scanner.Scan() {
		var record sparseRecord
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		if err := decoder.Decode(&record); err != nil || record.Name == "" || record.Version == "" || len(record.Checksum) != sha256.Size*2 {
			return crateState{}, errors.New("invalid cargo sparse index record")
		}
		name := strings.ToLower(record.Name)
		if state.Name == "" {
			state.Name = name
		}
		if state.Name != name || cratePrefix(name)+"/"+name != cleaned {
			return crateState{}, errors.New("cargo sparse index path does not match crate name")
		}
		if _, err := hex.DecodeString(record.Checksum); err != nil {
			return crateState{}, errors.New("invalid cargo crate checksum")
		}
		state.Checksums[record.Version] = strings.ToLower(record.Checksum)
	}
	if err := scanner.Err(); err != nil {
		return crateState{}, err
	}
	if state.Name == "" {
		return crateState{}, errors.New("empty cargo sparse index")
	}
	return state, nil
}

func isSparseIndexPath(cleaned string) bool {
	name := strings.ToLower(path.Base(cleaned))
	return name != "" && cratePrefix(name)+"/"+name == cleaned
}

func isCargoGitReadRequest(cleaned string, request *http.Request) bool {
	services, serviceSet := request.URL.Query()["service"]
	if request.Method == http.MethodGet && cleaned == "info/refs" && len(services) == 1 && services[0] == "git-upload-pack" {
		return true
	}
	return (request.Method == http.MethodGet || request.Method == http.MethodHead) && (!serviceSet || len(services) == 1 && services[0] == "") &&
		(cleaned == "HEAD" || cleaned == "info/refs" || cleaned == "objects/info/packs" ||
			cleaned == "objects/info/alternates" || cleaned == "objects/info/http-alternates" || strings.HasPrefix(cleaned, "objects/"))
}

func isCargoGitUploadPack(method, cleaned string) bool {
	return method == http.MethodPost && cleaned == "git-upload-pack"
}

func cargoDownloadURL(template, name, version, checksum string) (*url.URL, error) {
	value := template
	if strings.Contains(value, "{") {
		replacements := map[string]string{
			"{crate}": url.PathEscape(name), "{version}": url.PathEscape(version),
			"{prefix}": cratePrefix(name), "{lowerprefix}": strings.ToLower(cratePrefix(name)), "{sha256-checksum}": checksum,
		}
		for marker, replacement := range replacements {
			value = strings.ReplaceAll(value, marker, replacement)
		}
	} else {
		value = strings.TrimRight(value, "/") + "/" + url.PathEscape(name) + "/" + url.PathEscape(version) + "/download"
	}
	target, err := url.Parse(value)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil {
		return nil, errors.New("invalid cargo download URL")
	}
	return target, nil
}

func (h *handler) fetch(ctx context.Context, method, cleaned string, header http.Header) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), "")
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, err
	}
	transport.CopyReadRequestHeaders(request.Header, header)
	return h.client.DoRead(ctx, request, transport.AdmissionForeground)
}

func (h *handler) forwardRead(w http.ResponseWriter, request *http.Request, cleaned string) int {
	status, err := transport.ForwardRead(request.Context(), h.client, h.origin, w, request, cleaned)
	if err != nil && status == 0 {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	return status
}

func (h *handler) forwardGitUploadPack(w http.ResponseWriter, request *http.Request) {
	target, err := transport.JoinURL(h.origin, "git-upload-pack", request.URL.RawQuery)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	upstream, err := http.NewRequestWithContext(request.Context(), http.MethodPost, target.String(), request.Body)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	transport.CopyEndToEndHeaders(upstream.Header, request.Header)
	transport.SanitizeMethodOverrideHeaders(upstream.Header)
	response, err := h.client.DoReadOnlyPost(request.Context(), upstream, transport.AdmissionForeground)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	transport.WriteResponse(w, request, response, "BYPASS")
}

func (h *handler) writeSpooled(w http.ResponseWriter, header http.Header, file *os.File, result string) {
	_, _ = file.Seek(0, io.SeekStart)
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, file)
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func cargoCredentialScope(request *http.Request) string {
	credential := request.Header.Get("Authorization") + "\x00" + request.Header.Get("Cookie")
	if credential == "\x00" {
		return "anonymous"
	}
	digest := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(digest[:])
}

func copyCargoCredential(destination, source http.Header) {
	for _, name := range []string{"Authorization", "Cookie"} {
		for _, value := range source.Values(name) {
			destination.Add(name, value)
		}
	}
}

func cargoRefKey(kind string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return "refs/" + kind + "/" + hex.EncodeToString(hash.Sum(nil))
}

func cargoCacheable(request *http.Request, response *http.Response) bool {
	return transport.ResponseCacheable(response, cargoCredentialScope(request) != "anonymous")
}

func serveCargoObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, "", object.Fetched, object.Reader)
}

func writeCargoBytes(w http.ResponseWriter, request *http.Request, header http.Header, body []byte, result string) {
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
