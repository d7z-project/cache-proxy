package pypi

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/net/html"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/signedtoken"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	pypiTenant           = "pypi"
	simpleFreshness      = time.Minute
	fileAuthorizationTTL = 24 * time.Hour
	maxSimpleBody        = int64(64 << 20)
	maxSimpleFiles       = 1_000_000
)

type handler struct {
	origin    *url.URL
	workDir   string
	spooler   *storeio.Spooler
	store     *blobfs.Store
	client    *transport.Client
	secret    []byte
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
}

type fileAuthorization struct {
	URL       string `json:"url"`
	Project   string `json:"project"`
	Filename  string `json:"filename"`
	Algorithm string `json:"algorithm,omitempty"`
	Digest    string `json:"digest,omitempty"`
	Scope     string `json:"scope"`
	Expires   int64  `json:"expires"`
}

func newHandler(origin *url.URL, stateDir, workDir string, store *blobfs.Store, client *transport.Client) (*handler, error) {
	secret, err := storeio.LoadOrCreateSigningSecret(stateDir)
	if err != nil {
		return nil, err
	}
	return &handler{
		origin:    origin,
		workDir:   workDir,
		spooler:   client.EnsureSpooler(workDir),
		store:     store,
		client:    client,
		secret:    secret,
		lifecycle: storeio.NewLifecycle(),
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		return
	}
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid PyPI path", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(cleaned, "-/file/") {
		h.serveFile(w, request, strings.TrimPrefix(cleaned, "-/file/"))
		return
	}
	project, simple := simpleProject(cleaned)
	if !simple {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	h.serveSimple(w, request, cleaned, project)
}

func (h *handler) serveSimple(w http.ResponseWriter, request *http.Request, cleaned, project string) {
	scope := pypiCredentialScope(request)
	externalBase := proxyruntime.ExternalBaseURL(request)
	key := pypiRefKey(h.origin.String(), cleaned, request.URL.RawQuery, request.Header.Get("Accept"), scope, externalBase)
	object, cachedErr := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key)
	if cachedErr == nil && time.Since(object.CreatedAt) < fileAuthorizationTTL/2 && proxyruntime.ResponseFresh(object.Header, object.ValidatedAt, min(h.client.RefreshInterval(simpleFreshness), fileAuthorizationTTL/2)) && !proxyruntime.RequestForcesRevalidation(request) {
		servePyPIObject(w, request, object, "HIT")
		return
	}
	if object != nil {
		_ = object.Reader.Close()
	}
	if request.Method == http.MethodHead {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	flight, leader := h.flights.Begin("ref:" + key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if joined, err := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key); err == nil {
			if object != nil && !joined.ValidatedAt.After(object.ValidatedAt) {
				_ = joined.Reader.Close()
				h.forwardUpstream(w, request, cleaned)
				return
			}
			servePyPIObject(w, request, joined, "COALESCED")
			return
		}
		h.forwardUpstream(w, request, cleaned)
		return
	}
	defer h.flights.Finish("ref:"+key, flight, nil)
	if current, err := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key); err == nil {
		if object == nil || current.ValidatedAt.After(object.ValidatedAt) {
			servePyPIObject(w, request, current, "HIT")
			return
		}
		_ = current.Reader.Close()
	}
	upstreamHeader := request.Header.Clone()
	if object != nil && object.Origin == h.origin.String() && time.Since(object.CreatedAt) < fileAuthorizationTTL/2 {
		if value := object.Header.Get("X-Source-ETag"); value != "" {
			upstreamHeader.Set("If-None-Match", value)
		}
		if value := object.Header.Get("X-Source-Last-Modified"); value != "" {
			upstreamHeader.Set("If-Modified-Since", value)
		}
	}
	response, err := h.fetchUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, upstreamHeader)
	if err != nil || response.StatusCode >= 500 {
		if response != nil {
			_ = response.Body.Close()
		}
		if object != nil && proxyruntime.StaleAllowed(request, object.Header) {
			if stale, openErr := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key); openErr == nil {
				if !proxyruntime.StaleAllowed(request, stale.Header) || time.Since(stale.CreatedAt) >= fileAuthorizationTTL/2 {
					_ = stale.Reader.Close()
					transport.WriteError(w, http.StatusBadGateway)
					return
				}
				servePyPIObject(w, request, stale, "STALE")
				return
			}
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode == http.StatusNotModified && object != nil {
		_ = response.Body.Close()
		header := transport.SourceRevalidationHeader(response.Header)
		if refreshed, _ := storeio.RevalidateResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, pypiTenant, key, header); refreshed != nil {
			servePyPIObject(w, request, refreshed, "REVALIDATED")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	if !pypiCacheable(request, response) {
		_ = storeio.DeleteResponse(h.lifecycle.Context(), h.store, pypiTenant, key)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	defer func() { _ = response.Body.Close() }()
	spool, err := h.spooler.SpoolWithExpectedSize(h.lifecycle.Context(), response.Body, maxSimpleBody, response.ContentLength)
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = spool.Close() }()
	contentType := strings.ToLower(response.Header.Get("Content-Type"))
	output, err := os.CreateTemp(h.workDir, ".cache-proxy-tmp-pypi-simple-*")
	if err != nil {
		h.writeSpool(w, request, response.Header, spool.File, "BYPASS")
		return
	}
	defer func() { _ = output.Close(); _ = os.Remove(output.Name()) }()
	switch {
	case strings.Contains(contentType, "json"):
		err = h.rewriteSimpleJSON(spool.File, output, project, externalBase, scope)
	case strings.Contains(contentType, "html"):
		err = h.rewriteSimpleHTML(spool.File, output, project, externalBase, scope)
	default:
		err = errors.New("unsupported simple representation")
	}
	if err != nil {
		h.writeSpool(w, request, response.Header, spool.File, "BYPASS")
		return
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		h.writeSpool(w, request, response.Header, spool.File, "BYPASS")
		return
	}
	digest := sha256.New()
	size, err := io.Copy(digest, output)
	if err != nil {
		h.writeSpool(w, request, response.Header, spool.File, "BYPASS")
		return
	}
	header := response.Header.Clone()
	header.Del("Content-Encoding")
	header.Del("Content-Length")
	header.Del("Content-MD5")
	header.Del("Digest")
	header.Set("X-Source-ETag", response.Header.Get("ETag"))
	header.Set("X-Source-Last-Modified", response.Header.Get("Last-Modified"))
	header.Set("Content-Length", fmt.Sprintf("%d", size))
	header.Set("ETag", `"sha256-`+hex.EncodeToString(digest.Sum(nil))+`"`)
	_, _ = output.Seek(0, io.SeekStart)
	if err := storeio.PutResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, pypiTenant, key, h.origin.String(), http.StatusOK, header, hex.EncodeToString(digest.Sum(nil)), output); err != nil {
		_, _ = output.Seek(0, io.SeekStart)
		h.writeSpool(w, request, header, output, "BYPASS")
		return
	}
	_, _ = output.Seek(0, io.SeekStart)
	result := "MISS"
	if object != nil {
		result = "REFRESH"
	}
	h.writeSpool(w, request, header, output, result)
}

func (h *handler) rewriteSimpleJSON(source io.Reader, destination io.Writer, project, externalBase, scope string) error {
	decoder := json.NewDecoder(source)
	decoder.UseNumber()
	var document map[string]json.RawMessage
	if err := decoder.Decode(&document); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected JSON token %v", token)
	}
	if rawFiles := document["files"]; len(rawFiles) > 0 {
		var files []map[string]json.RawMessage
		if err := json.Unmarshal(rawFiles, &files); err != nil || len(files) > maxSimpleFiles {
			return errors.New("invalid PyPI Simple files")
		}
		for _, file := range files {
			var target, filename string
			_ = json.Unmarshal(file["url"], &target)
			_ = json.Unmarshal(file["filename"], &filename)
			if target == "" || filename == "" {
				continue
			}
			var hashes map[string]string
			_ = json.Unmarshal(file["hashes"], &hashes)
			algorithm, digest := strongestPyPIHash(hashes)
			rewritten, err := h.authorizeFile(target, project, filename, algorithm, digest, externalBase, scope)
			if err != nil {
				return err
			}
			file["url"], _ = json.Marshal(rewritten)
		}
		encoded, _ := json.Marshal(files)
		document["files"] = encoded
	}
	return json.NewEncoder(destination).Encode(document)
}

func (h *handler) rewriteSimpleHTML(source io.Reader, destination io.Writer, project, externalBase, scope string) error {
	tokenizer := html.NewTokenizer(source)
	for {
		tokenType := tokenizer.Next()
		if tokenType == html.ErrorToken {
			if errors.Is(tokenizer.Err(), io.EOF) {
				return nil
			}
			return tokenizer.Err()
		}
		token := tokenizer.Token()
		if tokenType == html.StartTagToken || tokenType == html.SelfClosingTagToken {
			for index := range token.Attr {
				if token.Data != "a" || token.Attr[index].Key != "href" {
					continue
				}
				target := token.Attr[index].Val
				if project == "" {
					if rewritten, ok := h.rewriteProjectURL(target, externalBase); ok {
						token.Attr[index].Val = rewritten
					}
					continue
				}
				resolved, err := h.origin.Parse(target)
				if err != nil {
					return err
				}
				algorithm, digest := hashFromFragment(resolved.Fragment)
				resolved.Fragment = ""
				filename := path.Base(resolved.Path)
				rewritten, err := h.authorizeFile(resolved.String(), project, filename, algorithm, digest, externalBase, scope)
				if err != nil {
					return err
				}
				token.Attr[index].Val = rewritten
			}
		}
		if _, err := io.WriteString(destination, token.String()); err != nil {
			return err
		}
	}
}

func (h *handler) authorizeFile(rawTarget, project, filename, algorithm, digest, externalBase, scope string) (string, error) {
	target, err := h.origin.Parse(rawTarget)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil {
		return "", errors.New("invalid PyPI file URL")
	}
	target.Fragment = ""
	authorization := fileAuthorization{URL: target.String(), Project: project, Filename: filename, Algorithm: algorithm,
		Digest: strings.ToLower(digest), Scope: scope, Expires: time.Now().Add(fileAuthorizationTTL).Unix()}
	token, err := signedtoken.Sign(h.secret, authorization)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(externalBase, "/") + "/-/file/" + token + "/" + url.PathEscape(filename), nil
}

func (h *handler) verifyAuthorization(token, scope string) (fileAuthorization, error) {
	var authorization fileAuthorization
	if err := signedtoken.Verify(h.secret, token, 32<<10, &authorization); err != nil {
		return fileAuthorization{}, errors.New("invalid PyPI file authorization")
	}
	if authorization.Scope != scope || time.Now().Unix() > authorization.Expires {
		return fileAuthorization{}, errors.New("expired PyPI file authorization")
	}
	target, err := url.Parse(authorization.URL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil {
		return fileAuthorization{}, errors.New("invalid PyPI file target")
	}
	return authorization, nil
}

func (h *handler) serveFile(w http.ResponseWriter, request *http.Request, route string) {
	token, _, _ := strings.Cut(route, "/")
	scope := pypiCredentialScope(request)
	authorization, err := h.verifyAuthorization(token, scope)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	key := "files/url/" + pypiRefKey(authorization.URL, scope)
	if authorization.Digest != "" {
		key = "files/" + authorization.Algorithm + "/" + strings.ToLower(authorization.Digest)
	}
	if object, err := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key); err == nil {
		if authorization.Digest != "" || time.Since(object.ValidatedAt) < time.Hour {
			servePyPIObject(w, request, object, "HIT")
			return
		}
		_ = object.Reader.Close()
	}
	target, _ := url.Parse(authorization.URL)
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		header := http.Header{}
		for _, name := range []string{"Accept", "If-Range", "Range", "User-Agent"} {
			for _, value := range request.Header.Values(name) {
				header.Add(name, value)
			}
		}
		if strings.EqualFold(target.Host, h.origin.Host) {
			copyPyPICredential(header, request.Header)
		}
		status, err := transport.ForwardReadTarget(request.Context(), h.client, target, w, request, header)
		if err != nil && status == 0 {
			transport.WriteError(w, http.StatusBadGateway)
		}
		return
	}
	flight, leader := h.flights.Begin(key)
	if leader {
		if object, err := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key); err == nil {
			if authorization.Digest != "" || time.Since(object.ValidatedAt) < time.Hour {
				h.flights.Finish(key, flight, nil)
				servePyPIObject(w, request, object, "HIT")
				return
			}
			_ = object.Reader.Close()
		}
		upstreamRequest, err := http.NewRequestWithContext(h.lifecycle.Context(), http.MethodGet, target.String(), nil)
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteError(w, http.StatusBadGateway)
			return
		}
		if strings.EqualFold(target.Host, h.origin.Host) {
			copyPyPICredential(upstreamRequest.Header, request.Header)
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
		if !pypiCacheable(request, response) {
			h.flights.Finish(key, flight, nil)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		header := response.Header.Clone()
		header.Del("Content-Length")
		reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
			Body: response.Body, ObjectPath: key, Spooler: h.spooler, Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
			VerifyFn: func(reader io.ReadSeeker) error { return verifyPyPIFile(reader, authorization) },
			StoreFn: func(ctx context.Context, body io.Reader) error {
				return storeio.PutResponse(storeio.WithResponseTiming(ctx, response), h.store, pypiTenant, key, target.Scheme+"://"+target.Host, http.StatusOK, response.Header, authorization.Digest, body)
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
	if object, openErr := storeio.OpenResponse(request.Context(), h.store, pypiTenant, key); openErr == nil {
		servePyPIObject(w, request, object, "COALESCED")
		return
	}
	transport.WriteError(w, http.StatusBadGateway)
}

func verifyPyPIFile(reader io.ReadSeeker, authorization fileAuthorization) error {
	if authorization.Digest == "" {
		return nil
	}
	var digest hash.Hash
	switch authorization.Algorithm {
	case "sha256":
		digest = sha256.New()
	case "sha512":
		digest = sha512.New()
	default:
		return errors.New("unsupported PyPI file hash")
	}
	if _, err := io.Copy(digest, reader); err != nil {
		return err
	}
	if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), authorization.Digest) {
		return errors.New("pypi file hash mismatch")
	}
	return nil
}

func strongestPyPIHash(hashes map[string]string) (string, string) {
	for _, algorithm := range []string{"sha512", "sha256"} {
		if digest := hashes[algorithm]; digest != "" {
			return algorithm, digest
		}
	}
	return "", ""
}

func hashFromFragment(fragment string) (string, string) {
	algorithm, digest, ok := strings.Cut(fragment, "=")
	if !ok || algorithm != "sha256" && algorithm != "sha512" {
		return "", ""
	}
	return algorithm, digest
}

func simpleProject(cleaned string) (string, bool) {
	trimmed := strings.TrimSuffix(cleaned, "/")
	if trimmed == "simple" {
		return "", true
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] != "simple" || parts[1] == "" {
		return "", false
	}
	return normalizeProjectName(parts[1]), true
}

func (h *handler) rewriteProjectURL(rawTarget, externalBase string) (string, bool) {
	target, err := h.origin.Parse(rawTarget)
	if err != nil || !strings.EqualFold(target.Host, h.origin.Host) {
		return "", false
	}
	parts := strings.Split(strings.Trim(target.Path, "/"), "/")
	if len(parts) < 2 || parts[len(parts)-2] != "simple" {
		return "", false
	}
	return strings.TrimRight(externalBase, "/") + "/simple/" + url.PathEscape(normalizeProjectName(parts[len(parts)-1])) + "/", true
}

func (h *handler) fetchUpstream(ctx context.Context, method, cleaned, rawQuery string, header http.Header) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), rawQuery)
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

func (h *handler) forwardUpstream(w http.ResponseWriter, request *http.Request, cleaned string) {
	status, err := transport.ForwardRead(request.Context(), h.client, h.origin, w, request, cleaned)
	if err != nil && status == 0 {
		transport.WriteError(w, http.StatusBadGateway)
	}
}

func (h *handler) writeSpool(w http.ResponseWriter, request *http.Request, header http.Header, file *os.File, result string) {
	_, _ = file.Seek(0, io.SeekStart)
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = io.Copy(w, file)
	}
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func pypiCredentialScope(request *http.Request) string {
	value := request.Header.Get("Authorization") + "\x00" + request.Header.Get("Cookie")
	if value == "\x00" {
		return "anonymous"
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func copyPyPICredential(destination, source http.Header) {
	for _, name := range []string{"Authorization", "Cookie"} {
		for _, value := range source.Values(name) {
			destination.Add(name, value)
		}
	}
}

func pypiRefKey(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return "refs/" + hex.EncodeToString(hash.Sum(nil))
}

func pypiCacheable(request *http.Request, response *http.Response) bool {
	return transport.ResponseCacheable(response, pypiCredentialScope(request) != "anonymous")
}

func servePyPIObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.ResponseHeader())
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, "", object.ValidatedAt, object.Reader)
}
