package npm

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
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

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const maxPackumentSize = 256 << 20
const maxDistObjectSize = 1 << 20
const tarballAuthorizationTTL = 24 * time.Hour
const packumentFreshness = time.Minute

type handler struct {
	name      string
	origin    *url.URL
	workDir   string
	store     *blobfs.Store
	client    *transport.Client
	stats     *metrics.Stats
	secret    []byte
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
}

type tarballAuthorization struct {
	URL       string `json:"url"`
	Integrity string `json:"integrity,omitempty"`
	Shasum    string `json:"shasum,omitempty"`
	Package   string `json:"package"`
	Version   string `json:"version"`
	Scope     string `json:"scope"`
	Expires   int64  `json:"expires"`
}

type transformedPackument struct {
	file   *os.File
	header http.Header
	spool  *storeio.SpoolResult
}

func newHandler(name, upstream, stateDir, workDir string, store *blobfs.Store, client *transport.Client, stats *metrics.Stats) (*handler, error) {
	origin, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse npm upstream: %w", err)
	}
	secret, err := loadSigningSecret(stateDir)
	if err != nil {
		return nil, fmt.Errorf("load npm signing state: %w", err)
	}
	return &handler{name: name, origin: origin, workDir: workDir, store: store, client: client, stats: stats, secret: secret, lifecycle: storeio.NewLifecycle()}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	status, result := h.serve(w, request)
	h.stats.RecordRequest(h.name, "npm", request.Method, result, status, 0)
}

func (h *handler) serve(w http.ResponseWriter, request *http.Request) (int, string) {
	cleaned, err := npmRequestPath(request.URL)
	if err != nil {
		http.Error(w, "invalid npm path", http.StatusBadRequest)
		return http.StatusBadRequest, "ERROR"
	}
	if isNPMAuditRequest(request.Method, cleaned) {
		return h.proxyAudit(w, request, cleaned), "BYPASS"
	}
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		return http.StatusMethodNotAllowed, "REJECTED"
	}
	if strings.HasPrefix(cleaned, "-/tarball/") && (request.Method == http.MethodGet || request.Method == http.MethodHead) {
		return h.serveTarball(w, request, strings.TrimPrefix(cleaned, "-/tarball/"))
	}
	packageName, packument := packageFromPath(cleaned)
	if !packument {
		status := h.forwardRead(w, request, cleaned)
		return status, "BYPASS"
	}
	variant := "full"
	if strings.Contains(request.Header.Get("Accept"), "application/vnd.npm.install-v1+json") {
		variant = "abbreviated"
	}
	externalBase := proxyruntime.ExternalBaseURL(request)
	key := "packuments/" + variant + "/" + hashKey(h.origin.String()+"\x00"+packageName+"\x00"+request.Header.Get("Accept")+"\x00"+credentialScope(request)+"\x00"+externalBase)
	if cached, err := openObject(request.Context(), h.store, key); err == nil {
		if time.Since(cached.fetchedAt) < packumentFreshness && !transport.RequestForcesRevalidation(request) {
			return serveCached(w, request, cached, "HIT"), "HIT"
		}
		_ = cached.reader.Close()
		if request.Method == http.MethodHead {
			cached, openErr := openObject(request.Context(), h.store, key)
			if openErr == nil {
				return serveCached(w, request, cached, "STALE"), "STALE"
			}
		}
		leader, revalidated := false, false
		var direct *http.Response
		var fallback *transformedPackument
		err = h.flights.Do(request.Context(), key, func() error {
			leader = true
			response, fetchErr := h.fetchPackument(request, packageName, cached)
			if fetchErr != nil {
				return fetchErr
			}
			if response == nil {
				revalidated = true
				return touchObject(h.lifecycle.Context(), h.store, key)
			}
			if response.StatusCode >= http.StatusInternalServerError {
				defer response.Body.Close()
				return fmt.Errorf("npm packument upstream returned %d", response.StatusCode)
			}
			if response.StatusCode != http.StatusOK {
				direct = response
				return nil
			}
			if !npmCacheable(request, response) {
				_ = storeio.DeleteResponse(context.Background(), h.store, npmTenant, key)
				direct = response
				return nil
			}
			defer response.Body.Close()
			fallback, fetchErr = h.transformAndCommit(request, packageName, key, response)
			return fetchErr
		})
		if err != nil {
			if stale, openErr := openObject(request.Context(), h.store, key); openErr == nil {
				return serveCached(w, request, stale, "STALE"), "STALE"
			}
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		if leader && direct != nil {
			defer direct.Body.Close()
			return transport.WriteResponse(w, request, direct, "BYPASS"), "BYPASS"
		}
		if leader && fallback != nil {
			defer fallback.close()
			return fallback.serve(w, request, "BYPASS"), "BYPASS"
		}
		refreshed, openErr := openObject(request.Context(), h.store, key)
		if openErr != nil {
			return h.forwardRead(w, request, cleaned), "BYPASS"
		}
		result := "COALESCED"
		if leader && revalidated {
			result = "REVALIDATED"
		} else if leader {
			result = "REFRESH"
		}
		return serveCached(w, request, refreshed, result), result
	}

	leader := false
	var fallback *transformedPackument
	directStatus := 0
	err = h.flights.Do(request.Context(), key, func() error {
		leader = true
		response, err := h.fetchPackument(request, packageName, nil)
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			directStatus = transport.WriteResponse(w, request, response, "BYPASS")
			return nil
		}
		if !npmCacheable(request, response) {
			directStatus = transport.WriteResponse(w, request, response, "BYPASS")
			return nil
		}
		fallback, err = h.transformAndCommit(request, packageName, key, response)
		if storeio.SpoolBodyUntouched(err) {
			directStatus = transport.WriteResponse(w, request, response, "BYPASS")
			return nil
		}
		return err
	})
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if leader && directStatus != 0 {
		return directStatus, "BYPASS"
	}
	if leader && fallback != nil {
		defer fallback.close()
		return fallback.serve(w, request, "BYPASS"), "BYPASS"
	}
	cached, err := openObject(request.Context(), h.store, key)
	if err != nil {
		return h.forwardRead(w, request, cleaned), "BYPASS"
	}
	return serveCached(w, request, cached, "MISS"), "MISS"
}

func (h *handler) fetchPackument(request *http.Request, packageName string, cached *cachedObject) (*http.Response, error) {
	escaped := packageName
	if strings.HasPrefix(packageName, "@") {
		escaped = strings.ReplaceAll(url.PathEscape(packageName), "%2F", "%2f")
	}
	target, err := transport.JoinURL(h.origin, escaped, request.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	upstreamRequest, err := http.NewRequestWithContext(h.lifecycle.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	upstreamRequest.Header.Set("Accept", request.Header.Get("Accept"))
	copyCredential(upstreamRequest.Header, request.Header)
	if cached != nil && cached.origin == h.origin.String() {
		if etag := cached.headers.Get("X-Source-ETag"); etag != "" {
			upstreamRequest.Header.Set("If-None-Match", etag)
		}
		if modified := cached.headers.Get("X-Source-Last-Modified"); modified != "" {
			upstreamRequest.Header.Set("If-Modified-Since", modified)
		}
	}
	response, err := h.client.DoRead(h.lifecycle.Context(), upstreamRequest, transport.AdmissionForeground)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusNotModified && cached != nil {
		_ = response.Body.Close()
		return nil, nil
	}
	return response, nil
}

func (h *handler) transformAndCommit(request *http.Request, packageName, key string, response *http.Response) (*transformedPackument, error) {
	source, err := h.client.EnsureSpooler(h.workDir).SpoolWithExpectedSize(h.lifecycle.Context(), response.Body, maxPackumentSize, response.ContentLength)
	if err != nil {
		return nil, err
	}
	keepSource := false
	defer func() {
		if !keepSource {
			_ = source.Close()
		}
	}()
	originalFallback := func() (*transformedPackument, error) {
		if _, err := source.File.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		keepSource = true
		return &transformedPackument{file: source.File, header: response.Header.Clone(), spool: source}, nil
	}
	output, err := os.CreateTemp(h.workDir, ".cache-proxy-tmp-npm-packument-*")
	if err != nil {
		return originalFallback()
	}
	removeOutput := true
	defer func() {
		if removeOutput {
			_ = output.Close()
			_ = os.Remove(output.Name())
		}
	}()
	if err := h.rewritePackument(source.File, output, packageName, proxyruntime.ExternalBaseURL(request), credentialScope(request)); err != nil {
		return originalFallback()
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		return originalFallback()
	}
	digest := sha256.New()
	size, err := io.Copy(digest, output)
	if err != nil {
		return originalFallback()
	}
	headers := response.Header.Clone()
	headers.Set("Content-Length", fmt.Sprintf("%d", size))
	headers.Set("Content-Type", "application/json")
	headers.Set("X-Source-ETag", response.Header.Get("ETag"))
	headers.Set("X-Source-Last-Modified", response.Header.Get("Last-Modified"))
	headers.Set("ETag", `"sha256-`+hex.EncodeToString(digest.Sum(nil))+`"`)
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		return originalFallback()
	}
	if err := putObject(h.lifecycle.Context(), h.store, key, h.origin.String(), headers, output); err != nil {
		if _, seekErr := output.Seek(0, io.SeekStart); seekErr != nil {
			return nil, errors.Join(err, seekErr)
		}
		removeOutput = false
		return &transformedPackument{file: output, header: headers}, nil
	}
	return nil, nil
}

func (h *handler) rewritePackument(source io.Reader, destination io.Writer, packageName, externalBase, scope string) error {
	decoder := json.NewDecoder(source)
	decoder.UseNumber()
	writer := bufio.NewWriterSize(destination, 32<<10)
	if err := h.rewriteJSONValue(decoder, writer, packageName, externalBase, scope, "", false); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected JSON token %v", token)
	}
	return writer.Flush()
}

func (h *handler) rewriteJSONValue(decoder *json.Decoder, writer io.Writer, packageName, externalBase, scope, version string, versionsObject bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		encoded, err := json.Marshal(token)
		if err == nil {
			_, err = writer.Write(encoded)
		}
		return err
	}
	_, _ = io.WriteString(writer, string(delimiter))
	switch delimiter {
	case '{':
		first := true
		for decoder.More() {
			if !first {
				_, _ = io.WriteString(writer, ",")
			}
			first = false
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("npm object key is not a string")
			}
			encodedKey, _ := json.Marshal(key)
			_, _ = writer.Write(encodedKey)
			_, _ = io.WriteString(writer, ":")
			childVersion := version
			if versionsObject {
				childVersion = key
			}
			if key == "dist" {
				var raw json.RawMessage
				if err := decoder.Decode(&raw); err != nil {
					return err
				}
				if len(raw) > maxDistObjectSize {
					return errors.New("npm dist object exceeds size limit")
				}
				rewritten, err := h.rewriteDist(raw, packageName, childVersion, externalBase, scope)
				if err != nil {
					return err
				}
				_, _ = writer.Write(rewritten)
				continue
			}
			if err := h.rewriteJSONValue(decoder, writer, packageName, externalBase, scope, childVersion, key == "versions"); err != nil {
				return err
			}
		}
	case '[':
		first := true
		for decoder.More() {
			if !first {
				_, _ = io.WriteString(writer, ",")
			}
			first = false
			if err := h.rewriteJSONValue(decoder, writer, packageName, externalBase, scope, version, false); err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid npm JSON delimiter")
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	_, err = io.WriteString(writer, string(closing.(json.Delim)))
	return err
}

func (h *handler) rewriteDist(raw json.RawMessage, packageName, version, externalBase, scope string) ([]byte, error) {
	var dist map[string]json.RawMessage
	if err := json.Unmarshal(raw, &dist); err != nil {
		return nil, err
	}
	var target, integrity, shasum string
	_ = json.Unmarshal(dist["tarball"], &target)
	_ = json.Unmarshal(dist["integrity"], &integrity)
	_ = json.Unmarshal(dist["shasum"], &shasum)
	if target == "" {
		return raw, nil
	}
	authorization := tarballAuthorization{
		URL: target, Integrity: integrity, Shasum: shasum, Package: packageName, Version: version,
		Scope: scope, Expires: time.Now().Add(tarballAuthorizationTTL).Unix(),
	}
	token, err := h.signAuthorization(authorization)
	if err != nil {
		return nil, err
	}
	rewritten := strings.TrimRight(externalBase, "/") + "/-/tarball/" + token + "/" + url.PathEscape(path.Base(target))
	dist["tarball"], _ = json.Marshal(rewritten)
	return json.Marshal(dist)
}

func (h *handler) signAuthorization(authorization tarballAuthorization) (string, error) {
	payload, err := json.Marshal(authorization)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (h *handler) verifyAuthorization(token string) (tarballAuthorization, error) {
	payloadToken, signatureToken, ok := strings.Cut(token, ".")
	if !ok {
		return tarballAuthorization{}, errors.New("invalid npm tarball authorization")
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadToken)
	if err != nil || len(payload) > 16<<10 {
		return tarballAuthorization{}, errors.New("invalid npm tarball authorization")
	}
	signature, err := base64.RawURLEncoding.DecodeString(signatureToken)
	if err != nil {
		return tarballAuthorization{}, errors.New("invalid npm tarball authorization")
	}
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return tarballAuthorization{}, errors.New("invalid npm tarball authorization")
	}
	var authorization tarballAuthorization
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&authorization); err != nil || time.Now().Unix() > authorization.Expires ||
		authorization.URL == "" || authorization.Package == "" || authorization.Version == "" || authorization.Scope == "" {
		return tarballAuthorization{}, errors.New("expired npm tarball authorization")
	}
	target, err := url.Parse(authorization.URL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil {
		return tarballAuthorization{}, errors.New("invalid npm tarball target")
	}
	return authorization, nil
}

func (h *handler) serveTarball(w http.ResponseWriter, request *http.Request, route string) (int, string) {
	token, _, _ := strings.Cut(route, "/")
	authorization, err := h.verifyAuthorization(token)
	if err != nil || authorization.Scope != credentialScope(request) {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return http.StatusForbidden, "ERROR"
	}
	key := "tarballs/url/" + hashKey(authorization.URL+"\x00"+credentialScope(request))
	if authorization.Integrity != "" {
		key = "tarballs/integrity/" + hashKey(authorization.Integrity+"\x00"+credentialScope(request))
	}
	if cached, err := openObject(request.Context(), h.store, key); err == nil {
		if authorization.Integrity != "" || authorization.Shasum != "" || time.Since(cached.fetchedAt) < time.Hour {
			return serveCached(w, request, cached, "HIT"), "HIT"
		}
		_ = cached.reader.Close()
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
			copyCredential(header, request.Header)
		}
		status, err := transport.ForwardReadTarget(request.Context(), h.client, target, w, request, header)
		if err != nil {
			if status == 0 {
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			return status, "ERROR"
		}
		return status, "BYPASS"
	}
	flight, leader := h.flights.Begin(key)
	if leader {
		if cached, err := openObject(request.Context(), h.store, key); err == nil {
			if authorization.Integrity != "" || authorization.Shasum != "" || time.Since(cached.fetchedAt) < time.Hour {
				h.flights.Finish(key, flight, nil)
				return serveCached(w, request, cached, "HIT"), "HIT"
			}
			_ = cached.reader.Close()
		}
		upstreamRequest, err := http.NewRequestWithContext(h.lifecycle.Context(), http.MethodGet, target.String(), nil)
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		if strings.EqualFold(target.Host, h.origin.Host) {
			copyCredential(upstreamRequest.Header, request.Header)
		}
		response, err := h.client.DoRead(h.lifecycle.Context(), upstreamRequest, transport.AdmissionForeground)
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		if response.StatusCode != http.StatusOK {
			h.flights.Finish(key, flight, nil)
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		if !npmCacheable(request, response) {
			h.flights.Finish(key, flight, nil)
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		header := response.Header.Clone()
		header.Del("Content-Length")
		reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
			Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
			VerifyFn: func(reader io.ReadSeeker) error { return verifyTarball(reader, authorization) },
			StoreFn: func(ctx context.Context, body io.Reader) error {
				return putObject(ctx, h.store, key, target.Scheme+"://"+target.Host, response.Header, body)
			},
			Done: func(err error) { h.flights.Finish(key, flight, err) },
		})
		if err != nil {
			h.flights.Finish(key, flight, err)
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		defer func() { _ = reader.Close() }()
		transport.CopyEndToEndHeaders(w.Header(), header)
		w.Header().Set("X-Cache", "MISS")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, reader)
		return http.StatusOK, "MISS"
	}
	if err := h.flights.Wait(request.Context(), flight); err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	cached, err := openObject(request.Context(), h.store, key)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	return serveCached(w, request, cached, "COALESCED"), "COALESCED"
}

func verifyTarball(reader io.ReadSeeker, authorization tarballAuthorization) error {
	for _, value := range strings.Fields(authorization.Integrity) {
		algorithm, expected, ok := strings.Cut(value, "-")
		if !ok || algorithm != "sha512" {
			continue
		}
		digest := sha512.New()
		_, _ = reader.Seek(0, io.SeekStart)
		if _, err := io.Copy(digest, reader); err != nil {
			return err
		}
		if !hmac.Equal([]byte(base64.StdEncoding.EncodeToString(digest.Sum(nil))), []byte(expected)) {
			return errors.New("npm tarball integrity mismatch")
		}
		return nil
	}
	if authorization.Shasum != "" {
		digest := sha1.New()
		_, _ = reader.Seek(0, io.SeekStart)
		if _, err := io.Copy(digest, reader); err != nil {
			return err
		}
		if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), authorization.Shasum) {
			return errors.New("npm tarball shasum mismatch")
		}
	}
	return nil
}

func (h *handler) forwardRead(w http.ResponseWriter, request *http.Request, cleaned string) int {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target.String(), nil)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	transport.CopyReadRequestHeaders(upstreamRequest.Header, request.Header)
	response, err := h.client.DoRead(request.Context(), upstreamRequest, transport.AdmissionForeground)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	return transport.WriteResponse(w, request, response, "BYPASS")
}

func (h *handler) proxyAudit(w http.ResponseWriter, request *http.Request, cleaned string) int {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), http.MethodPost, target.String(), request.Body)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	transport.CopyEndToEndHeaders(upstreamRequest.Header, request.Header)
	transport.SanitizeMethodOverrideHeaders(upstreamRequest.Header)
	response, err := h.client.DoReadOnlyPost(request.Context(), upstreamRequest, transport.AdmissionForeground)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	return transport.WriteResponse(w, request, response, "BYPASS")
}

func isNPMAuditRequest(method, cleaned string) bool {
	if method != http.MethodPost {
		return false
	}
	return cleaned == "-/npm/v1/security/advisories/bulk" || cleaned == "-/npm/v1/security/audits/quick"
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func packageFromPath(cleaned string) (string, bool) {
	if strings.HasPrefix(cleaned, "-/") || strings.Contains(cleaned, "/-/") {
		return "", false
	}
	parts := strings.Split(cleaned, "/")
	if strings.HasPrefix(cleaned, "@") {
		return cleaned, len(parts) == 2 && parts[0] != "" && parts[1] != ""
	}
	return cleaned, len(parts) == 1
}

func npmRequestPath(target *url.URL) (string, error) {
	if target == nil {
		return "", errors.New("missing npm request URL")
	}
	escaped := strings.TrimPrefix(target.EscapedPath(), "/")
	if escaped == "" || strings.HasPrefix(escaped, "/") {
		return "", errors.New("invalid npm request path")
	}
	parts := strings.Split(escaped, "/")
	decoded := make([]string, len(parts))
	for index, part := range parts {
		if part == "" {
			return "", errors.New("invalid npm request path")
		}
		segment, err := url.PathUnescape(part)
		if err != nil || segment == "" || segment == "." || segment == ".." || strings.Contains(segment, "\\") || strings.ContainsRune(segment, '\x00') {
			return "", errors.New("invalid npm request path")
		}
		if strings.Contains(segment, "/") && (len(parts) != 1 || !strings.HasPrefix(segment, "@") || strings.Count(segment, "/") != 1) {
			return "", errors.New("invalid npm request path")
		}
		decoded[index] = segment
	}
	return strings.Join(decoded, "/"), nil
}

func credentialScope(request *http.Request) string {
	value := request.Header.Get("Authorization") + "\x00" + request.Header.Get("Cookie")
	if value == "\x00" {
		return "anonymous"
	}
	return hashKey(value)
}

func copyCredential(destination, source http.Header) {
	for _, name := range []string{"Authorization", "Cookie", "Npm-Auth-Type", "Npm-Scope"} {
		for _, value := range source.Values(name) {
			destination.Add(name, value)
		}
	}
}

func npmCacheable(request *http.Request, response *http.Response) bool {
	return transport.ResponseCacheable(response, credentialScope(request) != "anonymous")
}

func (p *transformedPackument) serve(w http.ResponseWriter, request *http.Request, result string) int {
	transport.CopyEndToEndHeaders(w.Header(), p.header)
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, "", time.Now(), p.file)
	return http.StatusOK
}

func (p *transformedPackument) close() {
	if p.spool != nil {
		_ = p.spool.Close()
		p.file = nil
		p.spool = nil
		return
	}
	if p.file == nil {
		return
	}
	name := p.file.Name()
	_ = p.file.Close()
	_ = os.Remove(name)
	p.file = nil
}

func hashKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func serveCached(w http.ResponseWriter, request *http.Request, cached *cachedObject, result string) int {
	defer func() { _ = cached.reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), cached.headers)
	w.Header().Del("X-Source-ETag")
	w.Header().Del("X-Source-Last-Modified")
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, "", cached.fetchedAt, cached.reader)
	return http.StatusOK
}
