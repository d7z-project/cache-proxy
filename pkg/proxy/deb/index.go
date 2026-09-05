package deb

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type indexDescriptor struct {
	FetchPath    string
	FallbackPath string
	CachePaths   []string
	Size         int64
	SHA256       string
	SHA512       string
}

func parseByHashPath(cleaned string) (indexDescriptor, bool) {
	if relative, err := storeio.CleanRelative(cleaned); err != nil || relative != cleaned {
		return indexDescriptor{}, false
	}
	directory, digest := path.Split(cleaned)
	algorithm := path.Base(strings.TrimSuffix(directory, "/"))
	if path.Base(path.Dir(strings.TrimSuffix(directory, "/"))) != "by-hash" {
		return indexDescriptor{}, false
	}
	want := sha256.Size * 2
	if algorithm == "SHA512" {
		want = sha512.Size * 2
	} else if algorithm != "SHA256" {
		return indexDescriptor{}, false
	}
	if len(digest) != want || digest != strings.ToLower(digest) {
		return indexDescriptor{}, false
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return indexDescriptor{}, false
	}
	descriptor := indexDescriptor{FetchPath: cleaned, CachePaths: []string{cleaned}, Size: -1}
	if algorithm == "SHA256" {
		descriptor.SHA256 = digest
	} else {
		descriptor.SHA512 = digest
	}
	return descriptor, true
}

func (m releaseManifest) resolveIndex(root, cleaned string) (indexDescriptor, bool, error) {
	var selected *releaseEntry
	canonicalRequest := false
	matches := 0
	for i := range m.Entries {
		entry := &m.Entries[i]
		canonical := joinRoot(root, entry.Path)
		if cleaned == canonical {
			selected, canonicalRequest = entry, true
			break
		}
		if entry.SHA256 != "" && cleaned == releaseByHashPath(canonical, "SHA256", entry.SHA256) ||
			entry.SHA512 != "" && cleaned == releaseByHashPath(canonical, "SHA512", entry.SHA512) {
			if selected != nil && (selected.Size != entry.Size || selected.SHA256 != entry.SHA256 || selected.SHA512 != entry.SHA512) {
				return indexDescriptor{}, false, errors.New("conflicting Release by-hash entries")
			}
			selected = entry
			matches++
		}
	}
	if selected == nil {
		return indexDescriptor{}, false, nil
	}
	canonical := joinRoot(root, selected.Path)
	descriptor := indexDescriptor{Size: selected.Size, SHA256: selected.SHA256, SHA512: selected.SHA512}
	if selected.SHA512 != "" {
		descriptor.CachePaths = append(descriptor.CachePaths, releaseByHashPath(canonical, "SHA512", selected.SHA512))
	}
	if selected.SHA256 != "" {
		descriptor.CachePaths = append(descriptor.CachePaths, releaseByHashPath(canonical, "SHA256", selected.SHA256))
	}
	descriptor.FetchPath = cleaned
	if canonicalRequest {
		descriptor.FetchPath = descriptor.CachePaths[0]
		for _, entry := range m.Entries {
			if path.Dir(entry.Path) == path.Dir(selected.Path) &&
				(entry.SHA256 != "" && entry.SHA256 == selected.SHA256 || entry.SHA512 != "" && entry.SHA512 == selected.SHA512) {
				matches++
			}
		}
	}
	if matches <= 1 {
		descriptor.FallbackPath = canonical
	} else if canonicalRequest {
		// A canonical fallback must not establish availability of a different entry.
		descriptor.FallbackPath = canonical
		descriptor.CachePaths = []string{canonical + "\x00" + descriptor.FetchPath}
	} else {
		descriptor.CachePaths = []string{cleaned}
	}
	return descriptor, true, nil
}

func (h *handler) serveIndex(w http.ResponseWriter, request *http.Request, cleaned string) (int, string, bool) {
	descriptor, byHash := parseByHashPath(cleaned)
	lease := h.metadata.AcquireSnapshots(cleaned)
	if lease != nil {
		defer lease.Close()
		for i, snapshot := range lease.Snapshots {
			if i > 0 && !byHash {
				break
			}
			reader, err := lease.OpenAnchor(request.Context(), snapshot)
			if err != nil {
				continue
			}
			manifest, parseErr := parseReleaseManifest(request.Context(), reader)
			closeErr := reader.Close()
			if err := errors.Join(parseErr, closeErr); err != nil {
				continue
			}
			if !manifest.AcquireByHash {
				if !byHash {
					return 0, "", false
				}
				continue
			}
			selected, found, err := manifest.resolveIndex(snapshot.Root, cleaned)
			if err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR", true
			}
			if found {
				descriptor = selected
				break
			}
			if !byHash && isMetadataPath(cleaned) {
				return h.forwardUpstream(w, request, cleaned), "BYPASS", true
			}
		}
	}
	if len(descriptor.CachePaths) == 0 {
		return 0, "", false
	}
	status, result := h.serveVerifiedIndex(w, request, descriptor)
	return status, result, true
}

func (h *handler) indexResponseKey(cachePath string) string {
	digest := sha256.Sum256([]byte(h.origin.String() + "\x00" + cachePath))
	return "indexes/" + hex.EncodeToString(digest[:])
}

func (d indexDescriptor) matchesResponse(cached *storeio.ResponseObject, upstream string) bool {
	return cached.Status == http.StatusOK && cached.Origin == upstream &&
		(d.Size < 0 || cached.WireSize == d.Size) &&
		(d.SHA256 == "" || cached.SHA256 == d.SHA256)
}

func (h *handler) fetchIndex(ctx context.Context, method string, descriptor indexDescriptor, header http.Header) (*http.Response, error) {
	response, err := h.fetchUpstream(ctx, method, descriptor.FetchPath, "", header, transport.AdmissionForeground)
	if err != nil || descriptor.FallbackPath == "" || response.StatusCode != http.StatusNotFound && response.StatusCode != http.StatusForbidden {
		return response, err
	}
	_ = response.Body.Close()
	return h.fetchUpstream(ctx, method, descriptor.FallbackPath, "", header, transport.AdmissionForeground)
}

func (d indexDescriptor) verifyResponse(ctx context.Context, spooler *storeio.Spooler, response *http.Response) (*storeio.SpoolResult, error) {
	defer response.Body.Close()
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return nil, errors.New("unexpected index content encoding")
	}
	expectedSize := d.Size
	if expectedSize < 0 {
		expectedSize = response.ContentLength
	}
	sha512Hash := sha512.New()
	source := io.Reader(response.Body)
	if d.SHA512 != "" {
		source = io.TeeReader(source, sha512Hash)
	}
	spool, err := spooler.SpoolWithExpectedSize(ctx, source, filerepo.DefaultMaxObject, expectedSize)
	if err != nil {
		return nil, err
	}
	if d.SHA256 != "" && spool.SHA256 != d.SHA256 ||
		d.SHA512 != "" && hex.EncodeToString(sha512Hash.Sum(nil)) != d.SHA512 {
		_ = spool.Close()
		return nil, fmt.Errorf("debian index checksum mismatch: %s", d.FetchPath)
	}
	return spool, nil
}

func (h *handler) serveVerifiedIndex(w http.ResponseWriter, request *http.Request, descriptor indexDescriptor) (int, string) {
	key := h.indexResponseKey(descriptor.CachePaths[0])
	var baseline time.Time
	waited := false
	for {
		if request.Context().Err() != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		cached, _ := storeio.OpenResponse(request.Context(), h.store, debIndexTenant, key)
		if cached != nil {
			if !waited {
				baseline = cached.ValidatedAt
			}
			if descriptor.matchesResponse(cached, h.origin.String()) {
				fresh := proxyruntime.ResponseFresh(cached.Header, cached.ValidatedAt, debArtifactFreshness) && !proxyruntime.RequestForcesRevalidation(request)
				if fresh || waited && cached.ValidatedAt.After(baseline) {
					defer cached.Reader.Close()
					return serveIndexContent(w, request, cached.Reader, cached.ResponseHeader(), "HIT"), "HIT"
				}
			}
			_ = cached.Reader.Close()
		}
		if request.Method == http.MethodHead {
			response, err := h.fetchIndex(request.Context(), http.MethodHead, descriptor, request.Header)
			if err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		flight, leader := h.flights.Begin(key)
		if !leader {
			if err := h.flights.Wait(request.Context(), flight); err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			waited = true
			continue
		}
		// Recheck after acquiring the flight: another producer may have just finished.
		if updated, err := storeio.OpenResponse(request.Context(), h.store, debIndexTenant, key); err == nil {
			_ = updated.Reader.Close()
			if updated.ValidatedAt.After(baseline) && descriptor.matchesResponse(updated, h.origin.String()) {
				h.flights.Finish(key, flight, nil)
				waited = true
				continue
			}
		}
		var fillErr error
		defer func() { h.flights.Finish(key, flight, fillErr) }()
		_, done, err := h.lifecycle.Begin()
		if err != nil {
			fillErr = err
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		defer done()
		header := request.Header.Clone()
		for _, name := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
			header.Del(name)
		}
		header.Set("Accept-Encoding", "identity")
		response, err := h.fetchIndex(h.lifecycle.Context(), http.MethodGet, descriptor, header)
		if err != nil {
			fillErr = err
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		if response.StatusCode != http.StatusOK {
			if response.StatusCode >= 200 && response.StatusCode < 400 {
				_ = response.Body.Close()
				fillErr = fmt.Errorf("unexpected index response status: %d", response.StatusCode)
				transport.WriteError(w, http.StatusBadGateway)
				return http.StatusBadGateway, "ERROR"
			}
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		spool, err := descriptor.verifyResponse(h.lifecycle.Context(), h.spooler, response)
		if err != nil {
			fillErr = err
			transport.WriteError(w, http.StatusBadGateway)
			return http.StatusBadGateway, "ERROR"
		}
		defer spool.Close()
		cacheable := transport.ResponseCacheable(response, false)
		for _, cachePath := range descriptor.CachePaths {
			cacheKey := h.indexResponseKey(cachePath)
			if !cacheable {
				_ = storeio.DeleteResponse(h.lifecycle.Context(), h.store, debIndexTenant, cacheKey)
				continue
			}
			_, _ = spool.File.Seek(0, io.SeekStart)
			if err := storeio.PutResponse(storeio.WithResponseTiming(h.lifecycle.Context(), response), h.store, debIndexTenant, cacheKey, h.origin.String(), http.StatusOK, response.Header, spool.SHA256, spool.File); err != nil {
				slog.Warn("debian index cache publication failed", "path", descriptor.FetchPath, "err", err)
			}
		}
		_, _ = spool.File.Seek(0, io.SeekStart)
		h.flights.Finish(key, flight, nil)
		return serveIndexContent(w, request, spool.File, response.Header, "MISS"), "MISS"
	}
}

func serveIndexContent(w http.ResponseWriter, request *http.Request, body io.ReadSeeker, header http.Header, result string) int {
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Del("Content-Length")
	w.Header().Set("X-Cache", result)
	modified, _ := http.ParseTime(header.Get("Last-Modified"))
	status := &indexResponseWriter{ResponseWriter: w, status: http.StatusOK}
	http.ServeContent(status, request, path.Base(request.URL.Path), modified, body)
	return status.status
}

type indexResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *indexResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
