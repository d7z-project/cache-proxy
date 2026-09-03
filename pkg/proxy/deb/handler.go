package deb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const maxReleaseSize = 16 << 20

const (
	debArtifactTenant    = "deb-artifacts"
	debArtifactFreshness = 24 * time.Hour
)

type handler struct {
	name      string
	origin    *url.URL
	workDir   string
	store     *blobfs.Store
	client    *transport.Client
	stats     *metrics.Stats
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
	metadata  *filerepo.GenerationManager
}

func newHandler(name, upstream, stateDir, workDir string, blobs *blobfs.Store, client *transport.Client, stats *metrics.Stats, taskScheduler *scheduler.Scheduler) (*handler, error) {
	origin, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse debian upstream: %w", err)
	}
	h := &handler{name: name, origin: origin, workDir: workDir, store: blobs, client: client, stats: stats, lifecycle: storeio.NewLifecycle()}
	h.metadata, err = filerepo.New(filerepo.Config{
		Instance: name, Mode: "deb", Tenant: "deb-metadata", StateDir: stateDir, WorkDir: workDir, Spooler: client.EnsureSpooler(workDir), AnchorMaxBytes: maxReleaseSize, Store: blobs, Scheduler: taskScheduler,
		KeepPrevious: 2,
		Fetch: func(ctx context.Context, _ string, requestPath string, header http.Header) (*http.Response, error) {
			return h.openUpstream(ctx, http.MethodGet, requestPath, "", header, transport.AdmissionRefresh)
		},
		Build: h.buildSnapshot,
	})
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		h.stats.RecordRequest(h.name, "deb", request.Method, "REJECTED", http.StatusMethodNotAllowed, 0)
		return
	}
	cleaned := ""
	var err error
	if request.URL.Path != "" && request.URL.Path != "/" {
		decoded, decodeErr := storeio.DecodeURLPath(request.URL)
		err = decodeErr
		if err == nil {
			for _, segment := range strings.Split(decoded, "/") {
				if segment == ".." {
					err = errors.New("parent path segment is not allowed")
					break
				}
			}
		}
		if err == nil {
			trailingSlash := strings.HasSuffix(decoded, "/")
			cleaned, err = storeio.CleanRelative(strings.TrimPrefix(path.Clean(decoded), "/"))
			if err == nil && trailingSlash {
				cleaned += "/"
			}
		}
	}
	if err != nil {
		http.Error(w, "invalid Debian path", http.StatusBadRequest)
		h.stats.RecordRequest(h.name, "deb", request.Method, "ERROR", http.StatusBadRequest, 0)
		return
	}
	status, result := h.serve(w, request, cleaned)
	h.stats.RecordRequest(h.name, "deb", request.Method, result, status, 0)
}

func (h *handler) serve(w http.ResponseWriter, request *http.Request, cleaned string) (int, string) {
	if handled, status, result := h.metadata.ServeCurrent(w, request, cleaned, isMetadataPath(cleaned)); handled {
		return status, result
	}
	if isArtifactPath(cleaned) && request.Header.Get("Authorization") == "" && request.Header.Get("Cookie") == "" {
		return h.serveArtifact(w, request, cleaned)
	}
	if request.Method != http.MethodGet || !isAnchorPath(cleaned) || request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Range") != "" {
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}
	root := path.Dir(cleaned)
	if h.metadata.ServeStagedAnchorFor(w, request, cleaned, root) {
		return http.StatusOK, "COALESCED"
	}
	flightKey := "metadata\x00" + h.origin.String() + "\x00" + cleaned + "\x00identity"
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if handled, status, result := h.metadata.ServeCurrentFor(w, request, cleaned, true, root); handled {
			return status, result
		}
		if h.metadata.ServeStagedAnchorFor(w, request, cleaned, root) {
			return http.StatusOK, "COALESCED"
		}
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}
	finished := false
	defer func() {
		if !finished {
			h.flights.Finish(flightKey, flight, errors.New("metadata capture did not complete"))
		}
	}()
	header := request.Header.Clone()
	header.Set("Accept-Encoding", "identity")
	response, err := h.openUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, header, transport.AdmissionForeground)
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		finished = true
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "" && !strings.EqualFold(response.Header.Get("Content-Encoding"), "identity") {
		h.flights.Finish(flightKey, flight, nil)
		finished = true
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	spool, err := storeio.CaptureResponse(h.lifecycle.Context(), w, response, h.client.EnsureSpooler(h.workDir), maxReleaseSize, "MISS")
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			h.flights.Finish(flightKey, flight, err)
			finished = true
			h.metadata.ScheduleDiscovery(h.lifecycle, root, root, cleaned, h.origin.String())
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		h.flights.Finish(flightKey, flight, err)
		finished = true
		slog.Warn("debian metadata capture failed after response started", "path", cleaned, "err", err)
		return http.StatusOK, "BYPASS"
	}
	defer func() { _ = spool.Close() }()
	stageErr := error(nil)
	if _, err := parseReleaseManifest(h.lifecycle.Context(), spool.File); err == nil {
		_, _ = spool.File.Seek(0, io.SeekStart)
		stageErr = h.metadata.StageAnchor(h.lifecycle.Context(), root, cleaned, h.origin.String(), response.Header, spool.File)
		if stageErr != nil {
			slog.Warn("debian metadata staging failed", "path", cleaned, "err", stageErr)
		}
	}
	h.flights.Finish(flightKey, flight, stageErr)
	finished = true
	return http.StatusOK, "MISS"
}

func (h *handler) buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, anchor filerepo.Anchor) error {
	reader, err := anchor.Open(ctx)
	if err != nil {
		return err
	}
	manifest, err := parseReleaseManifest(ctx, io.LimitReader(reader, maxReleaseSize+1))
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("parse Debian Release: %w", err)
	}
	if err := reader.Close(); err != nil {
		return err
	}
	alternateName := "Release"
	if path.Base(anchor.Path) == "Release" {
		alternateName = "InRelease"
	}
	alternatePath := joinRoot(anchor.Root, alternateName)
	alternate, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: alternatePath, MaxBytes: maxReleaseSize, Optional: true})
	if err != nil {
		return err
	}
	hasRelease := path.Base(anchor.Path) == "Release"
	if alternate != nil {
		alternateReader, openErr := alternate.Open(ctx)
		if openErr != nil {
			return openErr
		}
		alternateManifest, parseErr := parseReleaseManifest(ctx, io.LimitReader(alternateReader, maxReleaseSize+1))
		closeErr := alternateReader.Close()
		if parseErr != nil {
			return filerepo.Retryable(fmt.Errorf("parse alternate Debian anchor: %w", parseErr))
		}
		if closeErr != nil {
			return closeErr
		}
		if !releaseManifestsEqual(manifest, alternateManifest) {
			return filerepo.Retryable(errors.New("debian InRelease and Release metadata differ"))
		}
		hasRelease = hasRelease || alternateName == "Release"
	}
	for _, entry := range manifest.Entries {
		if entry.SHA256 == "" && entry.SHA512 == "" {
			return fmt.Errorf("release entry %s has no strong digest", entry.Path)
		}
		canonical := joinRoot(anchor.Root, entry.Path)
		fetchPath := canonical
		if manifest.AcquireByHash {
			if entry.SHA256 != "" {
				fetchPath = releaseByHashPath(canonical, "SHA256", entry.SHA256)
			} else {
				fetchPath = releaseByHashPath(canonical, "SHA512", entry.SHA512)
			}
		}
		expectedSize := entry.Size
		checksums := make([]filerepo.Checksum, 0, 2)
		if entry.SHA256 != "" {
			checksums = append(checksums, filerepo.Checksum{Algorithm: "sha256", Digest: entry.SHA256})
		}
		if entry.SHA512 != "" {
			checksums = append(checksums, filerepo.Checksum{Algorithm: "sha512", Digest: entry.SHA512})
		}
		blob, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: canonical, FetchPath: fetchPath, ExpectedSize: &expectedSize, Checksums: checksums})
		if err != nil {
			return err
		}
		if entry.SHA256 != "" {
			if err := session.Alias(releaseByHashPath(canonical, "SHA256", entry.SHA256), blob); err != nil {
				return err
			}
		}
		if entry.SHA512 != "" {
			if err := session.Alias(releaseByHashPath(canonical, "SHA512", entry.SHA512), blob); err != nil {
				return err
			}
		}
	}
	companion := joinRoot(anchor.Root, "Release.gpg")
	if hasRelease && companion != anchor.Path {
		if _, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: companion, Optional: true}); err != nil {
			return err
		}
	}
	return nil
}

func (h *handler) serveArtifact(w http.ResponseWriter, request *http.Request, cleaned string) (int, string) {
	key := h.artifactKey(cleaned, request)
	object, _ := storeio.OpenResponse(request.Context(), h.store, debArtifactTenant, key)
	if object != nil {
		fresh := time.Since(object.Fetched) < debArtifactFreshness && !transport.RequestForcesRevalidation(request)
		if fresh {
			return serveArtifactObject(w, request, object, "HIT"), "HIT"
		}
		if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
			_ = object.Reader.Close()
			return h.forwardUpstream(w, request, cleaned), "BYPASS"
		}

		flightKey := "revalidate:" + key
		flight, leader := h.flights.Begin(flightKey)
		if !leader {
			_ = h.flights.Wait(request.Context(), flight)
			if updated, err := storeio.OpenResponse(request.Context(), h.store, debArtifactTenant, key); err == nil {
				_ = object.Reader.Close()
				return serveArtifactObject(w, request, updated, "COALESCED"), "COALESCED"
			}
			return serveArtifactObject(w, request, object, "STALE"), "STALE"
		}

		header := http.Header{}
		header.Set("If-None-Match", object.Header.Get("ETag"))
		header.Set("If-Modified-Since", object.Header.Get("Last-Modified"))
		response, err := h.openUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, header, transport.AdmissionForeground)
		if err != nil || response.StatusCode >= http.StatusInternalServerError {
			if response != nil {
				_ = response.Body.Close()
			}
			h.flights.Finish(flightKey, flight, err)
			return serveArtifactObject(w, request, object, "STALE"), "STALE"
		}
		if response.StatusCode == http.StatusNotModified {
			_ = response.Body.Close()
			err = storeio.TouchResponse(h.lifecycle.Context(), h.store, debArtifactTenant, key, response.Header)
			h.flights.Finish(flightKey, flight, err)
			result := "REVALIDATED"
			if err != nil {
				result = "STALE"
			}
			return serveArtifactObject(w, request, object, result), result
		}
		_ = object.Reader.Close()
		if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
			h.flights.Finish(flightKey, flight, nil)
			_ = storeio.DeleteResponse(context.Background(), h.store, debArtifactTenant, key)
			return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
		}
		return h.streamArtifact(w, request, response, key, flightKey, flight)
	}

	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}
	flight, leader := h.flights.Begin(key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if cached, err := storeio.OpenResponse(request.Context(), h.store, debArtifactTenant, key); err == nil {
			return serveArtifactObject(w, request, cached, "COALESCED"), "COALESCED"
		}
		return h.forwardUpstream(w, request, cleaned), "BYPASS"
	}
	if cached, err := storeio.OpenResponse(request.Context(), h.store, debArtifactTenant, key); err == nil {
		h.flights.Finish(key, flight, nil)
		return serveArtifactObject(w, request, cached, "HIT"), "HIT"
	}
	response, err := h.openUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, request.Header, transport.AdmissionForeground)
	if err != nil {
		h.flights.Finish(key, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway, "ERROR"
	}
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(key, flight, nil)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	return h.streamArtifact(w, request, response, key, key, flight)
}

func (h *handler) streamArtifact(w http.ResponseWriter, request *http.Request, response *http.Response, key, flightKey string, flight *storeio.Flight) (int, string) {
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, debArtifactTenant, key, h.origin.String(), http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		return transport.WriteResponse(w, request, response, "BYPASS"), "BYPASS"
	}
	defer func() { _ = reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
	return http.StatusOK, "MISS"
}

func (h *handler) artifactKey(cleaned string, request *http.Request) string {
	digest := sha256.Sum256([]byte(h.origin.String() + "\x00" + cleaned + "\x00" + request.URL.RawQuery + "\x00" + request.Header.Get("Accept-Encoding")))
	return "refs/" + hex.EncodeToString(digest[:])
}

func serveArtifactObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) int {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, path.Base(request.URL.Path), object.Fetched, object.Reader)
	return http.StatusOK
}

func isArtifactPath(cleaned string) bool {
	if strings.HasSuffix(cleaned, "/") {
		return false
	}
	name := strings.ToLower(path.Base(cleaned))
	for _, sidecar := range []string{".asc", ".sig", ".sha256", ".sha512", ".md5", ".md5sum"} {
		name = strings.TrimSuffix(name, sidecar)
	}
	for _, suffix := range []string{".deb", ".udeb", ".ddeb", ".dsc", ".changes", ".buildinfo", ".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst", ".diff.gz"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func (h *handler) forwardUpstream(w http.ResponseWriter, request *http.Request, cleaned string) int {
	response, err := h.openUpstream(request.Context(), request.Method, cleaned, request.URL.RawQuery, request.Header, transport.AdmissionForeground)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	return transport.WriteResponse(w, request, response, "BYPASS")
}

func (h *handler) openUpstream(ctx context.Context, method, requestPath, rawQuery string, headers http.Header, class transport.AdmissionClass) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(requestPath), "")
	if err != nil {
		return nil, err
	}
	target.RawQuery = rawQuery
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"Accept", "Accept-Encoding", "Authorization", "Range", "If-None-Match", "If-Modified-Since", "User-Agent"} {
		for _, value := range headers.Values(name) {
			request.Header.Add(name, value)
		}
	}
	return h.client.DoRead(ctx, request, class)
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func isAnchorPath(cleaned string) bool {
	if strings.HasSuffix(cleaned, "/") {
		return false
	}
	name := path.Base(cleaned)
	return name == "InRelease" || name == "Release"
}

func isMetadataPath(cleaned string) bool {
	if strings.HasSuffix(cleaned, "/") {
		return false
	}
	name := path.Base(cleaned)
	if isAnchorPath(cleaned) || strings.Contains(cleaned, "/by-hash/SHA256/") || strings.Contains(cleaned, "/by-hash/SHA512/") {
		return true
	}
	for _, prefix := range []string{"Packages", "Sources", "Contents", "Components", "Translation", "Release.gpg"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func joinRoot(root, name string) string {
	if root == "." || root == "" {
		return strings.TrimPrefix(name, "/")
	}
	return strings.TrimSuffix(root, "/") + "/" + strings.TrimPrefix(name, "/")
}
