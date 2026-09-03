package pacman

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	pacmanTenant      = "pacman"
	artifactFreshness = 24 * time.Hour
)

type handler struct {
	origin    *url.URL
	workDir   string
	store     *blobfs.Store
	client    *transport.Client
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
	metadata  *filerepo.GenerationManager
}

func newHandler(instance, stateDir string, origin *url.URL, workDir string, store *blobfs.Store, client *transport.Client, taskScheduler *scheduler.Scheduler) (*handler, error) {
	h := &handler{origin: origin, workDir: workDir, store: store, client: client, lifecycle: storeio.NewLifecycle()}
	var err error
	h.metadata, err = filerepo.New(filerepo.Config{
		Instance: instance, Mode: "pacman", Tenant: "pacman-metadata", StateDir: stateDir, WorkDir: workDir, Spooler: client.EnsureSpooler(workDir), Store: store, Scheduler: taskScheduler,
		Fetch: func(ctx context.Context, upstream, requestPath string, header http.Header) (*http.Response, error) {
			if upstream != h.origin.String() {
				return nil, errors.New("pacman metadata upstream is not configured")
			}
			return h.fetchUpstreamWithClass(ctx, http.MethodGet, requestPath, "", header, transport.AdmissionRefresh)
		},
		Build: h.buildDatabaseSnapshot,
	})
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		return
	}
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid Pacman path", http.StatusBadRequest)
		return
	}
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if isPacmanDatabasePath(cleaned) {
		if request.URL.RawQuery != "" {
			h.forwardUpstream(w, request, cleaned)
			return
		}
		anchorPath := strings.TrimSuffix(cleaned, ".sig")
		rootID := "pacman:" + h.origin.String() + ":" + anchorPath
		if handled, _, _ := h.metadata.ServeCurrentFor(w, request, cleaned, true, rootID); handled {
			return
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Range") != "" || strings.HasSuffix(cleaned, ".sig") {
			h.forwardUpstream(w, request, cleaned)
			return
		}
		if request.Method == http.MethodGet {
			h.serveDatabaseAnchor(w, request, cleaned)
			return
		}
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if !isPacmanArtifactPath(cleaned) {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	key := artifactKey(h.origin, cleaned, request)
	if object, err := storeio.OpenResponse(request.Context(), h.store, pacmanTenant, key); err == nil {
		if time.Since(object.Fetched) < artifactFreshness && !transport.RequestForcesRevalidation(request) {
			serveArtifactObject(w, request, object, "HIT")
			return
		}
		h.revalidateArtifact(w, request, cleaned, key, object)
		return
	}
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	flight, leader := h.flights.Begin(key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if object, openErr := storeio.OpenResponse(request.Context(), h.store, pacmanTenant, key); openErr == nil {
			serveArtifactObject(w, request, object, "COALESCED")
			return
		}
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if object, err := storeio.OpenResponse(request.Context(), h.store, pacmanTenant, key); err == nil {
		h.flights.Finish(key, flight, nil)
		serveArtifactObject(w, request, object, "HIT")
		return
	}
	h.fillArtifact(w, request, cleaned, flight)
}

func (h *handler) serveDatabaseAnchor(w http.ResponseWriter, request *http.Request, cleaned string) {
	rootID := "pacman:" + h.origin.String() + ":" + cleaned
	if h.metadata.ServeStagedAnchorFor(w, request, cleaned, rootID) {
		return
	}
	flightKey := "metadata\x00pacman\x00" + cleaned + "\x00identity"
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if handled, _, _ := h.metadata.ServeCurrentFor(w, request, cleaned, true, rootID); handled || h.metadata.ServeStagedAnchorFor(w, request, cleaned, rootID) {
			return
		}
		h.forwardUpstream(w, request, cleaned)
		return
	}
	finished := false
	defer func() {
		if !finished {
			h.flights.Finish(flightKey, flight, errors.New("metadata capture did not complete"))
		}
	}()
	header := request.Header.Clone()
	header.Set("Accept-Encoding", "identity")
	response, err := h.fetchUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, "", header)
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		finished = true
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "" && !strings.EqualFold(response.Header.Get("Content-Encoding"), "identity") {
		h.flights.Finish(flightKey, flight, nil)
		finished = true
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	spool, err := storeio.CaptureResponse(h.lifecycle.Context(), w, response, h.client.EnsureSpooler(h.workDir), filerepo.DefaultMaxObject, "MISS")
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			h.flights.Finish(flightKey, flight, err)
			finished = true
			root := path.Dir(cleaned)
			h.metadata.ScheduleDiscovery(h.lifecycle, rootID, root, cleaned, h.origin.String())
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		_ = response.Body.Close()
		h.flights.Finish(flightKey, flight, err)
		finished = true
		slog.Warn("pacman metadata capture failed after response started", "path", cleaned, "err", err)
		return
	}
	_ = response.Body.Close()
	defer func() { _ = spool.Close() }()
	_, _ = spool.File.Seek(0, io.SeekStart)
	root := path.Dir(cleaned)
	stageErr := h.metadata.StageAnchorID(h.lifecycle.Context(), rootID, root, cleaned, h.origin.String(), response.Header, spool.File)
	if stageErr != nil {
		slog.Warn("pacman metadata staging failed", "path", cleaned, "err", stageErr)
	}
	h.flights.Finish(flightKey, flight, stageErr)
	finished = true
}

func (h *handler) buildDatabaseSnapshot(ctx context.Context, session *filerepo.RefreshSession, anchor filerepo.Anchor) error {
	if _, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: anchor.Path + ".sig", Optional: true}); err != nil {
		return err
	}
	return nil
}

func (h *handler) fillArtifact(w http.ResponseWriter, request *http.Request, cleaned string, flight *storeio.Flight) {
	flightKey := artifactKey(h.origin, cleaned, request)
	response, err := h.fetchUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, request.Header)
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(flightKey, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: flightKey, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, pacmanTenant, flightKey, h.origin.String(), http.StatusOK, response.Header, "", body)
		}, Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
	_ = reader.Close()
}

func (h *handler) revalidateArtifact(w http.ResponseWriter, request *http.Request, cleaned, key string, object *storeio.ResponseObject) {
	defer func() { _ = object.Reader.Close() }()
	flightKey := "revalidate:" + key
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if updated, err := storeio.OpenResponse(request.Context(), h.store, pacmanTenant, key); err == nil {
			serveArtifactObject(w, request, updated, "COALESCED")
		} else {
			serveArtifactObject(w, request, object, "STALE")
		}
		return
	}
	header := http.Header{}
	header.Set("If-None-Match", object.Header.Get("ETag"))
	header.Set("If-Modified-Since", object.Header.Get("Last-Modified"))
	response, err := h.fetchUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, header)
	if err != nil || response.StatusCode >= 500 {
		if response != nil {
			_ = response.Body.Close()
		}
		h.flights.Finish(flightKey, flight, err)
		serveArtifactObject(w, request, object, "STALE")
		return
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		_ = storeio.TouchResponse(h.lifecycle.Context(), h.store, pacmanTenant, key, response.Header)
		h.flights.Finish(flightKey, flight, nil)
		serveArtifactObject(w, request, object, "REVALIDATED")
		return
	}
	_ = object.Reader.Close()
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(flightKey, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		_ = storeio.DeleteResponse(context.Background(), h.store, pacmanTenant, key)
		return
	}
	header = response.Header.Clone()
	header.Del("Content-Length")
	reader, streamErr := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, pacmanTenant, key, h.origin.String(), http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if streamErr != nil {
		h.flights.Finish(flightKey, flight, streamErr)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "REFRESH")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
	_ = reader.Close()
}

func (h *handler) fetchUpstream(ctx context.Context, method, cleaned, rawQuery string, header http.Header) (*http.Response, error) {
	return h.fetchUpstreamWithClass(ctx, method, cleaned, rawQuery, header, transport.AdmissionForeground)
}

func (h *handler) fetchUpstreamWithClass(ctx context.Context, method, cleaned, rawQuery string, header http.Header, class transport.AdmissionClass) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), rawQuery)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, err
	}
	transport.CopyReadRequestHeaders(request.Header, header)
	return h.client.DoRead(ctx, request, class)
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

func isPacmanDatabasePath(cleaned string) bool {
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		return false
	}
	base := path.Base(cleaned)
	base = strings.TrimSuffix(base, ".sig")
	for _, marker := range []string{".db", ".files"} {
		index := strings.LastIndex(base, marker)
		if index <= 0 {
			continue
		}
		suffix := base[index+len(marker):]
		for _, allowed := range []string{"", ".tar", ".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst", ".tar.lz4", ".gz", ".bz2", ".xz", ".zst", ".lz4", ".lrz", ".lzo", ".Z"} {
			if suffix == allowed {
				return true
			}
		}
	}
	return false
}

func isPacmanArtifactPath(cleaned string) bool {
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		return false
	}
	base := strings.ToLower(path.Base(cleaned))
	for _, suffix := range []string{".asc", ".sig", ".sha256", ".sha512", ".md5", ".md5sum"} {
		base = strings.TrimSuffix(base, suffix)
	}
	if strings.HasSuffix(base, ".delta") {
		return true
	}
	for _, suffix := range []string{".pkg.tar", ".pkg.tar.gz", ".pkg.tar.bz2", ".pkg.tar.xz", ".pkg.tar.zst", ".pkg.tar.lz4", ".pkg.tar.lrz", ".pkg.tar.lzo", ".pkg.tar.z"} {
		if strings.HasSuffix(base, suffix) {
			return true
		}
	}
	return false
}

func artifactKey(origin *url.URL, cleaned string, request *http.Request) string {
	identity := origin.String() + "\x00" + cleaned
	if request.URL.RawQuery != "" {
		identity += "\x00query:" + request.URL.RawQuery
	}
	digest := sha256.Sum256([]byte(identity + "\x00" + request.Header.Get("Accept-Encoding")))
	return "refs/" + hex.EncodeToString(digest[:])
}

func serveArtifactObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, path.Base(request.URL.Path), object.Fetched, object.Reader)
}
