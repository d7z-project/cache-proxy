package apk

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
	apkTenant         = "apk"
	indexFreshness    = time.Minute
	artifactFreshness = 24 * time.Hour
	directIndexMarker = "@direct-index"
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

func newHandler(instance, stateDir string, origin *url.URL, workDir string, store *blobfs.Store, client *transport.Client, sched *scheduler.Scheduler) (*handler, error) {
	h := &handler{origin: origin, workDir: workDir, store: store, client: client, lifecycle: storeio.NewLifecycle()}
	var err error
	h.metadata, err = filerepo.New(filerepo.Config{
		Instance: instance, Mode: "apk", Tenant: "apk-metadata", StateDir: stateDir, WorkDir: workDir, Spooler: client.EnsureSpooler(workDir), Store: store, Scheduler: sched,
		Fetch: func(ctx context.Context, upstream, requestPath string, header http.Header) (*http.Response, error) {
			if upstream != h.origin.String() {
				return nil, errors.New("apk metadata upstream is not configured")
			}
			return h.fetchWithClass(ctx, http.MethodGet, requestPath, header, transport.AdmissionRefresh)
		},
		Build: func(context.Context, *filerepo.RefreshSession, filerepo.Anchor) error {
			return nil
		},
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
	cleaned := ""
	if request.URL.Path == "" || request.URL.Path == "/" {
		cleaned = directIndexMarker
	} else {
		var err error
		cleaned, err = storeio.CleanURLPath(request.URL)
		if err != nil {
			http.Error(w, "invalid APK path", http.StatusBadRequest)
			return
		}
	}
	if isIndexRequest(cleaned) {
		rootID := "apk:" + h.origin.String() + ":" + cleaned
		if handled, _, _ := h.metadata.ServeCurrentFor(w, request, cleaned, true, rootID); handled {
			return
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Range") != "" {
			h.forward(w, request, cleaned)
			return
		}
		if request.Method == http.MethodGet {
			h.serveIndex(w, request, cleaned)
			return
		}
	}
	if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
		h.forward(w, request, cleaned)
		return
	}
	index := isIndexRequest(cleaned)
	freshness := artifactFreshness
	if index {
		freshness = indexFreshness
	}
	key := apkKey(h.origin, cleaned, request)
	if object, err := storeio.OpenResponse(request.Context(), h.store, apkTenant, key); err == nil {
		if time.Since(object.Fetched) < freshness && !transport.RequestForcesRevalidation(request) {
			serveAPKObject(w, request, object, "HIT")
			return
		}
		h.revalidate(w, request, cleaned, key, object)
		return
	}
	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		h.forward(w, request, cleaned)
		return
	}
	flight, leader := h.flights.Begin(key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if object, openErr := storeio.OpenResponse(request.Context(), h.store, apkTenant, key); openErr == nil {
			serveAPKObject(w, request, object, "COALESCED")
			return
		}
		h.forward(w, request, cleaned)
		return
	}
	if object, err := storeio.OpenResponse(request.Context(), h.store, apkTenant, key); err == nil {
		h.flights.Finish(key, flight, nil)
		serveAPKObject(w, request, object, "HIT")
		return
	}
	h.fill(w, request, cleaned, flight)
}

func (h *handler) serveIndex(w http.ResponseWriter, request *http.Request, cleaned string) {
	rootID := "apk:" + h.origin.String() + ":" + cleaned
	if h.metadata.ServeStagedAnchorFor(w, request, cleaned, rootID) {
		return
	}
	flightKey := "metadata\x00apk\x00" + cleaned + "\x00identity"
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if handled, _, _ := h.metadata.ServeCurrentFor(w, request, cleaned, true, rootID); handled || h.metadata.ServeStagedAnchorFor(w, request, cleaned, rootID) {
			return
		}
		h.forward(w, request, cleaned)
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
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, header)
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
			if cleaned == directIndexMarker {
				root = "."
			}
			h.metadata.ScheduleDiscovery(h.lifecycle, rootID, root, cleaned, h.origin.String())
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		_ = response.Body.Close()
		h.flights.Finish(flightKey, flight, err)
		finished = true
		slog.Warn("apk metadata capture failed after response started", "path", cleaned, "err", err)
		return
	}
	_ = response.Body.Close()
	defer spool.Close()
	_, _ = spool.File.Seek(0, io.SeekStart)
	root := path.Dir(cleaned)
	if cleaned == directIndexMarker {
		root = "."
	}
	stageErr := h.metadata.StageAnchorID(h.lifecycle.Context(), rootID, root, cleaned, h.origin.String(), response.Header, spool.File)
	if stageErr != nil {
		slog.Warn("apk metadata staging failed", "path", cleaned, "err", stageErr)
	}
	h.flights.Finish(flightKey, flight, stageErr)
	finished = true
}

func (h *handler) fill(w http.ResponseWriter, request *http.Request, cleaned string, flight *storeio.Flight) {
	flightKey := apkKey(h.origin, cleaned, request)
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, request.Header)
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
			return storeio.PutResponse(ctx, h.store, apkTenant, flightKey, h.origin.String(), http.StatusOK, response.Header, "", body)
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

func (h *handler) revalidate(w http.ResponseWriter, request *http.Request, cleaned, key string, object *storeio.ResponseObject) {
	defer func() { _ = object.Reader.Close() }()
	flightKey := "revalidate:" + key
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if updated, err := storeio.OpenResponse(request.Context(), h.store, apkTenant, key); err == nil {
			serveAPKObject(w, request, updated, "COALESCED")
		} else {
			serveAPKObject(w, request, object, "STALE")
		}
		return
	}
	header := http.Header{}
	header.Set("If-None-Match", object.Header.Get("ETag"))
	header.Set("If-Modified-Since", object.Header.Get("Last-Modified"))
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, header)
	if err != nil || response.StatusCode >= 500 {
		if response != nil {
			_ = response.Body.Close()
		}
		h.flights.Finish(flightKey, flight, err)
		serveAPKObject(w, request, object, "STALE")
		return
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		_ = storeio.TouchResponse(h.lifecycle.Context(), h.store, apkTenant, key, response.Header)
		h.flights.Finish(flightKey, flight, nil)
		serveAPKObject(w, request, object, "REVALIDATED")
		return
	}
	_ = object.Reader.Close()
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(flightKey, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		_ = storeio.DeleteResponse(context.Background(), h.store, apkTenant, key)
		return
	}
	header = response.Header.Clone()
	header.Del("Content-Length")
	reader, streamErr := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, apkTenant, key, h.origin.String(), http.StatusOK, response.Header, "", body)
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

func (h *handler) fetch(ctx context.Context, method, cleaned string, header http.Header) (*http.Response, error) {
	return h.fetchWithClass(ctx, method, cleaned, header, transport.AdmissionForeground)
}

func (h *handler) fetchWithClass(ctx context.Context, method, cleaned string, header http.Header, class transport.AdmissionClass) (*http.Response, error) {
	var target *url.URL
	var err error
	if cleaned == directIndexMarker {
		originCopy := *h.origin
		target = &originCopy
	} else {
		target, err = transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), "")
	}
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

func (h *handler) forward(w http.ResponseWriter, request *http.Request, cleaned string) int {
	response, err := h.fetch(request.Context(), request.Method, cleaned, request.Header)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return http.StatusBadGateway
	}
	return transport.WriteResponse(w, request, response, "BYPASS")
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func isIndexRequest(cleaned string) bool {
	base := path.Base(cleaned)
	return cleaned == directIndexMarker || base == "APKINDEX.tar.gz" || base == "Packages.adb"
}

func apkKey(origin *url.URL, cleaned string, request *http.Request) string {
	digest := sha256.Sum256([]byte(origin.String() + "\x00" + cleaned + "\x00" + request.Header.Get("Accept-Encoding")))
	return "refs/" + hex.EncodeToString(digest[:])
}

func serveAPKObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, path.Base(request.URL.Path), object.Fetched, object.Reader)
}
