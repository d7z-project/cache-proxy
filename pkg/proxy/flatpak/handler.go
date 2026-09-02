package flatpak

import (
	"bytes"
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

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	flatpakTenant    = "flatpak"
	mutableFreshness = time.Minute
	deltaFreshness   = 24 * time.Hour
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
		Instance: instance, Mode: "flatpak", Tenant: "flatpak-metadata", StateDir: stateDir, WorkDir: workDir, Spooler: client.EnsureSpooler(workDir), Store: store, Scheduler: sched,
		Fetch: func(ctx context.Context, _ string, requestPath string, header http.Header) (*http.Response, error) {
			return h.fetchWithClass(ctx, http.MethodGet, requestPath, header, transport.AdmissionRefresh)
		},
		Build: func(ctx context.Context, session *filerepo.RefreshSession, anchor filerepo.Anchor) error {
			switch anchor.Path {
			case "summary":
				_, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: "summary.sig", Optional: true})
				return err
			case "summary.idx":
				reader, err := anchor.Open(ctx)
				if err != nil {
					return err
				}
				index, parseErr := readSummaryIndex(reader, anchor.Size())
				closeErr := reader.Close()
				if err := errors.Join(parseErr, closeErr); err != nil {
					return err
				}
				for _, digest := range index.subsummaryDigests {
					blob, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: "summaries/" + digest + ".gz", MaxBytes: indexedSummaryMaxBytes})
					if err != nil {
						return err
					}
					if err := verifyIndexedSummary(ctx, blob, digest); err != nil {
						return err
					}
				}
				if _, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: "summaries/" + index.digest + ".idx.sig", MaxBytes: summaryIndexMaxBytes, Optional: true}); err != nil {
					return err
				}
				_, err = session.Fetch(ctx, filerepo.ObjectSpec{Path: "summary.idx.sig", MaxBytes: summaryIndexMaxBytes, Optional: true})
				return err
			}
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
	cleaned := "summary"
	if request.URL.Path != "" && request.URL.Path != "/" {
		var err error
		cleaned, err = storeio.CleanURLPath(request.URL)
		if err != nil {
			http.Error(w, "invalid Flatpak path", http.StatusBadRequest)
			return
		}
	}
	if anchorPath, metadataPath := metadataAnchorPath(cleaned); metadataPath {
		rootID := "flatpak:" + anchorPath
		if handled, _, _ := h.metadata.ServeCurrentFor(w, request, cleaned, true, rootID); handled {
			return
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Range") != "" || cleaned != anchorPath || request.Method == http.MethodHead {
			h.forward(w, request, cleaned)
			return
		}
		h.serveMetadataAnchor(w, request, cleaned, rootID)
		return
	}
	if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
		h.forward(w, request, cleaned)
		return
	}
	if digest, extension, ok := objectDigestFromPath(cleaned); ok {
		h.serveDigestObject(w, request, cleaned, digest, extension)
		return
	}
	if isDescriptorPath(cleaned) {
		h.serveDescriptor(w, request, cleaned)
		return
	}
	objectPath := isObjectPath(cleaned)
	deltaPath := isDeltaPath(cleaned)
	if strings.HasPrefix(cleaned, "refs/") || deltaPath || objectPath {
		freshness := mutableFreshness
		if deltaPath || objectPath {
			freshness = deltaFreshness
		}
		cacheNotFound := isDeltaIndexPath(cleaned) || objectPath && strings.HasSuffix(cleaned, ".commitmeta")
		h.serveMutable(w, request, cleaned, freshness, cacheNotFound)
		return
	}
	h.forward(w, request, cleaned)
}

func (h *handler) serveMetadataAnchor(w http.ResponseWriter, request *http.Request, cleaned, rootID string) {
	if h.metadata.ServeStagedAnchorFor(w, request, cleaned, rootID) {
		return
	}
	flightKey := "metadata\x00flatpak\x00" + h.origin.String() + "\x00" + cleaned + "\x00identity"
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
	defer response.Body.Close()
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
			h.metadata.ScheduleDiscovery(h.lifecycle, rootID, ".", cleaned, h.origin.String())
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		h.flights.Finish(flightKey, flight, err)
		finished = true
		slog.Warn("flatpak metadata capture failed after response started", "path", cleaned, "err", err)
		return
	}
	defer spool.Close()
	_, _ = spool.File.Seek(0, io.SeekStart)
	stageErr := h.metadata.StageAnchorID(h.lifecycle.Context(), rootID, ".", cleaned, h.origin.String(), response.Header, spool.File)
	if stageErr != nil {
		slog.Warn("flatpak metadata staging failed", "path", cleaned, "err", stageErr)
	}
	h.flights.Finish(flightKey, flight, stageErr)
	finished = true
}

func (h *handler) serveDigestObject(w http.ResponseWriter, request *http.Request, cleaned, digest, extension string) {
	key := "objects/sha256/" + digest
	if object, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key); err == nil {
		serveFlatpakObject(w, request, object, "HIT")
		return
	}
	if request.Method == http.MethodHead {
		h.forward(w, request, cleaned)
		return
	}
	rangeRequest := request.Header.Get("Range") != ""
	flight, leader := h.flights.Begin(key)
	if leader {
		if object, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key); err == nil {
			h.flights.Finish(key, flight, nil)
			serveFlatpakObject(w, request, object, "HIT")
			return
		}
		upstreamHeader := request.Header.Clone()
		if rangeRequest {
			upstreamHeader.Del("Range")
			upstreamHeader.Del("If-Range")
		}
		response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, upstreamHeader)
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
		if !transport.ResponseCacheable(response, false) {
			h.flights.Finish(key, flight, nil)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		header := response.Header.Clone()
		header.Del("Content-Length")
		reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
			Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
			VerifyFn: func(reader io.ReadSeeker) error {
				if extension == ".filez" {
					return verifyOSTreeFileObject(reader, digest)
				}
				hash := sha256.New()
				if _, err := io.Copy(hash, reader); err != nil {
					return err
				}
				if !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), digest) {
					return errors.New("ostree object digest mismatch")
				}
				return nil
			},
			StoreFn: func(ctx context.Context, body io.Reader) error {
				return storeio.PutResponse(ctx, h.store, flatpakTenant, key, h.origin.String(), http.StatusOK, response.Header, digest, body)
			}, Done: func(err error) { h.flights.Finish(key, flight, err) },
		})
		if err != nil {
			h.flights.Finish(key, flight, err)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		if rangeRequest {
			_, copyErr := io.Copy(io.Discard, reader)
			closeErr := reader.Close()
			if err := errors.Join(copyErr, closeErr); err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return
			}
			if err := h.flights.Wait(request.Context(), flight); err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return
			}
			object, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key)
			if err != nil {
				transport.WriteError(w, http.StatusBadGateway)
				return
			}
			serveFlatpakObject(w, request, object, "MISS")
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
	if object, openErr := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key); openErr == nil {
		serveFlatpakObject(w, request, object, "COALESCED")
		return
	}
	transport.WriteError(w, http.StatusBadGateway)
}

func (h *handler) serveMutable(w http.ResponseWriter, request *http.Request, cleaned string, freshness time.Duration, cacheNotFound bool) {
	key := flatpakRefKey(h.origin.String(), cleaned, request.Header.Get("Accept-Encoding"))
	object, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key)
	if err == nil && time.Since(object.Fetched) < freshness && !transport.RequestForcesRevalidation(request) {
		serveFlatpakObject(w, request, object, "HIT")
		return
	}
	if request.Header.Get("Range") != "" {
		if object != nil {
			serveFlatpakObject(w, request, object, "STALE")
		} else {
			h.forward(w, request, cleaned)
		}
		return
	}
	if request.Method == http.MethodHead {
		if object != nil {
			_ = object.Reader.Close()
		}
		h.forward(w, request, cleaned)
		return
	}
	flightKey := "revalidate:" + key
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if object != nil {
			_ = object.Reader.Close()
		}
		if updated, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key); err == nil {
			serveFlatpakObject(w, request, updated, "COALESCED")
			return
		}
		h.forward(w, request, cleaned)
		return
	}
	if current, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key); err == nil {
		if object == nil || current.Fetched.After(object.Fetched) {
			if object != nil {
				_ = object.Reader.Close()
			}
			h.flights.Finish(flightKey, flight, nil)
			serveFlatpakObject(w, request, current, "HIT")
			return
		}
		_ = current.Reader.Close()
	}
	conditional := request.Header.Clone()
	if object != nil {
		conditional.Set("If-None-Match", object.Header.Get("ETag"))
		conditional.Set("If-Modified-Since", object.Header.Get("Last-Modified"))
	}
	response, fetchErr := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, conditional)
	if fetchErr != nil || response.StatusCode >= 500 {
		if response != nil {
			_ = response.Body.Close()
		}
		if object != nil {
			h.flights.Finish(flightKey, flight, fetchErr)
			serveFlatpakObject(w, request, object, "STALE")
			return
		}
		transport.WriteError(w, http.StatusBadGateway)
		h.flights.Finish(flightKey, flight, fetchErr)
		return
	}
	if response.StatusCode == http.StatusNotModified && object != nil {
		_ = response.Body.Close()
		_ = storeio.TouchResponse(h.lifecycle.Context(), h.store, flatpakTenant, key, response.Header)
		h.flights.Finish(flightKey, flight, nil)
		serveFlatpakObject(w, request, object, "REVALIDATED")
		return
	}
	if object != nil {
		_ = object.Reader.Close()
	}
	cacheableStatus := response.StatusCode == http.StatusOK || cacheNotFound && response.StatusCode == http.StatusNotFound
	if !cacheableStatus || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(flightKey, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, streamErr := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, flatpakTenant, key, h.origin.String(), response.StatusCode, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if streamErr != nil {
		h.flights.Finish(flightKey, flight, streamErr)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, reader)
	_ = reader.Close()
}

func (h *handler) serveDescriptor(w http.ResponseWriter, request *http.Request, cleaned string) {
	externalBase := strings.TrimRight(proxyruntime.ExternalBaseURL(request), "/")
	key := flatpakRefKey(h.origin.String(), cleaned, externalBase)
	if object, err := storeio.OpenResponse(request.Context(), h.store, flatpakTenant, key); err == nil && time.Since(object.Fetched) < mutableFreshness {
		serveFlatpakObject(w, request, object, "HIT")
		return
	}
	if request.Method == http.MethodHead {
		h.forward(w, request, cleaned)
		return
	}
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, request.Header)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	if !transport.ResponseCacheable(response, false) {
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDescriptorSize+1))
	if err != nil || len(body) > maxDescriptorSize {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	body = rewriteDescriptor(request, body)
	digest := sha256.Sum256(body)
	header := response.Header.Clone()
	header.Del("Content-Encoding")
	header.Del("Content-Length")
	header.Del("Content-MD5")
	header.Del("Digest")
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	header.Set("ETag", `"sha256-`+hex.EncodeToString(digest[:])+`"`)
	result := "MISS"
	if err := storeio.PutResponse(h.lifecycle.Context(), h.store, flatpakTenant, key, h.origin.String(), http.StatusOK, header, hex.EncodeToString(digest[:]), bytes.NewReader(body)); err != nil {
		result = "BYPASS"
	}
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", result)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (h *handler) fetch(ctx context.Context, method, cleaned string, header http.Header) (*http.Response, error) {
	return h.fetchWithClass(ctx, method, cleaned, header, transport.AdmissionForeground)
}

func (h *handler) fetchWithClass(ctx context.Context, method, cleaned string, header http.Header, class transport.AdmissionClass) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), "")
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

func flatpakRefKey(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return "refs/" + hex.EncodeToString(hash.Sum(nil))
}

func serveFlatpakObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer func() { _ = object.Reader.Close() }()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", result)
	if object.Status != http.StatusOK {
		w.WriteHeader(object.Status)
		if request.Method != http.MethodHead {
			_, _ = io.Copy(w, object.Reader)
		}
		return
	}
	http.ServeContent(w, request, path.Base(request.URL.Path), object.Fetched, object.Reader)
}
