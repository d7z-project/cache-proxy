package rpm

import (
	"compress/bzip2"
	"compress/gzip"
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

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	rpmArtifactTenant    = "rpm-artifacts"
	rpmArtifactFreshness = 24 * time.Hour
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

func newHandler(instance string, origin *url.URL, stateDir, workDir string, blobs *blobfs.Store, client *transport.Client, sched *scheduler.Scheduler) (*handler, error) {
	h := &handler{origin: origin, workDir: workDir, store: blobs, client: client, lifecycle: storeio.NewLifecycle()}
	var err error
	h.metadata, err = filerepo.New(filerepo.Config{
		Instance: instance, Mode: "rpm", Tenant: "rpm-metadata", StateDir: stateDir, WorkDir: workDir, Spooler: client.EnsureSpooler(workDir), AnchorMaxBytes: maxRepomdSize, Store: blobs, Scheduler: sched,
		Fetch: func(ctx context.Context, _ string, requestPath string, header http.Header) (*http.Response, error) {
			return h.fetch(ctx, http.MethodGet, requestPath, "", header, transport.AdmissionRefresh)
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
		return
	}
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid RPM path", http.StatusBadRequest)
		return
	}
	if handled, _, _ := h.metadata.ServeCurrent(w, request, cleaned, isRPMMetadataPath(cleaned)); handled {
		return
	}
	if isRPMArtifactPath(cleaned) && request.Header.Get("Authorization") == "" && request.Header.Get("Cookie") == "" {
		h.serveArtifact(w, request, cleaned)
		return
	}
	if request.Method != http.MethodGet || !isRepomdPath(cleaned) || request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Range") != "" {
		h.forward(w, request, cleaned)
		return
	}
	root := path.Dir(path.Dir(cleaned))
	if h.metadata.ServeStagedAnchorFor(w, request, cleaned, root) {
		return
	}
	flightKey := "metadata\x00" + h.origin.String() + "\x00" + cleaned + "\x00identity"
	flight, leader := h.flights.Begin(flightKey)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if handled, _, _ := h.metadata.ServeCurrentFor(w, request, cleaned, true, root); handled {
			return
		}
		if h.metadata.ServeStagedAnchorFor(w, request, cleaned, root) {
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
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, header, transport.AdmissionForeground)
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
	spool, err := storeio.CaptureResponse(h.lifecycle.Context(), w, response, h.client.EnsureSpooler(h.workDir), maxRepomdSize, "MISS")
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			h.flights.Finish(flightKey, flight, err)
			finished = true
			h.metadata.ScheduleDiscovery(h.lifecycle, root, root, cleaned, h.origin.String())
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		h.flights.Finish(flightKey, flight, err)
		finished = true
		slog.Warn("rpm metadata capture failed after response started", "path", cleaned, "err", err)
		return
	}
	defer spool.Close()
	_, parseErr := parseRepomdReader(h.lifecycle.Context(), spool.File)
	stageErr := parseErr
	if parseErr == nil {
		_, _ = spool.File.Seek(0, io.SeekStart)
		stageErr = h.metadata.StageAnchor(h.lifecycle.Context(), root, cleaned, h.origin.String(), response.Header, spool.File)
		if stageErr != nil {
			slog.Warn("rpm metadata staging failed", "path", cleaned, "err", stageErr)
		}
	}
	h.flights.Finish(flightKey, flight, stageErr)
	finished = true
}

func (h *handler) buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, anchor filerepo.Anchor) error {
	reader, err := anchor.Open(ctx)
	if err != nil {
		return err
	}
	items, err := parseRepomdReader(ctx, reader)
	reader.Close()
	if err != nil {
		return err
	}
	for _, item := range items {
		location, err := storeio.CleanRelative(item.Location)
		if err != nil || location != item.Location || item.Checksum == "" {
			return fmt.Errorf("invalid repomd location %q", item.Location)
		}
		var expectedSize *int64
		if item.Size >= 0 {
			size := item.Size
			expectedSize = &size
		}
		blob, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: joinRoot(anchor.Root, location), ExpectedSize: expectedSize, Checksums: []filerepo.Checksum{{Algorithm: item.SumType, Digest: item.Checksum}}})
		if err != nil {
			return err
		}
		if item.OpenChecksum != "" || item.OpenSize >= 0 {
			if err := inspectOpenMetadata(ctx, blob, item); err != nil {
				return err
			}
		}
	}
	for _, suffix := range []string{".asc", ".sig"} {
		if _, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: anchor.Path + suffix, Optional: true}); err != nil {
			return err
		}
	}
	return nil
}

func inspectOpenMetadata(ctx context.Context, blob *filerepo.Blob, item repomdItem) error {
	reader, err := blob.Open(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	return inspectOpenMetadataReader(ctx, reader, blob.Size(), item)
}

func inspectOpenMetadataReader(ctx context.Context, reader io.Reader, wireSize int64, item repomdItem) error {
	hash, err := rpmChecksum(item.OpenSumType)
	if item.OpenChecksum == "" {
		hash = nil
		err = nil
	}
	if err != nil {
		return err
	}
	expandedLimit := int64(64 << 20)
	if wireSize > expandedLimit/200 {
		if wireSize >= int64(8<<30)/200 {
			expandedLimit = 8 << 30
		} else {
			expandedLimit = wireSize * 200
		}
	}
	if strings.HasSuffix(item.Location, ".zck") {
		output := io.Writer(io.Discard)
		if hash != nil {
			output = hash
		}
		size, err := decompressZchunk(ctx, reader, output, expandedLimit)
		if err != nil {
			return err
		}
		if item.OpenSize >= 0 && size != item.OpenSize {
			return fmt.Errorf("%s: repomd open-size mismatch: got %d, want %d", item.Location, size, item.OpenSize)
		}
		if hash != nil && !strings.EqualFold(item.OpenChecksum, hex.EncodeToString(hash.Sum(nil))) {
			return fmt.Errorf("%s: repomd open-checksum mismatch", item.Location)
		}
		return nil
	}
	var source io.Reader = reader
	var closeReader io.Closer
	switch {
	case strings.HasSuffix(item.Location, ".gz"):
		decoder, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		source, closeReader = decoder, decoder
	case strings.HasSuffix(item.Location, ".bz2"):
		source = bzip2.NewReader(reader)
	case strings.HasSuffix(item.Location, ".xz"):
		decoder, err := xz.NewReader(reader)
		if err != nil {
			return err
		}
		source = decoder
	case strings.HasSuffix(item.Location, ".zst"):
		decoder, err := zstd.NewReader(reader, zstd.WithDecoderMaxMemory(256<<20), zstd.WithDecoderMaxWindow(64<<20))
		if err != nil {
			return err
		}
		closer := decoder.IOReadCloser()
		source, closeReader = closer, closer
	}
	if closeReader != nil {
		defer closeReader.Close()
	}
	limited := &io.LimitedReader{R: source, N: expandedLimit + 1}
	counter := &countingReader{reader: limited}
	input := io.Reader(counter)
	if hash != nil {
		input = io.TeeReader(counter, hash)
	}
	if _, err := io.Copy(io.Discard, &contextReader{ctx: ctx, reader: input}); err != nil {
		return err
	}
	if limited.N == 0 {
		return fmt.Errorf("%s: decompressed metadata exceeds %d bytes", item.Location, expandedLimit)
	}
	if item.OpenSize >= 0 && counter.count != item.OpenSize {
		return fmt.Errorf("%s: repomd open-size mismatch: got %d, want %d", item.Location, counter.count, item.OpenSize)
	}
	if hash != nil && !strings.EqualFold(item.OpenChecksum, hex.EncodeToString(hash.Sum(nil))) {
		return fmt.Errorf("%s: repomd open-checksum mismatch", item.Location)
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.count += int64(n)
	return n, err
}

func isRepomdPath(cleaned string) bool {
	return path.Base(cleaned) == "repomd.xml" && path.Base(path.Dir(cleaned)) == "repodata"
}

func isRPMMetadataPath(cleaned string) bool {
	return isRepomdPath(cleaned) || strings.Contains(cleaned, "/repodata/")
}

func joinRoot(root, name string) string {
	if root == "." || root == "" {
		return strings.TrimPrefix(name, "/")
	}
	return strings.TrimSuffix(root, "/") + "/" + strings.TrimPrefix(name, "/")
}

func (h *handler) serveArtifact(w http.ResponseWriter, request *http.Request, cleaned string) {
	key := h.artifactKey(cleaned, request)
	object, _ := storeio.OpenResponse(request.Context(), h.store, rpmArtifactTenant, key)
	if object != nil {
		fresh := time.Since(object.Fetched) < rpmArtifactFreshness && !transport.RequestForcesRevalidation(request)
		if fresh {
			serveRPMArtifactObject(w, request, object, "HIT")
			return
		}
		if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
			_ = object.Reader.Close()
			h.forward(w, request, cleaned)
			return
		}

		flightKey := "revalidate:" + key
		flight, leader := h.flights.Begin(flightKey)
		if !leader {
			_ = h.flights.Wait(request.Context(), flight)
			if updated, err := storeio.OpenResponse(request.Context(), h.store, rpmArtifactTenant, key); err == nil {
				_ = object.Reader.Close()
				serveRPMArtifactObject(w, request, updated, "COALESCED")
				return
			}
			serveRPMArtifactObject(w, request, object, "STALE")
			return
		}

		header := http.Header{}
		header.Set("If-None-Match", object.Header.Get("ETag"))
		header.Set("If-Modified-Since", object.Header.Get("Last-Modified"))
		response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, header, transport.AdmissionForeground)
		if err != nil || response.StatusCode >= http.StatusInternalServerError {
			if response != nil {
				_ = response.Body.Close()
			}
			h.flights.Finish(flightKey, flight, err)
			serveRPMArtifactObject(w, request, object, "STALE")
			return
		}
		if response.StatusCode == http.StatusNotModified {
			_ = response.Body.Close()
			err = storeio.TouchResponse(h.lifecycle.Context(), h.store, rpmArtifactTenant, key, response.Header)
			h.flights.Finish(flightKey, flight, err)
			result := "REVALIDATED"
			if err != nil {
				result = "STALE"
			}
			serveRPMArtifactObject(w, request, object, result)
			return
		}
		_ = object.Reader.Close()
		if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
			h.flights.Finish(flightKey, flight, nil)
			_ = storeio.DeleteResponse(context.Background(), h.store, rpmArtifactTenant, key)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		h.streamArtifact(w, request, response, key, flightKey, flight)
		return
	}

	if request.Method == http.MethodHead || request.Header.Get("Range") != "" {
		h.forward(w, request, cleaned)
		return
	}
	flight, leader := h.flights.Begin(key)
	if !leader {
		_ = h.flights.Wait(request.Context(), flight)
		if cached, err := storeio.OpenResponse(request.Context(), h.store, rpmArtifactTenant, key); err == nil {
			serveRPMArtifactObject(w, request, cached, "COALESCED")
			return
		}
		h.forward(w, request, cleaned)
		return
	}
	if cached, err := storeio.OpenResponse(request.Context(), h.store, rpmArtifactTenant, key); err == nil {
		h.flights.Finish(key, flight, nil)
		serveRPMArtifactObject(w, request, cached, "HIT")
		return
	}
	response, err := h.fetch(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, request.Header, transport.AdmissionForeground)
	if err != nil {
		h.flights.Finish(key, flight, err)
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) {
		h.flights.Finish(key, flight, nil)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	h.streamArtifact(w, request, response, key, key, flight)
}

func (h *handler) streamArtifact(w http.ResponseWriter, request *http.Request, response *http.Response, key, flightKey string, flight *storeio.Flight) {
	header := response.Header.Clone()
	header.Del("Content-Length")
	reader, err := storeio.StartStream(h.lifecycle.Context(), storeio.StreamConfig{
		Body: response.Body, ObjectPath: key, Spooler: h.client.EnsureSpooler(h.workDir), Lifecycle: h.lifecycle, ExpectedSize: &response.ContentLength,
		StoreFn: func(ctx context.Context, body io.Reader) error {
			return storeio.PutResponse(ctx, h.store, rpmArtifactTenant, key, h.origin.String(), http.StatusOK, response.Header, "", body)
		},
		Done: func(err error) { h.flights.Finish(flightKey, flight, err) },
	})
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	defer reader.Close()
	transport.CopyEndToEndHeaders(w.Header(), header)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func (h *handler) artifactKey(cleaned string, request *http.Request) string {
	digest := sha256.Sum256([]byte(h.origin.String() + "\x00" + cleaned + "\x00" + request.URL.RawQuery + "\x00" + request.Header.Get("Accept-Encoding")))
	return "refs/" + hex.EncodeToString(digest[:])
}

func serveRPMArtifactObject(w http.ResponseWriter, request *http.Request, object *storeio.ResponseObject, result string) {
	defer object.Reader.Close()
	transport.CopyEndToEndHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", result)
	http.ServeContent(w, request, path.Base(request.URL.Path), object.Fetched, object.Reader)
}

func isRPMArtifactPath(cleaned string) bool {
	name := strings.ToLower(path.Base(cleaned))
	for _, sidecar := range []string{".asc", ".sig", ".sha256", ".sha512"} {
		name = strings.TrimSuffix(name, sidecar)
	}
	return strings.HasSuffix(name, ".rpm") || strings.HasSuffix(name, ".drpm")
}

func (h *handler) fetch(ctx context.Context, method, cleaned, rawQuery string, header http.Header, class transport.AdmissionClass) (*http.Response, error) {
	target, err := transport.JoinURL(h.origin, transport.EscapePathSegments(cleaned), "")
	if err != nil {
		return nil, err
	}
	target.RawQuery = rawQuery
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
