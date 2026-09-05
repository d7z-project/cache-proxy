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

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/artifactcache"
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
	spooler   *storeio.Spooler
	client    *transport.Client
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
	metadata  *filerepo.GenerationManager
	artifacts artifactcache.Cache
}

func newHandler(instance string, origin *url.URL, stateDir, workDir string, blobs *blobfs.Store, client *transport.Client, taskScheduler *scheduler.Scheduler) (*handler, error) {
	spooler := client.EnsureSpooler(workDir)
	h := &handler{origin: origin, spooler: spooler, client: client, lifecycle: storeio.NewLifecycle()}
	h.artifacts = artifactcache.Cache{
		Tenant:    rpmArtifactTenant,
		Upstream:  origin.String(),
		Freshness: rpmArtifactFreshness,
		Store:     blobs,
		Spooler:   spooler,
		Lifecycle: h.lifecycle,
		Flights:   &h.flights,
		FetchUpstream: func(ctx context.Context, method, requestPath, rawQuery string, header http.Header) (*http.Response, error) {
			return h.fetchUpstream(ctx, method, requestPath, rawQuery, header, transport.AdmissionForeground)
		},
		CacheKey: h.artifactKey,
	}
	var err error
	h.metadata, err = filerepo.New(filerepo.Config{
		RefreshInterval: client.RefreshInterval(15 * time.Minute),
		Instance:        instance,
		Mode:            config.ModeRPM,
		Tenant:          "rpm-metadata",
		Upstream:        origin.String(),
		StateDir:        stateDir,
		WorkDir:         workDir,
		Spooler:         spooler,
		AnchorMaxBytes:  maxRepomdSize,
		Store:           blobs,
		Scheduler:       taskScheduler,
		Fetch: func(ctx context.Context, requestPath string, header http.Header) (*http.Response, error) {
			return h.fetchUpstream(ctx, http.MethodGet, requestPath, "", header, transport.AdmissionRefresh)
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
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if handled, _, _ := h.metadata.ServeCurrent(w, request, cleaned, path.Base(path.Dir(cleaned)) == "repodata"); handled {
		return
	}
	if isRPMArtifactPath(cleaned) && request.Header.Get("Authorization") == "" && request.Header.Get("Cookie") == "" {
		_, _ = h.artifacts.Serve(w, request, cleaned)
		return
	}
	if request.Method != http.MethodGet || path.Base(cleaned) != "repomd.xml" || path.Base(path.Dir(cleaned)) != "repodata" || request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Range") != "" {
		h.forwardUpstream(w, request, cleaned)
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
	response, err := h.fetchUpstream(h.lifecycle.Context(), http.MethodGet, cleaned, request.URL.RawQuery, header, transport.AdmissionForeground)
	if err != nil {
		h.flights.Finish(flightKey, flight, err)
		finished = true
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || !transport.ResponseCacheable(response, false) || response.Header.Get("Content-Encoding") != "" && !strings.EqualFold(response.Header.Get("Content-Encoding"), "identity") {
		h.flights.Finish(flightKey, flight, nil)
		finished = true
		transport.WriteResponse(w, request, response, "BYPASS")
		return
	}
	spool, err := storeio.CaptureResponse(h.lifecycle.Context(), w, response, h.spooler, maxRepomdSize, "MISS")
	if err != nil {
		if storeio.SpoolBodyUntouched(err) {
			h.flights.Finish(flightKey, flight, err)
			finished = true
			h.metadata.ScheduleDiscovery(h.lifecycle, root, root, cleaned)
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		h.flights.Finish(flightKey, flight, err)
		finished = true
		slog.Warn("rpm metadata capture failed after response started", "path", cleaned, "err", err)
		return
	}
	defer func() { _ = spool.Close() }()
	_, parseErr := parseRepomdReader(h.lifecycle.Context(), spool.File)
	stageErr := parseErr
	if parseErr == nil {
		_, _ = spool.File.Seek(0, io.SeekStart)
		stageErr = h.metadata.StageAnchor(storeio.WithResponseTiming(h.lifecycle.Context(), response), root, cleaned, response.Header, spool.File)
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
	if err != nil {
		_ = reader.Close()
		return err
	}
	if err := reader.Close(); err != nil {
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
		metadataPath := path.Join(anchor.Root, location)
		blob, err := session.Fetch(ctx, filerepo.ObjectSpec{
			Path:             metadataPath,
			ExpectedSize:     expectedSize,
			Checksums:        []filerepo.Checksum{{Algorithm: item.SumType, Digest: item.Checksum}},
			AllowUnavailable: true,
		})
		if err != nil {
			return err
		}
		if blob == nil {
			continue
		}
		if item.OpenChecksum != "" || item.OpenSize >= 0 {
			reader, err := blob.Open(ctx)
			if err != nil {
				return err
			}
			inspectErr := inspectOpenMetadataReader(ctx, reader, blob.Size(), item)
			closeErr := reader.Close()
			if inspectErr != nil {
				return inspectErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	for _, suffix := range []string{".asc", ".sig"} {
		if _, err := session.Fetch(ctx, filerepo.ObjectSpec{Path: anchor.Path + suffix, AllowUnavailable: true}); err != nil {
			return err
		}
	}
	return nil
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
	source := io.Reader(reader)
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
		defer func() { _ = closeReader.Close() }()
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

func (h *handler) artifactKey(cleaned string, request *http.Request) string {
	digest := sha256.Sum256([]byte(h.origin.String() + "\x00" + cleaned + "\x00" + request.URL.RawQuery + "\x00" + request.Header.Get("Accept-Encoding")))
	return "refs/" + hex.EncodeToString(digest[:])
}

func isRPMArtifactPath(cleaned string) bool {
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		return false
	}
	name := strings.ToLower(path.Base(cleaned))
	for _, sidecar := range []string{".asc", ".sig", ".sha256", ".sha512"} {
		name = strings.TrimSuffix(name, sidecar)
	}
	return strings.HasSuffix(name, ".rpm") || strings.HasSuffix(name, ".drpm")
}

func (h *handler) fetchUpstream(ctx context.Context, method, cleaned, rawQuery string, header http.Header, class transport.AdmissionClass) (*http.Response, error) {
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

func (h *handler) forwardUpstream(w http.ResponseWriter, request *http.Request, cleaned string) {
	status, err := transport.ForwardRead(request.Context(), h.client, h.origin, w, request, cleaned)
	if err != nil && status == 0 {
		transport.WriteError(w, http.StatusBadGateway)
	}
}

func (h *handler) CloseContext(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}
