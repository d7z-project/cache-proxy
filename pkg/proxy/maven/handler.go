package maven

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/artifactcache"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	mavenTenant       = "maven"
	metadataFreshness = time.Minute
	snapshotFreshness = 5 * time.Minute
	releaseFreshness  = 24 * time.Hour
)

type handler struct {
	origin    *url.URL
	client    *transport.Client
	lifecycle *storeio.Lifecycle
	flights   storeio.FlightGroup
	artifacts artifactcache.Cache
}

func newHandler(origin *url.URL, workDir string, store *blobfs.Store, client *transport.Client) *handler {
	h := &handler{origin: origin, client: client, lifecycle: storeio.NewLifecycle()}
	h.artifacts = artifactcache.Cache{
		Tenant:    mavenTenant,
		Upstream:  origin.String(),
		Freshness: releaseFreshness,
		FreshnessForPath: func(requestPath string) time.Duration {
			base := path.Base(requestPath)
			if strings.HasPrefix(base, "maven-metadata.xml") {
				return client.RefreshInterval(metadataFreshness)
			}
			if strings.Contains(strings.ToUpper(requestPath), "SNAPSHOT") {
				return client.RefreshInterval(snapshotFreshness)
			}
			return releaseFreshness
		},
		Store:     store,
		Spooler:   client.EnsureSpooler(workDir),
		Lifecycle: h.lifecycle,
		Flights:   &h.flights,
		FetchUpstream: func(ctx context.Context, method, requestPath, rawQuery string, header http.Header) (*http.Response, error) {
			target, err := transport.JoinURL(origin, transport.EscapePathSegments(requestPath), rawQuery)
			if err != nil {
				return nil, err
			}
			upstreamRequest, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
			if err != nil {
				return nil, err
			}
			for _, name := range []string{"Accept", "Accept-Encoding", "Cache-Control", "If-None-Match", "If-Modified-Since", "User-Agent"} {
				for _, value := range header.Values(name) {
					upstreamRequest.Header.Add(name, value)
				}
			}
			return client.DoRead(ctx, upstreamRequest, transport.AdmissionForeground)
		},
		CacheKey: func(requestPath string, request *http.Request) string {
			return mavenKey(origin, requestPath, request)
		},
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !proxyruntime.RequireReadMethod(w, request.Method) {
		return
	}
	cleaned, err := storeio.CleanURLPath(request.URL)
	if err != nil {
		http.Error(w, "invalid Maven path", http.StatusBadRequest)
		return
	}
	if cleaned == "" || strings.HasSuffix(cleaned, "/") {
		h.forwardUpstream(w, request, cleaned)
		return
	}
	if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
		h.forwardUpstream(w, request, cleaned)
		return
	}

	_, _ = h.artifacts.Serve(w, request, cleaned)
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

func mavenKey(origin *url.URL, cleaned string, request *http.Request) string {
	hash := sha256.New()
	for _, value := range []string{origin.String(), cleaned, request.URL.RawQuery, request.Header.Get("Accept"), request.Header.Get("Accept-Encoding")} {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return "refs/" + hex.EncodeToString(hash.Sum(nil))
}
