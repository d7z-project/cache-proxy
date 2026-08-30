package flatpak

import (
	"context"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const (
	metadataRoot        = "flatpak/metadata"
	maxMetadataSize     = 64 << 20
	maxDescriptorSize   = 10 << 20
	metadataGenerations = 3
)

type currentMetadata struct {
	Generation  string    `yaml:"generation"`
	Upstream    string    `yaml:"upstream"`
	Published   time.Time `yaml:"published"`
	Fingerprint string    `yaml:"fingerprint,omitempty"`
}

type Handler struct {
	name             string
	store            *blobfs.Store
	stats            *httpcache.Stats
	base             *httpcache.Handler
	client           *utils.HTTPClientWrapper
	upstreams        []string
	expireAfter      config.Expiration
	deltaExpireAfter config.Expiration
	refreshInterval  time.Duration
	serviceHealth    *health.ServiceHealth

	mu            sync.RWMutex
	refreshMu     sync.Mutex
	current       currentMetadata
	rewriteDesc   bool
	verifyObjects bool
	upstreamGate  *httpcache.UpstreamGate
}

func NewHandler(
	name string,
	upstreams []string,
	transport *config.TransportConfig,
	expireAfter config.Expiration,
	refreshInterval time.Duration,
	policy *Policy,
	store *blobfs.Store,
	stats *httpcache.Stats,
	svcHealth *health.ServiceHealth,
	upstreamGate *httpcache.UpstreamGate,
	runtimeCfg httpcache.RuntimeConfig,
) *Handler {
	handler := &Handler{
		name:             name,
		store:            store,
		stats:            stats,
		upstreams:        append([]string(nil), upstreams...),
		expireAfter:      expireAfter,
		deltaExpireAfter: resolveDeltaExpireAfter(policy, expireAfter),
		refreshInterval:  refreshInterval,
		serviceHealth:    svcHealth,
		rewriteDesc:      policy.DescriptorRewrite != nil && *policy.DescriptorRewrite,
		verifyObjects:    policy.VerifyObjects != nil && *policy.VerifyObjects,
		upstreamGate:     upstreamGate,
	}
	handler.client = utils.DefaultHTTPClientWrapper()
	httpcache.ConfigureClientTransport(handler.client, name, transport)
	runtimeCfg.VerifyFunc = handler.verifyCacheObject
	runtimeCfg.UpstreamGate = upstreamGate
	handler.base = httpcache.NewHandler(name, runtimeCfg, store, resolver{policy: policy}, stats, svcHealth)
	return handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "." || cleanPath == "" {
		cleanPath = "summary"
	}
	if h.rewriteDesc && isDescriptorPath(cleanPath) {
		h.serveDescriptor(w, req, cleanPath)
		return
	}
	if !isMetadataPath(cleanPath) {
		h.base.ServeHTTP(w, req)
		return
	}
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		h.base.ServeHTTP(w, req)
		return
	}
	if cleanPath == "summary" {
		h.serveSummary(w, req)
		return
	}
	h.serveCompanionMetadata(w, req, cleanPath)
}

func (h *Handler) Start(ctx context.Context) error {
	if h.serviceHealth != nil {
		h.serviceHealth.AddResource("/", []health.ResourceTarget{{Path: "summary"}}, h.upstreams)
	}
	h.cleanCurrentTemp(ctx)
	if err := h.restoreCurrent(ctx); err != nil {
		return err
	}
	if h.serviceHealth != nil && h.currentSnapshot().Generation != "" {
		h.serviceHealth.MarkResourceActive("/", []health.ResourceTarget{{Path: "summary"}})
	}
	return nil
}

func (h *Handler) Stop(ctx context.Context) error {
	return h.base.CloseContext(ctx)
}
