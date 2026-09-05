package oci

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const manifestAccept = "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.oci.artifact.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v1+json, application/json"

type authHandler struct {
	tokenMu    sync.Mutex
	tokens     map[string]ociToken
	preemptive string
	group      singleflight.Group
}

type refState struct {
	Header         http.Header       `yaml:"header,omitempty"`
	SourceUpstream string            `yaml:"source_upstream"`
	Repo           string            `yaml:"repo"`
	Ref            string            `yaml:"ref"`
	Representation string            `yaml:"representation,omitempty"`
	FetchedAt      time.Time         `yaml:"fetched_at"`
	ExpireAfter    config.Expiration `yaml:"expire_after"`
	ManifestDigest string            `yaml:"manifest_digest"`
	ContentType    string            `yaml:"content_type,omitempty"`
	ContentLength  int64             `yaml:"content_length"`
	ETag           string            `yaml:"etag,omitempty"`
	LastModified   string            `yaml:"last_modified,omitempty"`
	Vary           string            `yaml:"vary,omitempty"`
}

type ociToken struct {
	value  string
	expire time.Time
}

type ociChallenge struct {
	scheme string
	realm  string
	params map[string]string
}

type handler struct {
	manifestFlights storeio.FlightGroup
	name            string
	upstream        string
	expireAfter     config.Expiration
	metadataTTL     time.Duration
	workDir         string
	spooler         *storeio.Spooler
	options         *Options
	store           *blobfs.Store
	stats           *metrics.Stats
	client          *transport.UpstreamHTTPClient
	upstreamGate    *transport.UpstreamGate
	lifecycle       *storeio.Lifecycle
	auth            authHandler
	downloads       sync.Map
	refLocks        *referenceLocks
	manifestMu      sync.Mutex
	manifestReaders map[string]int
	cleanupMu       sync.Mutex
	cleanupPhase    string
	cleanupCursor   string
	cleanupRefs     map[string]struct{}
}
