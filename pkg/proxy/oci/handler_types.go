package oci

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const manifestAccept = "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.oci.artifact.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v1+json, application/json"

type authHandler struct {
	tokenMu sync.Mutex
	tokens  map[string]ociToken
	group   singleflight.Group
}

type refState struct {
	Repo           string            `yaml:"repo"`
	Ref            string            `yaml:"ref"`
	FetchedAt      time.Time         `yaml:"fetched_at"`
	ExpireAfter    config.Expiration `yaml:"expire_after"`
	ManifestDigest string            `yaml:"manifest_digest,omitempty"`
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
	name         string
	upstream     string
	expireAfter  config.Expiration
	policy       *Policy
	store        *blobfs.Store
	stats        *httpcache.Stats
	client       *utils.HttpClientWrapper
	upstreamGate *httpcache.UpstreamGate
	lifecycleCtx context.Context
	cancel       context.CancelFunc
	wait         sync.WaitGroup
	auth         authHandler
	downloads    sync.Map
	refLocks     *utils.RWLockGroup
}
