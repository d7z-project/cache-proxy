package npm

import (
	"errors"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type Resolver struct {
	cfg *Policy
}

const abbreviatedMetadataType = "application/vnd.npm.install-v1+json"

func New(cfg *Policy) *Resolver {
	return &Resolver{cfg: cfg}
}

func (r *Resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid npm request path")
	}
	upstreamPath := cleanPath
	objectPath := cleanPath
	if req.URL.RawQuery != "" {
		upstreamPath += "?" + req.URL.RawQuery
		objectPath += "/" + httpcache.HashKey(req.URL.RawQuery)
	}
	if strings.HasSuffix(cleanPath, ".tgz") {
		return httpcache.Route{
			ObjectPath:   "npm/tarballs/" + objectPath,
			UpstreamPath: upstreamPath,
			Policy:       r.cfg.TarballPolicy,
			BusyPolicy:   config.BusyPolicyJoin,
		}, nil
	}
	requestHeaders := map[string]string(nil)
	if strings.Contains(strings.ToLower(req.Header.Get("Accept")), abbreviatedMetadataType) {
		objectPath += "/install-v1"
		requestHeaders = map[string]string{"Accept": abbreviatedMetadataType}
	}
	return httpcache.Route{
		ObjectPath:     "npm/metadata/" + httpcache.HashKey(objectPath),
		UpstreamPath:   upstreamPath,
		RequestHeaders: requestHeaders,
		Policy:         r.cfg.MetadataPolicy,
		BusyPolicy:     r.cfg.MetadataBusyPolicy,
		FreshFor:       r.cfg.MetadataFreshFor,
		RewriteKind:    "npm-metadata",
	}, nil
}
