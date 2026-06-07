package oci

import (
	"errors"
	"net/http"
	"strings"

	containername "github.com/google/go-containerregistry/pkg/name"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

type Resolver struct {
	cfg *config.OCIConfig
}

func New(cfg *config.OCIConfig) *Resolver {
	return &Resolver{cfg: cfg}
}

func (r *Resolver) Resolve(req *http.Request) (proxy.Route, error) {
	cleanPath := strings.TrimLeft(req.URL.Path, "/")
	if cleanPath == "v2" || cleanPath == "v2/" {
		return proxy.Route{ObjectPath: "oci/ping", UpstreamPath: cleanPath, Policy: config.PolicyBypass}, nil
	}
	if !proxy.SafePath(cleanPath) {
		return proxy.Route{}, errors.New("invalid OCI request path")
	}
	if !strings.HasPrefix(cleanPath, "v2/") {
		return proxy.Route{}, errors.New("invalid OCI request path")
	}
	parts := strings.Split(cleanPath, "/")
	for i, part := range parts {
		if part == "blobs" && i+2 < len(parts) && parts[i+1] == "uploads" {
			return proxy.Route{}, errors.New("OCI blob uploads are not proxied")
		}
		if part == "blobs" && i+1 < len(parts) {
			digest := parts[i+1]
			if strings.Contains(digest, ":") {
				return proxy.Route{ObjectPath: "oci/blobs/" + strings.ReplaceAll(digest, ":", "/"), UpstreamPath: cleanPath, Policy: r.blobPolicy()}, nil
			}
		}
		if part == "manifests" && i+1 < len(parts) {
			repo := strings.Join(parts[1:i], "/")
			ref := parts[i+1]
			if _, err := containername.ParseReference("example.com/"+repo+":"+ref, containername.WeakValidation); err != nil && !strings.Contains(ref, ":") {
				return proxy.Route{}, err
			}
			return proxy.Route{ObjectPath: "oci/manifests/" + repo + "/" + proxy.HashKey(ref), UpstreamPath: cleanPath, Policy: r.manifestPolicy()}, nil
		}
		if part == "tags" && i+1 < len(parts) && parts[i+1] == "list" {
			repo := strings.Join(parts[1:i], "/")
			return proxy.Route{ObjectPath: "oci/tags/" + repo + "/list", UpstreamPath: cleanPath, Policy: r.tagPolicy()}, nil
		}
	}
	return proxy.Route{ObjectPath: "oci/other/" + proxy.HashKey(cleanPath), UpstreamPath: cleanPath, Policy: config.PolicyBypass}, nil
}

func (r *Resolver) blobPolicy() string {
	if r.cfg != nil && r.cfg.BlobPolicy != "" {
		return r.cfg.BlobPolicy
	}
	return config.PolicyImmutable
}

func (r *Resolver) manifestPolicy() string {
	if r.cfg != nil && r.cfg.ManifestPolicy != "" {
		return r.cfg.ManifestPolicy
	}
	return config.PolicyRevalidate
}

func (r *Resolver) tagPolicy() string {
	if r.cfg != nil && r.cfg.TagPolicy != "" {
		return r.cfg.TagPolicy
	}
	return config.PolicyRevalidate
}
