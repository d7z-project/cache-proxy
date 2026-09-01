package oci

import (
	"errors"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type requestKind uint8

const (
	requestInvalid requestKind = iota
	requestPing
	requestTags
	requestManifest
	requestBlob
	requestBypass
)

type request struct {
	kind         requestKind
	repo         string
	ref          string
	digest       string
	upstreamPath string
}

func resolveRequest(req *http.Request, _ *Options) (request, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "v2" || cleanPath == "v2/" {
		return request{kind: requestPing, upstreamPath: "v2"}, nil
	}
	if !httpcache.SafePath(cleanPath) || !strings.HasPrefix(cleanPath, "v2/") {
		return request{}, errors.New("invalid OCI request path")
	}
	parts := strings.Split(cleanPath, "/")
	for i, part := range parts {
		if part == "blobs" && i+2 < len(parts) && parts[i+1] == "uploads" {
			return request{}, errors.New("oci blob uploads are not proxied")
		}
		if part == "manifests" && i+1 < len(parts) {
			repo := strings.Join(parts[1:i], "/")
			ref := parts[i+1]
			if repo == "" || ref == "" {
				return request{}, errors.New("invalid OCI manifest path")
			}
			return request{kind: requestManifest, repo: repo, ref: ref, upstreamPath: cleanPath}, nil
		}
		if part == "blobs" && i+1 < len(parts) {
			repo := strings.Join(parts[1:i], "/")
			digest := parts[i+1]
			if repo == "" || !isSHA256Digest(digest) {
				return request{}, errors.New("invalid OCI blob path")
			}
			return request{kind: requestBlob, repo: repo, digest: digest, upstreamPath: cleanPath}, nil
		}
		if part == "tags" && i+1 < len(parts) && parts[i+1] == "list" {
			return request{kind: requestTags, repo: strings.Join(parts[1:i], "/"), upstreamPath: cleanPath}, nil
		}
	}
	return request{kind: requestBypass, upstreamPath: cleanPath}, nil
}
