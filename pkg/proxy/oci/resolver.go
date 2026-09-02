package oci

import (
	"errors"
	"net/http"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
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
	if req == nil || req.URL == nil {
		return request{}, errors.New("invalid oci request URL")
	}
	decodedPath, err := storeio.DecodeCanonicalURLPath(req.URL)
	if err != nil {
		return request{}, errors.New("invalid oci request path")
	}
	cleanPath := strings.TrimPrefix(decodedPath, "/")
	if cleanPath == "v2" || cleanPath == "v2/" {
		return request{kind: requestPing, upstreamPath: "v2"}, nil
	}
	if !safePath(cleanPath) || !strings.HasPrefix(cleanPath, "v2/") {
		return request{}, errors.New("invalid OCI request path")
	}
	parts := strings.Split(cleanPath, "/")
	for i, part := range parts {
		if part == "blobs" && i+1 < len(parts) && parts[i+1] == "uploads" {
			return request{}, errors.New("oci upload paths are not readable")
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
