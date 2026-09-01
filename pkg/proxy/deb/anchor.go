package deb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

const maxReleaseSize = 16 << 20

type releaseContextKey struct{}
type releaseRefreshKey struct{}

type requestRelease struct {
	root    string
	entries map[string]releaseObject
}

type releaseObject struct {
	entry        releaseEntry
	upstreamPath string
}

type anchoredHandler struct {
	base *httpcache.Handler
}

func (h *anchoredHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	root, anchor, metadata := distributionAnchor(cleanPath)
	if !metadata || cleanPath == anchor || path.Base(cleanPath) == "Release" || isReleaseCompanion(cleanPath) {
		h.base.ServeHTTP(w, req)
		return
	}

	manifest, status, err := h.loadRelease(req, anchor, false)
	if status == http.StatusNotFound {
		manifest, status, err = h.loadRelease(req, path.Join(root, "Release"), false)
	}
	if err != nil || status != http.StatusOK {
		if err != nil {
			http.Error(w, "invalid Debian Release metadata", http.StatusBadGateway)
			return
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	entries := buildReleaseObjects(root, manifest)
	if _, declared := entries[cleanPath]; !declared {
		manifest, status, err = h.loadRelease(req, anchor, true)
		if status == http.StatusNotFound {
			manifest, status, err = h.loadRelease(req, path.Join(root, "Release"), true)
		}
		if err != nil || status != http.StatusOK {
			http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			return
		}
		entries = buildReleaseObjects(root, manifest)
		if _, declared = entries[cleanPath]; !declared {
			http.NotFound(w, req)
			return
		}
	}
	ctx := context.WithValue(req.Context(), releaseContextKey{}, requestRelease{root: root, entries: entries})
	h.base.ServeHTTP(w, req.Clone(ctx))
}

func buildReleaseObjects(root string, manifest releaseManifest) map[string]releaseObject {
	objects := make(map[string]releaseObject, len(manifest.Entries)*2)
	for _, entry := range manifest.Entries {
		canonical := path.Join(root, entry.Path)
		sourcePath := canonical
		if manifest.AcquireByHash {
			sourcePath = releaseByHashPath(canonical, entry.SHA256)
			objects[sourcePath] = releaseObject{entry: entry, upstreamPath: sourcePath}
		}
		objects[canonical] = releaseObject{entry: entry, upstreamPath: sourcePath}
	}
	return objects
}

func (h *anchoredHandler) loadRelease(req *http.Request, anchorPath string, refresh bool) (releaseManifest, int, error) {
	ctx := req.Context()
	if refresh {
		ctx = context.WithValue(ctx, releaseRefreshKey{}, true)
	}
	request := req.Clone(ctx)
	request.Method = http.MethodGet
	request.URL.Path = "/" + anchorPath
	request.URL.RawPath = ""
	request.Header = req.Header.Clone()
	request.Header.Del("Range")
	status, body, err := httpcache.Capture(h.base, request, maxReleaseSize)
	if err != nil {
		return releaseManifest{}, status, err
	}
	if status != http.StatusOK {
		return releaseManifest{}, status, nil
	}
	manifest, err := parseReleaseManifest(bytes.NewReader(body))
	if err != nil {
		return releaseManifest{}, http.StatusOK, err
	}
	return manifest, http.StatusOK, nil
}

type anchoredResolver struct{}

func (anchoredResolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "" || cleanPath == "." || !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid Debian request path")
	}
	route := httpcache.Route{
		Class:        classifyPath(cleanPath),
		ObjectPath:   path.Join("deb", cleanPath),
		UpstreamPath: cleanPath,
	}
	if req.Context().Value(releaseRefreshKey{}) == true {
		route.Class = httpcache.ClassPassthrough
	}
	if release, ok := req.Context().Value(releaseContextKey{}).(requestRelease); ok {
		object, declared := release.entries[cleanPath]
		if !declared {
			return httpcache.Route{}, fmt.Errorf("metadata %s is not declared by Release", cleanPath)
		}
		route.UpstreamPath = object.upstreamPath
		route.ExpectedSize = object.entry.Size
		route.ExpectedDigestType = "sha256"
		route.ExpectedDigest = object.entry.SHA256
		route.ObjectPath = path.Join("deb", release.root, "objects", object.entry.SHA256)
	}
	return route, nil
}

func verifyAnchoredObject(_ *http.Request, route httpcache.Route, reader io.ReadSeeker) error {
	if route.ExpectedDigest == "" {
		return nil
	}
	if route.ExpectedDigestType != "sha256" {
		return fmt.Errorf("unsupported Debian digest %q", route.ExpectedDigestType)
	}
	return verifyReleaseEntryReader(route.UpstreamPath, releaseEntry{
		Size: route.ExpectedSize, SHA256: route.ExpectedDigest,
	}, reader)
}

func distributionAnchor(cleanPath string) (root, anchor string, metadata bool) {
	parts := strings.Split(strings.Trim(cleanPath, "/"), "/")
	for index := 0; index+1 < len(parts); index++ {
		if parts[index] != "dists" || parts[index+1] == "" {
			continue
		}
		root = strings.Join(parts[:index+2], "/")
		return root, path.Join(root, "InRelease"), true
	}
	return "", "", false
}

func isReleaseCompanion(cleanPath string) bool {
	name := path.Base(cleanPath)
	return name == "Release.gpg" || name == "Release.sig" || name == "Release.asc"
}
