package rpm

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

const maxRepomdSize = 16 << 20

type repomdContextKey struct{}
type repomdRefreshKey struct{}

type requestRepomd struct {
	root  string
	items map[string]repomdItem
}

type anchoredHandler struct {
	base *httpcache.Handler
}

func (h *anchoredHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	root, anchor, dependent := repomdAnchor(cleanPath)
	if !dependent || cleanPath == anchor || isRepomdCompanion(cleanPath) {
		h.base.ServeHTTP(w, req)
		return
	}

	request := req.Clone(req.Context())
	request.Method = http.MethodGet
	request.URL.Path = "/" + anchor
	request.URL.RawPath = ""
	request.Header = req.Header.Clone()
	request.Header.Del("Range")
	status, body, err := httpcache.Capture(h.base, request, maxRepomdSize)
	if err != nil {
		http.Error(w, "invalid RPM repository metadata", http.StatusBadGateway)
		return
	}
	if status != http.StatusOK {
		http.Error(w, http.StatusText(status), status)
		return
	}
	parsed, err := parseRepomdReader(bytes.NewReader(body))
	if err != nil {
		http.Error(w, "invalid RPM repository metadata", http.StatusBadGateway)
		return
	}
	items, err := buildRepomdItems(root, parsed)
	if err != nil {
		http.Error(w, "invalid RPM repository metadata", http.StatusBadGateway)
		return
	}
	if _, declared := items[cleanPath]; !declared {
		refreshCtx := context.WithValue(req.Context(), repomdRefreshKey{}, true)
		request = req.Clone(refreshCtx)
		request.Method = http.MethodGet
		request.URL.Path = "/" + anchor
		request.URL.RawPath = ""
		request.Header = req.Header.Clone()
		request.Header.Del("Range")
		status, body, err = httpcache.Capture(h.base, request, maxRepomdSize)
		if err != nil || status != http.StatusOK {
			http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			return
		}
		parsed, err = parseRepomdReader(bytes.NewReader(body))
		if err != nil {
			http.Error(w, "invalid RPM repository metadata", http.StatusBadGateway)
			return
		}
		items, err = buildRepomdItems(root, parsed)
		if err != nil {
			http.Error(w, "invalid RPM repository metadata", http.StatusBadGateway)
			return
		}
		if _, declared = items[cleanPath]; !declared {
			http.NotFound(w, req)
			return
		}
	}
	ctx := context.WithValue(req.Context(), repomdContextKey{}, requestRepomd{root: root, items: items})
	h.base.ServeHTTP(w, req.Clone(ctx))
}

func buildRepomdItems(root string, parsed []repomdItem) (map[string]repomdItem, error) {
	items := make(map[string]repomdItem, len(parsed))
	for _, item := range parsed {
		if item.Location == "" || !httpcache.SafePath(item.Location) {
			return nil, fmt.Errorf("invalid repomd location %q", item.Location)
		}
		sum, err := rpmChecksum(item.SumType)
		if err != nil {
			return nil, err
		}
		digest, err := hex.DecodeString(item.Checksum)
		if err != nil || len(digest) != sum.Size() {
			return nil, fmt.Errorf("invalid repomd %s checksum for %s", item.SumType, item.Location)
		}
		item.Checksum = strings.ToLower(item.Checksum)
		cleanPath := path.Join(root, item.Location)
		if previous, exists := items[cleanPath]; exists && previous != item {
			return nil, fmt.Errorf("conflicting repomd entries for %s", cleanPath)
		}
		items[cleanPath] = item
	}
	return items, nil
}

type anchoredResolver struct{}

func (anchoredResolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "" || cleanPath == "." || !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid RPM request path")
	}
	route := httpcache.Route{
		Class:        classifyPath(cleanPath),
		ObjectPath:   path.Join("rpm", cleanPath),
		UpstreamPath: cleanPath,
	}
	if req.Context().Value(repomdRefreshKey{}) == true {
		route.Class = httpcache.ClassPassthrough
	}
	if repomd, ok := req.Context().Value(repomdContextKey{}).(requestRepomd); ok {
		item, declared := repomd.items[cleanPath]
		if !declared {
			return httpcache.Route{}, fmt.Errorf("metadata %s is not declared by repomd.xml", cleanPath)
		}
		route.ExpectedDigestType = item.SumType
		route.ExpectedDigest = item.Checksum
		route.ExpectedSize = item.Size
		route.ObjectPath = path.Join("rpm", repomd.root, "objects", strings.ToLower(item.SumType), strings.ToLower(item.Checksum))
	}
	return route, nil
}

func verifyAnchoredObject(_ *http.Request, route httpcache.Route, reader io.ReadSeeker) error {
	if route.ExpectedDigest == "" {
		return nil
	}
	return verifyRepomdChecksumReader(route.UpstreamPath, route.ExpectedDigestType, route.ExpectedDigest, route.ExpectedSize, reader)
}

func classifyPath(cleanPath string) httpcache.ObjectClass {
	if cleanPath == "repomd.xml" || strings.Contains(cleanPath, "/repodata/") || strings.HasPrefix(cleanPath, "repodata/") ||
		strings.HasSuffix(cleanPath, "/mirrorlist") || strings.HasSuffix(cleanPath, "/metalink") {
		return httpcache.ClassMetadata
	}
	return httpcache.ClassContent
}

func repomdAnchor(cleanPath string) (root, anchor string, dependent bool) {
	marker := "/repodata/"
	index := strings.LastIndex("/"+cleanPath, marker)
	if index < 0 {
		return "", "", false
	}
	root = strings.TrimPrefix(("/" + cleanPath)[:index], "/")
	return root, path.Join(root, "repodata", "repomd.xml"), true
}

func isRepomdCompanion(cleanPath string) bool {
	name := path.Base(cleanPath)
	return name == "repomd.xml.asc" || name == "repomd.xml.sig" || name == "repomd.xml.key"
}
