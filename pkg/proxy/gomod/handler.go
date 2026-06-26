package gomod

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"

	"golang.org/x/mod/module"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

const disableModuleFetchHeader = "Disable-Module-Fetch"

type moduleRequestKind uint8

const (
	moduleRequestInvalid moduleRequestKind = iota
	moduleRequestList
	moduleRequestLatest
	moduleRequestInfo
	moduleRequestMod
	moduleRequestZip
)

type Handler struct {
	name   string
	policy *Policy
	store  *blobfs.Store
	base   *httpcache.Handler
}

type moduleRequest struct {
	kind       moduleRequestKind
	modulePath string
	version    string
	cacheKey   string
}

func NewHandler(name string, expireAfter config.Expiration, upstreams []string, transport *config.TransportConfig, policy *Policy, store *blobfs.Store, stats *httpcache.Stats) (*Handler, error) {
	if policy == nil {
		policy = &Policy{}
	}
	applyDefaults(policy)
	base := httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:            config.ModeGo,
		ExpireAfter:     expireAfter,
		Upstreams:       append([]string(nil), upstreams...),
		Transport:       transport,
		BusyPolicy:      policy.ModuleBusyPolicy,
		DefaultFreshFor: policy.ModuleFreshFor,
	}, store, &resolver{policy: policy}, stats, nil)
	return &Handler{name: name, policy: policy, store: store, base: base}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	target := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if modulePath, ok := modulePathFromTarget(target); ok && matchesPrivateModule(h.policy, modulePath) {
		http.NotFound(w, req)
		return
	}
	route, err := (&resolver{policy: h.policy}).Resolve(req)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	if req.Header.Get(disableModuleFetchHeader) != "" && h.policy.DisableModuleFetchHeader {
		if _, err := h.store.OpenObject(req.Context(), h.name, route.ObjectPath); err != nil {
			http.NotFound(w, req)
			return
		}
	}
	h.base.ServeHTTP(w, req)
}

func (h *Handler) Close() {
	h.base.Close()
}

func (h *Handler) CloseContext(ctx context.Context) error {
	return h.base.CloseContext(ctx)
}

func (h *Handler) Cleanup(ctx context.Context) error {
	return h.base.Cleanup(ctx)
}

type resolver struct {
	policy *Policy
}

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	target := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if strings.HasPrefix(target, "sumdb/") {
		return r.resolveSumDB(target)
	}
	moduleReq, err := parseModuleRequest(target)
	if err != nil {
		return httpcache.Route{}, err
	}
	route := httpcache.Route{
		ObjectPath:   "go/" + moduleReq.cacheKey,
		UpstreamPath: moduleReq.cacheKey,
		Policy:       r.policy.ModulePolicy,
		FreshFor:     r.policy.ModuleFreshFor,
		BusyPolicy:   r.policy.ModuleBusyPolicy,
	}
	if moduleReq.kind == moduleRequestZip {
		route.Policy = r.policy.ZipPolicy
		route.BusyPolicy = config.BusyPolicyBypass
	}
	return route, nil
}

func (r *resolver) resolveSumDB(target string) (httpcache.Route, error) {
	if r.policy == nil || r.policy.SumDB == nil || !r.policy.SumDB.Enabled {
		return httpcache.Route{}, fs.ErrNotExist
	}
	name := strings.TrimSpace(r.policy.SumDB.Name)
	prefix := "sumdb/" + name + "/"
	if name == "" || !strings.HasPrefix(target, prefix) {
		return httpcache.Route{}, fs.ErrNotExist
	}
	baseURL, err := url.Parse(strings.TrimSpace(r.policy.SumDB.URL))
	if err != nil {
		return httpcache.Route{}, err
	}
	return httpcache.Route{
		ObjectPath: "go/" + target,
		TargetURL:  baseURL.JoinPath(strings.TrimPrefix(target, prefix)).String(),
		Policy:     config.PolicyRevalidate,
		FreshFor:   r.policy.SumDBFreshFor,
		BusyPolicy: r.policy.SumDBBusyPolicy,
	}, nil
}

func parseModuleRequest(target string) (moduleRequest, error) {
	target = strings.TrimPrefix(path.Clean("/"+target), "/")
	if target == "." || target == "" || strings.HasPrefix(target, "sumdb/") {
		return moduleRequest{}, fs.ErrNotExist
	}
	modulePath, suffix, ok := strings.Cut(target, "/@")
	if !ok || modulePath == "" {
		return moduleRequest{}, fs.ErrNotExist
	}
	unescapedModulePath, err := module.UnescapePath(modulePath)
	if err != nil || unescapedModulePath == "" {
		return moduleRequest{}, fs.ErrNotExist
	}
	switch suffix {
	case "v/list":
		return moduleRequest{kind: moduleRequestList, modulePath: unescapedModulePath, cacheKey: target, version: "list"}, nil
	case "latest":
		return moduleRequest{kind: moduleRequestLatest, modulePath: unescapedModulePath, cacheKey: target, version: "latest"}, nil
	}
	if !strings.HasPrefix(suffix, "v/") {
		return moduleRequest{}, fs.ErrNotExist
	}
	versionFile := strings.TrimPrefix(suffix, "v/")
	for _, candidate := range []struct {
		kind   moduleRequestKind
		suffix string
	}{
		{kind: moduleRequestInfo, suffix: ".info"},
		{kind: moduleRequestMod, suffix: ".mod"},
		{kind: moduleRequestZip, suffix: ".zip"},
	} {
		if strings.HasSuffix(versionFile, candidate.suffix) {
			version := strings.TrimSuffix(versionFile, candidate.suffix)
			if version == "" || strings.Contains(version, "/") {
				return moduleRequest{}, errors.New("invalid go module version")
			}
			if candidate.kind != moduleRequestList && candidate.kind != moduleRequestLatest {
				if normalized := module.CanonicalVersion(version); normalized == "" || normalized != version {
					return moduleRequest{}, unsupportedQueryError(version)
				}
			}
			return moduleRequest{kind: candidate.kind, modulePath: unescapedModulePath, version: version, cacheKey: target}, nil
		}
	}
	return moduleRequest{}, fs.ErrNotExist
}

func unsupportedQueryError(query string) error {
	return fmt.Errorf("go module query %q requires direct source resolution, which this proxy disables: %w", query, fs.ErrNotExist)
}
