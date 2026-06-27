package cargo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type resolver struct {
	policy *Policy
	store  *blobfs.Store
	name   string

	cfgMu     sync.Mutex
	cfgCached bool
	cfgDL     string
}

func newResolver(policy *Policy, store *blobfs.Store, name string) *resolver {
	return &resolver{policy: policy, store: store, name: name}
}

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	lookupPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if !httpcache.SafePath(lookupPath) {
		return httpcache.Route{}, fmt.Errorf("invalid cargo request path")
	}
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "config.json"
	}
	switch {
	case lookupPath == "config.json":
		return httpcache.Route{
			ObjectPath:   "cargo/index/config.json",
			UpstreamPath: "config.json",
			Policy:       config.PolicyRevalidate,
			FreshFor:     r.policy.IndexFreshFor,
			BusyPolicy:   r.policy.IndexBusyPolicy,
			RewriteKind:  "cargo-config",
			AuthRequired: r.policy.AuthRequired,
		}, nil
	case strings.HasPrefix(lookupPath, "api/v1/crates/") && strings.HasSuffix(lookupPath, "/download"):
		objectPath := "cargo/crates/" + strings.TrimPrefix(lookupPath, "api/v1/crates/")
		return httpcache.Route{
			ObjectPath:   objectPath,
			UpstreamPath: lookupPath,
			TargetURL:    r.crateTargetURL(lookupPath),
			Policy:       r.policy.CratePolicy,
			BusyPolicy:   config.BusyPolicyBypass,
		}, nil
	default:
		return httpcache.Route{
			ObjectPath:   "cargo/index/" + lookupPath,
			UpstreamPath: lookupPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     r.policy.IndexFreshFor,
			BusyPolicy:   r.policy.IndexBusyPolicy,
		}, nil
	}
}

func (r *resolver) crateTargetURL(upstreamPath string) string {
	r.cfgMu.Lock()
	defer r.cfgMu.Unlock()
	if !r.cfgCached {
		r.cfgCached = true
		reader, err := r.store.OpenObject(context.Background(), r.name, "cargo/index/config.json")
		if err != nil {
			return ""
		}
		defer reader.Close()
		var cfg httpcache.CargoConfig
		if err := json.NewDecoder(reader).Decode(&cfg); err != nil || cfg.DL == "" {
			return ""
		}
		r.cfgDL = cfg.DL
	}
	if r.cfgDL == "" {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(upstreamPath, "api/v1/crates/"), "/", 3)
	if len(parts) < 2 || parts[len(parts)-1] != "download" {
		return ""
	}
	return strings.NewReplacer(
		"{crate}", parts[0],
		"{version}", parts[1],
		"{prefix}", "",
		"{lowerprefix}", "",
	).Replace(r.cfgDL)
}
