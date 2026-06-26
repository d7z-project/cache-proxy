package cargo

import (
	"fmt"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func cargoRoute(policy *Policy, lookupPath string) (httpcache.Route, error) {
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "config.json"
	}
	switch {
	case lookupPath == "config.json":
		return httpcache.Route{
			ObjectPath:   "cargo/index/config.json",
			UpstreamPath: "config.json",
			Policy:       config.PolicyRevalidate,
			FreshFor:     policy.IndexFreshFor,
			BusyPolicy:   policy.IndexBusyPolicy,
			RewriteKind:  "cargo-config",
		}, nil
	case strings.HasPrefix(lookupPath, "api/v1/crates/") && strings.HasSuffix(lookupPath, "/download"):
		return httpcache.Route{
			ObjectPath:   "cargo/crates/" + strings.TrimPrefix(lookupPath, "api/v1/crates/"),
			UpstreamPath: lookupPath,
			Policy:       policy.CratePolicy,
			BusyPolicy:   config.BusyPolicyBypass,
		}, nil
	default:
		return httpcache.Route{
			ObjectPath:   "cargo/index/" + lookupPath,
			UpstreamPath: lookupPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     policy.IndexFreshFor,
			BusyPolicy:   policy.IndexBusyPolicy,
		}, nil
	}
}

func parseCargoDownloadPath(objectPath string) (string, string, error) {
	parts := strings.Split(objectPath, "/")
	if len(parts) < 3 || parts[len(parts)-1] != "download" {
		return "", "", fmt.Errorf("invalid cargo download path")
	}
	return parts[0], parts[1], nil
}

func cargoDownloadURL(template, crate, version string) string {
	return strings.NewReplacer(
		"{crate}", crate,
		"{version}", version,
		"{prefix}", "",
		"{lowerprefix}", "",
	).Replace(template)
}
