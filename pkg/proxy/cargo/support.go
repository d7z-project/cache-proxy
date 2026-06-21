package cargo

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
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

func cloneHeaders(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func transportForConfig(cfg *config.TransportConfig) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg == nil {
		return transport
	}
	if cfg.Proxy != "" {
		proxyURL, err := url.Parse(cfg.Proxy)
		if err == nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	if cfg.Timeout > 0 {
		transport.DialContext = utils.DefaultDialContext(cfg.Timeout.Duration())
	}
	return transport
}

type statsTransport struct {
	base     http.RoundTripper
	stats    *httpcache.Stats
	instance string
	mode     string
}

func (t *statsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		t.stats.RecordUpstream(t.instance, t.mode, req.Method, 0)
		return nil, err
	}
	t.stats.RecordUpstream(t.instance, t.mode, req.Method, resp.StatusCode)
	return resp, nil
}
