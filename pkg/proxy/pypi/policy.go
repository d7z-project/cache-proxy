package pypi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy struct {
	MetadataPolicy      string           `json:"metadataPolicy,omitempty" yaml:"metadata_policy,omitempty"`
	MetadataFreshFor    config.Freshness `json:"metadataFreshFor,omitempty" yaml:"metadata_fresh_for,omitempty"`
	MetadataBusyPolicy  string           `json:"metadataBusyPolicy,omitempty" yaml:"metadata_busy_policy,omitempty"`
	ArtifactPolicy      string           `json:"artifactPolicy,omitempty" yaml:"artifact_policy,omitempty"`
	AuxiliaryPolicy     string           `json:"auxiliaryPolicy,omitempty" yaml:"auxiliary_policy,omitempty"`
	AuxiliaryFreshFor   config.Freshness `json:"auxiliaryFreshFor,omitempty" yaml:"auxiliary_fresh_for,omitempty"`
	AuxiliaryBusyPolicy string           `json:"auxiliaryBusyPolicy,omitempty" yaml:"auxiliary_busy_policy,omitempty"`
	ProxyJSON           bool             `json:"proxyJson,omitempty" yaml:"proxy_json,omitempty"`
	ProxyCoreMetadata   bool             `json:"proxyCoreMetadata,omitempty" yaml:"proxy_core_metadata,omitempty"`
	ProxySignatures     bool             `json:"proxySignatures,omitempty" yaml:"proxy_signatures,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModePyPI }

func (Driver) DecodeJSON(data json.RawMessage) (any, error) {
	policy := &Policy{}
	if len(data) == 0 || string(data) == "null" {
		return policy, nil
	}
	if err := json.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) { return json.Marshal(policy) }

func (Driver) DecodeYAML(data []byte) (any, error) {
	policy := &Policy{}
	if len(data) == 0 {
		return policy, nil
	}
	if err := yaml.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (Driver) EncodeYAML(policy any) ([]byte, error) { return yaml.Marshal(policy) }

func (Driver) ApplyDefaults(spec *proxydriver.ResolvedSpec) {
	policy := spec.Policy.(*Policy)
	if policy.MetadataPolicy == "" {
		policy.MetadataPolicy = config.PolicyRevalidate
	}
	if policy.MetadataFreshFor == 0 {
		policy.MetadataFreshFor = config.Freshness(time.Minute)
	}
	if policy.MetadataBusyPolicy == "" {
		policy.MetadataBusyPolicy = config.BusyPolicyStale
	}
	if policy.ArtifactPolicy == "" {
		policy.ArtifactPolicy = config.PolicyImmutable
	}
	if policy.AuxiliaryPolicy == "" {
		policy.AuxiliaryPolicy = config.PolicyRevalidate
	}
	if policy.AuxiliaryFreshFor == 0 {
		policy.AuxiliaryFreshFor = config.Freshness(30 * time.Second)
	}
	if policy.AuxiliaryBusyPolicy == "" {
		policy.AuxiliaryBusyPolicy = config.BusyPolicyStale
	}
	if !policy.ProxyJSON {
		policy.ProxyJSON = true
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) != 1 {
		return errors.New("pypi mode requires exactly one upstream")
	}
	policy := spec.Policy.(*Policy)
	for _, value := range []string{policy.MetadataPolicy, policy.ArtifactPolicy, policy.AuxiliaryPolicy} {
		if value != config.PolicyBypass && value != config.PolicyImmutable && value != config.PolicyRevalidate {
			return fmt.Errorf("invalid pypi policy %q", value)
		}
	}
	for _, value := range []string{policy.MetadataBusyPolicy, policy.AuxiliaryBusyPolicy} {
		if value != config.BusyPolicyBypass && value != config.BusyPolicyStale {
			return fmt.Errorf("invalid pypi busy policy %q", value)
		}
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness {
	return spec.Policy.(*Policy).AuxiliaryFreshFor
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler := proxy.NewHandler(name, proxy.RuntimeConfig{
		Mode:            config.ModePyPI,
		ExpireAfter:     spec.Meta.ExpireAfter,
		Upstreams:       append([]string(nil), spec.Source.Upstreams...),
		Transport:       spec.Source.Transport,
		BusyPolicy:      spec.Policy.(*Policy).AuxiliaryBusyPolicy,
		DefaultFreshFor: spec.Policy.(*Policy).AuxiliaryFreshFor,
	}, store, &resolver{policy: spec.Policy.(*Policy)}, stats)
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return routeForPath(spec.Policy.(*Policy), strings.TrimPrefix(path.Clean("/"+lookupPath), "/"))
}

type resolver struct {
	policy *Policy
}

func (r *resolver) Resolve(req *http.Request) (proxy.Route, error) {
	return routeForPath(r.policy, strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/"))
}

func routeForPath(policy *Policy, lookupPath string) (proxy.Route, error) {
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "simple/"
	}
	switch {
	case lookupPath == "simple" || lookupPath == "simple/":
		return proxy.Route{
			ObjectPath:   "pypi/simple/root.html",
			UpstreamPath: "simple/",
			Policy:       policy.MetadataPolicy,
			FreshFor:     policy.MetadataFreshFor,
			BusyPolicy:   policy.MetadataBusyPolicy,
			RewriteKind:  "pypi-simple",
		}, nil
	case strings.HasPrefix(lookupPath, "simple/"):
		trimmed := strings.TrimPrefix(lookupPath, "simple/")
		if strings.HasSuffix(trimmed, "/json") {
			name := normalizeProjectName(strings.TrimSuffix(trimmed, "/json"))
			if !policy.ProxyJSON {
				return proxy.Route{}, errors.New("json simple api is disabled")
			}
			return proxy.Route{
				ObjectPath:     "pypi/simple/" + name + ".json",
				UpstreamPath:   "simple/" + name + "/",
				Policy:         policy.MetadataPolicy,
				FreshFor:       policy.MetadataFreshFor,
				BusyPolicy:     policy.MetadataBusyPolicy,
				RequestHeaders: map[string]string{"Accept": "application/vnd.pypi.simple.v1+json"},
				RewriteKind:    "pypi-simple",
			}, nil
		}
		name := normalizeProjectName(strings.TrimSuffix(trimmed, "/"))
		return proxy.Route{
			ObjectPath:   "pypi/simple/" + name + ".html",
			UpstreamPath: "simple/" + name + "/",
			Policy:       policy.MetadataPolicy,
			FreshFor:     policy.MetadataFreshFor,
			BusyPolicy:   policy.MetadataBusyPolicy,
			RewriteKind:  "pypi-simple",
		}, nil
	case strings.HasPrefix(lookupPath, "files/"):
		sourceURL, err := decodeSourceURL(path.Base(lookupPath))
		if err != nil {
			return proxy.Route{}, err
		}
		return fileRoute(policy, lookupPath, sourceURL), nil
	default:
		targetURL := strings.TrimPrefix(lookupPath, "/")
		if _, err := url.Parse(targetURL); err == nil && strings.Contains(targetURL, "://") {
			return fileRoute(policy, lookupPath, targetURL), nil
		}
		return fileRoute(policy, lookupPath, targetURL), nil
	}
}

func fileRoute(policy *Policy, lookupPath, rawURL string) proxy.Route {
	objectPath := "pypi/files/" + path.Base(lookupPath)
	if !strings.HasPrefix(lookupPath, "files/") {
		objectPath = "pypi/files/" + encodeSourceURL(rawURL)
	}
	route := proxy.Route{
		ObjectPath: objectPath,
		Policy:     policy.ArtifactPolicy,
	}
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		route.TargetURL = rawURL
	} else {
		route.UpstreamPath = lookupPath
	}
	if isAuxiliaryPath(rawURL, policy) {
		route.Policy = policy.AuxiliaryPolicy
		route.FreshFor = policy.AuxiliaryFreshFor
		route.BusyPolicy = policy.AuxiliaryBusyPolicy
	}
	return route
}

func isAuxiliaryPath(rawURL string, policy *Policy) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	cleanPath := strings.ToLower(parsed.Path)
	if policy.ProxySignatures {
		for _, suffix := range []string{".asc", ".sig", ".minisig"} {
			if strings.HasSuffix(cleanPath, suffix) {
				return true
			}
		}
	}
	if policy.ProxyCoreMetadata {
		for _, suffix := range []string{".metadata", ".json", ".attestation", ".provenance"} {
			if strings.HasSuffix(cleanPath, suffix) {
				return true
			}
		}
	}
	return false
}

func normalizeProjectName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	return strings.ReplaceAll(name, ".", "-")
}

func encodeSourceURL(raw string) string {
	return hex.EncodeToString([]byte(raw))
}

func decodeSourceURL(value string) (string, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func init() {
	proxydriver.Default.Register(Driver{})
}
