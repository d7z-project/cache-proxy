package maven

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy struct {
	MetadataFreshFor    config.Freshness `json:"metadataFreshFor,omitempty" yaml:"metadata_fresh_for,omitempty"`
	MetadataBusyPolicy  string           `json:"metadataBusyPolicy,omitempty" yaml:"metadata_busy_policy,omitempty"`
	AuxiliaryPolicy     string           `json:"auxiliaryPolicy,omitempty" yaml:"auxiliary_policy,omitempty"`
	AuxiliaryFreshFor   config.Freshness `json:"auxiliaryFreshFor,omitempty" yaml:"auxiliary_fresh_for,omitempty"`
	AuxiliaryBusyPolicy string           `json:"auxiliaryBusyPolicy,omitempty" yaml:"auxiliary_busy_policy,omitempty"`
	ReleasePolicy       string           `json:"releasePolicy,omitempty" yaml:"release_policy,omitempty"`
	SnapshotPolicy      string           `json:"snapshotPolicy,omitempty" yaml:"snapshot_policy,omitempty"`
	SnapshotFreshFor    config.Freshness `json:"snapshotFreshFor,omitempty" yaml:"snapshot_fresh_for,omitempty"`
	Rules               []Rule           `json:"rules" yaml:"rules"`
}

type Rule struct {
	Match       string            `json:"match" yaml:"match"`
	Policy      string            `json:"policy,omitempty" yaml:"policy,omitempty"`
	FreshFor    config.Freshness  `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter config.Expiration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModeMaven }

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

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) {
	data, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	return data, nil
}

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
	if policy.MetadataBusyPolicy == "" {
		policy.MetadataBusyPolicy = config.BusyPolicyStale
	}
	if policy.ReleasePolicy == "" {
		policy.ReleasePolicy = config.PolicyImmutable
	}
	if policy.AuxiliaryPolicy == "" {
		policy.AuxiliaryPolicy = config.PolicyRevalidate
	}
	if policy.AuxiliaryBusyPolicy == "" {
		policy.AuxiliaryBusyPolicy = config.BusyPolicyStale
	}
	if policy.SnapshotPolicy == "" {
		policy.SnapshotPolicy = config.PolicyRevalidate
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) != 1 {
		return fmt.Errorf("maven mode requires exactly one upstream")
	}
	policy := spec.Policy.(*Policy)
	if !validPolicy(policy.ReleasePolicy) {
		return fmt.Errorf("invalid maven release policy %q", policy.ReleasePolicy)
	}
	if !validPolicy(policy.SnapshotPolicy) {
		return fmt.Errorf("invalid maven snapshot policy %q", policy.SnapshotPolicy)
	}
	if !validPolicy(policy.AuxiliaryPolicy) {
		return fmt.Errorf("invalid maven auxiliary policy %q", policy.AuxiliaryPolicy)
	}
	if policy.MetadataBusyPolicy != config.BusyPolicyBypass && policy.MetadataBusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid maven metadata busy policy %q", policy.MetadataBusyPolicy)
	}
	if policy.AuxiliaryBusyPolicy != config.BusyPolicyBypass && policy.AuxiliaryBusyPolicy != config.BusyPolicyStale {
		return fmt.Errorf("invalid maven auxiliary busy policy %q", policy.AuxiliaryBusyPolicy)
	}
	for i, rule := range policy.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("maven rule %d: match is empty", i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("maven rule %d: invalid match %q", i, rule.Match)
		}
		if rule.Policy == "" {
			rule.Policy = config.PolicyBypass
		}
		if !validPolicy(rule.Policy) {
			return fmt.Errorf("maven rule %d: invalid policy %q", i, rule.Policy)
		}
		policy.Rules[i] = rule
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness {
	return 0
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	policy := spec.Policy.(*Policy)
	handler := proxy.NewHandler(name, proxy.RuntimeConfig{
		Mode:        config.ModeMaven,
		ExpireAfter: spec.Meta.ExpireAfter,
		Upstreams:   append([]string(nil), spec.Source.Upstreams...),
		Transport:   spec.Source.Transport,
		BusyPolicy:  config.BusyPolicyBypass,
	}, store, newResolver(policy), stats)
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	req, _ := http.NewRequest(http.MethodGet, "/"+strings.TrimPrefix(lookupPath, "/"), nil)
	return newResolver(spec.Policy.(*Policy)).Resolve(req)
}

type resolver struct{ policy *Policy }

func newResolver(policy *Policy) *resolver { return &resolver{policy: policy} }

func (r *resolver) Resolve(req *http.Request) (proxy.Route, error) {
	lookupPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if lookupPath == "." || lookupPath == "" {
		return proxy.Route{}, fmt.Errorf("path is required")
	}
	route := proxy.Route{
		ObjectPath:   "maven/" + lookupPath,
		UpstreamPath: lookupPath,
		Policy:       r.defaultPolicy(lookupPath),
	}
	if isMetadataPath(lookupPath) {
		route.Policy = config.PolicyRevalidate
		route.FreshFor = r.policy.MetadataFreshFor
		route.BusyPolicy = r.policy.MetadataBusyPolicy
		route.ExpireAfter = 0
		return route, nil
	}
	if isAuxiliaryPath(lookupPath) {
		route.Policy = r.policy.AuxiliaryPolicy
		route.FreshFor = r.policy.AuxiliaryFreshFor
		route.BusyPolicy = r.policy.AuxiliaryBusyPolicy
	} else if isSnapshotPath(lookupPath) {
		route.Policy = r.policy.SnapshotPolicy
		route.FreshFor = r.policy.SnapshotFreshFor
	}
	for _, rule := range r.policy.Rules {
		if ok, _ := doublestar.Match(rule.Match, lookupPath); ok {
			route.Policy = rule.Policy
			route.FreshFor = rule.FreshFor
			route.ExpireAfter = rule.ExpireAfter
		}
	}
	return route, nil
}

func (r *resolver) defaultPolicy(lookupPath string) string {
	if isSnapshotPath(lookupPath) {
		return r.policy.SnapshotPolicy
	}
	return r.policy.ReleasePolicy
}

func isSnapshotPath(lookupPath string) bool {
	return strings.Contains(lookupPath, "-SNAPSHOT") || strings.Contains(lookupPath, "/SNAPSHOT/")
}

func validPolicy(policy string) bool {
	return policy == config.PolicyBypass || policy == config.PolicyImmutable || policy == config.PolicyRevalidate
}

func isMetadataPath(lookupPath string) bool {
	return strings.HasSuffix(lookupPath, "maven-metadata.xml")
}

func isAuxiliaryPath(lookupPath string) bool {
	if strings.Contains(lookupPath, "maven-metadata.xml.") {
		return true
	}
	for _, suffix := range []string{".sha1", ".sha256", ".sha512", ".md5", ".asc", ".sig"} {
		if strings.HasSuffix(lookupPath, suffix) {
			return true
		}
	}
	return false
}

func init() {
	proxydriver.Default.Register(Driver{})
}
