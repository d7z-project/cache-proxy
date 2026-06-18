package file

import (
	"encoding/json"
	"net/http"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/filerepo"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy = filerepo.Policy
type Rule = filerepo.Rule

type Driver struct{}

func (Driver) Mode() string { return config.ModeFile }

func (Driver) DecodeJSON(data json.RawMessage) (any, error) { return filerepo.DecodeJSON(data) }

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) { return json.Marshal(policy) }

func (Driver) DecodeYAML(data []byte) (any, error) { return filerepo.DecodeYAML(data) }

func (Driver) EncodeYAML(policy any) ([]byte, error) { return yaml.Marshal(policy) }

func (Driver) ApplyDefaults(spec *proxydriver.ResolvedSpec) {
	policy := spec.Policy.(*Policy)
	if policy.MetadataPolicy == "" {
		policy.MetadataPolicy = config.PolicyBypass
	}
	if policy.MetadataBusyPolicy == "" {
		policy.MetadataBusyPolicy = config.BusyPolicyBypass
	}
	if policy.ArtifactPolicy == "" {
		policy.ArtifactPolicy = config.PolicyBypass
	}
	if policy.ArtifactBusyPolicy == "" {
		policy.ArtifactBusyPolicy = config.BusyPolicyBypass
	}
	if policy.AuxiliaryPolicy == "" {
		policy.AuxiliaryPolicy = config.PolicyBypass
	}
	if policy.AuxiliaryBusyPolicy == "" {
		policy.AuxiliaryBusyPolicy = config.BusyPolicyBypass
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	return filerepo.Validate(config.ModeFile, spec.Policy.(*Policy))
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness {
	return spec.Policy.(*Policy).AuxiliaryFreshFor
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler, err := filerepo.NewHandler(name, config.ModeFile, "file", config.Freshness(time.Minute), func(string) filerepo.ResourceClass {
		return filerepo.ResourceAuxiliary
	}, spec.Source, spec.Meta, spec.Policy.(*Policy), store, stats)
	if err != nil {
		return nil, nil, err
	}
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return filerepo.Lookup("file", spec.Policy.(*Policy), func(string) filerepo.ResourceClass {
		return filerepo.ResourceAuxiliary
	}, lookupPath)
}

func init() {
	proxydriver.Default.Register(Driver{})
}
