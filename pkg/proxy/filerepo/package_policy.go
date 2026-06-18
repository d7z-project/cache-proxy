package filerepo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

type Policy struct {
	PassHeaders          []string          `json:"passHeaders,omitempty" yaml:"pass_headers,omitempty"`
	MetadataPolicy       string            `json:"metadataPolicy,omitempty" yaml:"metadata_policy,omitempty"`
	MetadataFreshFor     config.Freshness  `json:"metadataFreshFor,omitempty" yaml:"metadata_fresh_for,omitempty"`
	MetadataBusyPolicy   string            `json:"metadataBusyPolicy,omitempty" yaml:"metadata_busy_policy,omitempty"`
	MetadataExpireAfter  config.Expiration `json:"metadataExpireAfter,omitempty" yaml:"metadata_expire_after,omitempty"`
	ArtifactPolicy       string            `json:"artifactPolicy,omitempty" yaml:"artifact_policy,omitempty"`
	ArtifactFreshFor     config.Freshness  `json:"artifactFreshFor,omitempty" yaml:"artifact_fresh_for,omitempty"`
	ArtifactBusyPolicy   string            `json:"artifactBusyPolicy,omitempty" yaml:"artifact_busy_policy,omitempty"`
	ArtifactExpireAfter  config.Expiration `json:"artifactExpireAfter,omitempty" yaml:"artifact_expire_after,omitempty"`
	AuxiliaryPolicy      string            `json:"auxiliaryPolicy,omitempty" yaml:"auxiliary_policy,omitempty"`
	AuxiliaryFreshFor    config.Freshness  `json:"auxiliaryFreshFor,omitempty" yaml:"auxiliary_fresh_for,omitempty"`
	AuxiliaryBusyPolicy  string            `json:"auxiliaryBusyPolicy,omitempty" yaml:"auxiliary_busy_policy,omitempty"`
	AuxiliaryExpireAfter config.Expiration `json:"auxiliaryExpireAfter,omitempty" yaml:"auxiliary_expire_after,omitempty"`
	Rules                []Rule            `json:"rules,omitempty" yaml:"rules,omitempty"`
}

func DecodeJSON(data json.RawMessage) (*Policy, error) {
	policy := &Policy{}
	if len(data) == 0 || string(data) == "null" {
		return policy, nil
	}
	if err := json.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func DecodeYAML(data []byte) (*Policy, error) {
	policy := &Policy{}
	if len(data) == 0 {
		return policy, nil
	}
	if err := yaml.Unmarshal(data, policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func ApplyDefaults(policy *Policy, metadataFreshFor config.Freshness) {
	if policy.MetadataPolicy == "" {
		policy.MetadataPolicy = config.PolicyRevalidate
	}
	if policy.MetadataFreshFor == 0 {
		policy.MetadataFreshFor = metadataFreshFor
	}
	if policy.MetadataBusyPolicy == "" {
		policy.MetadataBusyPolicy = config.BusyPolicyStale
	}
	if policy.ArtifactPolicy == "" {
		policy.ArtifactPolicy = config.PolicyImmutable
	}
	if policy.ArtifactBusyPolicy == "" {
		policy.ArtifactBusyPolicy = config.BusyPolicyBypass
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
}

func Validate(mode string, policy *Policy) error {
	for field, value := range map[string]string{
		"metadata":  policy.MetadataPolicy,
		"artifact":  policy.ArtifactPolicy,
		"auxiliary": policy.AuxiliaryPolicy,
	} {
		if err := ValidatePolicy(mode, value); err != nil {
			return fmt.Errorf("%s %s", field, err.Error())
		}
	}
	for field, value := range map[string]string{
		"metadata":  policy.MetadataBusyPolicy,
		"artifact":  policy.ArtifactBusyPolicy,
		"auxiliary": policy.AuxiliaryBusyPolicy,
	} {
		if err := ValidateBusyPolicy(mode, value); err != nil {
			return fmt.Errorf("%s %s", field, err.Error())
		}
	}
	if err := ValidatePassHeaders(policy.PassHeaders); err != nil {
		return err
	}
	return ValidateRules(mode, policy.Rules)
}

func NewHandler(name, mode, objectRoot string, metadataFreshFor config.Freshness, classifier func(string) ResourceClass, source config.InstanceSource, meta config.InstanceMeta, policy *Policy, store *blobfs.Store, stats *proxy.Stats) (*proxy.Handler, error) {
	ApplyDefaults(policy, metadataFreshFor)
	handler := proxy.NewHandler(name, proxy.RuntimeConfig{
		Mode:            mode,
		ExpireAfter:     meta.ExpireAfter,
		Upstreams:       append([]string(nil), source.Upstreams...),
		Transport:       source.Transport,
		PassHeaders:     append([]string(nil), policy.PassHeaders...),
		DefaultFreshFor: policy.AuxiliaryFreshFor,
		BusyPolicy:      policy.AuxiliaryBusyPolicy,
	}, store, NewResolver(Config{
		ObjectRoot: objectRoot,
		Defaults: Defaults{
			Metadata: CacheProfile{
				Policy:      policy.MetadataPolicy,
				FreshFor:    policy.MetadataFreshFor,
				BusyPolicy:  policy.MetadataBusyPolicy,
				ExpireAfter: policy.MetadataExpireAfter,
			},
			Artifact: CacheProfile{
				Policy:      policy.ArtifactPolicy,
				FreshFor:    policy.ArtifactFreshFor,
				BusyPolicy:  policy.ArtifactBusyPolicy,
				ExpireAfter: policy.ArtifactExpireAfter,
			},
			Auxiliary: CacheProfile{
				Policy:      policy.AuxiliaryPolicy,
				FreshFor:    policy.AuxiliaryFreshFor,
				BusyPolicy:  policy.AuxiliaryBusyPolicy,
				ExpireAfter: policy.AuxiliaryExpireAfter,
			},
		},
		Rules:      append([]Rule(nil), policy.Rules...),
		Classifier: classifier,
	}), stats)
	return handler, nil
}

func Lookup(objectRoot string, policy *Policy, classifier func(string) ResourceClass, lookupPath string) (proxy.Route, error) {
	req, _ := http.NewRequest(http.MethodGet, "/"+strings.TrimPrefix(lookupPath, "/"), nil)
	return NewResolver(Config{
		ObjectRoot: objectRoot,
		Defaults: Defaults{
			Metadata: CacheProfile{
				Policy:      policy.MetadataPolicy,
				FreshFor:    policy.MetadataFreshFor,
				BusyPolicy:  policy.MetadataBusyPolicy,
				ExpireAfter: policy.MetadataExpireAfter,
			},
			Artifact: CacheProfile{
				Policy:      policy.ArtifactPolicy,
				FreshFor:    policy.ArtifactFreshFor,
				BusyPolicy:  policy.ArtifactBusyPolicy,
				ExpireAfter: policy.ArtifactExpireAfter,
			},
			Auxiliary: CacheProfile{
				Policy:      policy.AuxiliaryPolicy,
				FreshFor:    policy.AuxiliaryFreshFor,
				BusyPolicy:  policy.AuxiliaryBusyPolicy,
				ExpireAfter: policy.AuxiliaryExpireAfter,
			},
		},
		Rules:      append([]Rule(nil), policy.Rules...),
		Classifier: classifier,
	}).Resolve(req)
}
