package filerepo

import (
	"fmt"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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

type BasicPolicy struct {
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
}

func (p BasicPolicy) AsPolicy() *Policy {
	return &Policy{
		PassHeaders:          append([]string(nil), p.PassHeaders...),
		MetadataPolicy:       p.MetadataPolicy,
		MetadataFreshFor:     p.MetadataFreshFor,
		MetadataBusyPolicy:   p.MetadataBusyPolicy,
		MetadataExpireAfter:  p.MetadataExpireAfter,
		ArtifactPolicy:       p.ArtifactPolicy,
		ArtifactFreshFor:     p.ArtifactFreshFor,
		ArtifactBusyPolicy:   p.ArtifactBusyPolicy,
		ArtifactExpireAfter:  p.ArtifactExpireAfter,
		AuxiliaryPolicy:      p.AuxiliaryPolicy,
		AuxiliaryFreshFor:    p.AuxiliaryFreshFor,
		AuxiliaryBusyPolicy:  p.AuxiliaryBusyPolicy,
		AuxiliaryExpireAfter: p.AuxiliaryExpireAfter,
	}
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

func NewHandler(name, mode, objectRoot string, metadataFreshFor config.Freshness, classifier func(string) ResourceClass, upstreams []string, transport *config.TransportConfig, expireAfter config.Expiration, policy *Policy, store *blobfs.Store, stats *httpcache.Stats) *httpcache.Handler {
	ApplyDefaults(policy, metadataFreshFor)
	return httpcache.NewHandler(name, httpcache.RuntimeConfig{
		Mode:            mode,
		ExpireAfter:     expireAfter,
		Upstreams:       append([]string(nil), upstreams...),
		Transport:       transport,
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
	}), stats, nil)
}
