package filerepo

import (
	"fmt"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

type Policy struct {
	PassHeaders          []string          `json:"passHeaders,omitempty" yaml:"pass_headers,omitempty"`
	ArtifactPolicy       string            `json:"artifactPolicy,omitempty" yaml:"artifact_policy,omitempty"`
	ArtifactFreshFor     config.Freshness  `json:"artifactFreshFor,omitempty" yaml:"artifact_fresh_for,omitempty"`
	ArtifactBusyPolicy   string            `json:"artifactBusyPolicy,omitempty" yaml:"artifact_busy_policy,omitempty"`
	ArtifactExpireAfter  config.Expiration `json:"artifactExpireAfter,omitempty" yaml:"artifact_expire_after,omitempty"`
	AuxiliaryPolicy      string            `json:"auxiliaryPolicy,omitempty" yaml:"auxiliary_policy,omitempty"`
	AuxiliaryFreshFor    config.Freshness  `json:"auxiliaryFreshFor,omitempty" yaml:"auxiliary_fresh_for,omitempty"`
	AuxiliaryBusyPolicy  string            `json:"auxiliaryBusyPolicy,omitempty" yaml:"auxiliary_busy_policy,omitempty"`
	AuxiliaryExpireAfter config.Expiration `json:"auxiliaryExpireAfter,omitempty" yaml:"auxiliary_expire_after,omitempty"`
}

func ApplyPolicyDefaults(policy *Policy) {
	if policy.ArtifactPolicy == "" {
		policy.ArtifactPolicy = config.PolicyImmutable
	}
	if policy.ArtifactBusyPolicy == "" {
		policy.ArtifactBusyPolicy = config.BusyPolicyJoin
	}
	if policy.ArtifactExpireAfter.IsUnset() {
		policy.ArtifactExpireAfter = config.DefaultExpireAfter
	}
	if policy.AuxiliaryPolicy == "" {
		policy.AuxiliaryPolicy = config.PolicyImmutable
	}
	if policy.AuxiliaryFreshFor == 0 {
		policy.AuxiliaryFreshFor = config.Freshness(30 * time.Second)
	}
	if policy.AuxiliaryBusyPolicy == "" {
		policy.AuxiliaryBusyPolicy = config.BusyPolicyJoin
	}
	if policy.AuxiliaryExpireAfter.IsUnset() {
		policy.AuxiliaryExpireAfter = config.DefaultExpireAfter
	}
}

func ValidatePackagePolicy(mode string, policy *Policy) error {
	if err := ValidatePolicy(mode, policy.ArtifactPolicy); err != nil {
		return fmt.Errorf("artifact: %w", err)
	}
	if err := ValidatePolicy(mode, policy.AuxiliaryPolicy); err != nil {
		return fmt.Errorf("auxiliary: %w", err)
	}
	if err := ValidateBusyPolicy(mode, policy.ArtifactBusyPolicy); err != nil {
		return fmt.Errorf("artifact: %w", err)
	}
	if err := ValidateBusyPolicy(mode, policy.AuxiliaryBusyPolicy); err != nil {
		return fmt.Errorf("auxiliary: %w", err)
	}
	return ValidatePassHeaders(policy.PassHeaders)
}
