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

type BasicPolicy struct {
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

func (p BasicPolicy) AsPolicy() *Policy {
	return &Policy{
		PassHeaders:          append([]string(nil), p.PassHeaders...),
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

func ApplyDefaults(policy *Policy) {
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
		policy.AuxiliaryBusyPolicy = config.BusyPolicyBypass
	}
}

func Validate(mode string, policy *Policy) error {
	for field, value := range map[string]string{
		"artifact":  policy.ArtifactPolicy,
		"auxiliary": policy.AuxiliaryPolicy,
	} {
		if err := ValidatePolicy(mode, value); err != nil {
			return fmt.Errorf("%s %s", field, err.Error())
		}
	}
	for field, value := range map[string]string{
		"artifact":  policy.ArtifactBusyPolicy,
		"auxiliary": policy.AuxiliaryBusyPolicy,
	} {
		if err := ValidateBusyPolicy(mode, value); err != nil {
			return fmt.Errorf("%s %s", field, err.Error())
		}
	}
	return ValidatePassHeaders(policy.PassHeaders)
}
