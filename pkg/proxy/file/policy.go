package file

import (
	"context"
	"fmt"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Policy struct {
	PassHeaders   []string         `json:"passHeaders,omitempty" yaml:"pass_headers,omitempty"`
	DefaultPolicy string           `json:"defaultPolicy,omitempty" yaml:"default_policy,omitempty"`
	FreshFor      config.Freshness `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	BusyPolicy    string           `json:"busyPolicy,omitempty" yaml:"busy_policy,omitempty"`
	Rules         []Rule           `json:"rules,omitempty" yaml:"rules,omitempty"`
}

type Rule struct {
	Match       string            `json:"match,omitempty" yaml:"match,omitempty"`
	Policy      string            `json:"policy,omitempty" yaml:"policy,omitempty"`
	FreshFor    config.Freshness  `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	BusyPolicy  string            `json:"busyPolicy,omitempty" yaml:"busy_policy,omitempty"`
	ExpireAfter config.Expiration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type Block struct {
	ExpireAfter config.Expiration `yaml:"expire_after"`
	Route       struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
	Upstreams []string                `yaml:"upstreams"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Policy    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }

func (Driver) Mode() string { return config.ModeFile }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if block.DefaultPolicy == "" {
		block.DefaultPolicy = config.PolicyBypass
	}
	if block.BusyPolicy == "" {
		block.BusyPolicy = config.BusyPolicyBypass
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	packagePolicy := toPackagePolicy(block.Policy)
	if err := filerepo.Validate(config.ModeFile, packagePolicy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler := filerepo.NewHandler(
		plan.Name(),
		config.ModeFile,
		"file",
		config.Freshness(time.Minute),
		func(string) filerepo.ResourceClass { return filerepo.ResourceAuxiliary },
		block.Upstreams,
		block.Transport,
		expireAfter,
		packagePolicy,
		plan.Store(),
		plan.Stats(),
	)
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:      handler,
		Close:        func() error { handler.Close(); return nil },
		CloseContext: handler.CloseContext,
		CleanupFn:    handler.Cleanup,
	})
}

func toPackagePolicy(policy Policy) *filerepo.Policy {
	rules := make([]filerepo.Rule, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		rules = append(rules, filerepo.Rule{
			Match:         rule.Match,
			ResourceClass: filerepo.ResourceAuxiliary,
			Policy:        rule.Policy,
			FreshFor:      rule.FreshFor,
			BusyPolicy:    rule.BusyPolicy,
			ExpireAfter:   rule.ExpireAfter,
		})
	}
	return &filerepo.Policy{
		PassHeaders:         append([]string(nil), policy.PassHeaders...),
		MetadataPolicy:      policy.DefaultPolicy,
		MetadataFreshFor:    policy.FreshFor,
		MetadataBusyPolicy:  policy.BusyPolicy,
		ArtifactPolicy:      policy.DefaultPolicy,
		ArtifactFreshFor:    policy.FreshFor,
		ArtifactBusyPolicy:  policy.BusyPolicy,
		AuxiliaryPolicy:     policy.DefaultPolicy,
		AuxiliaryFreshFor:   policy.FreshFor,
		AuxiliaryBusyPolicy: policy.BusyPolicy,
		Rules:               rules,
	}
}
