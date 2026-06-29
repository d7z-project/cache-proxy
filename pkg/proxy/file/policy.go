package file

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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
	if err := validatePolicy(&block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:            config.ModeFile,
		ExpireAfter:     expireAfter,
		Upstreams:       block.Upstreams,
		Transport:       block.Transport,
		PassHeaders:     append([]string(nil), block.PassHeaders...),
		BusyPolicy:      block.BusyPolicy,
		DefaultFreshFor: block.FreshFor,
		DownloadLimiter: plan.Downloads(),
	}, plan.Store(), fileResolver{policy: &block.Policy}, plan.Stats(), nil)
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:      handler,
		Close:        func() error { handler.Close(); return nil },
		CloseContext: handler.CloseContext,
		CleanupFn:    handler.Cleanup,
	})
}

type fileResolver struct {
	policy *Policy
}

func (r fileResolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "." || cleanPath == "" {
		return httpcache.Route{}, errors.New("path is required")
	}
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid file request path")
	}
	route := httpcache.Route{
		ObjectPath:   "file/" + cleanPath,
		UpstreamPath: cleanPath,
		Policy:       r.policy.DefaultPolicy,
		FreshFor:     r.policy.FreshFor,
		BusyPolicy:   r.policy.BusyPolicy,
	}
	for _, rule := range r.policy.Rules {
		if !doublestar.MatchUnvalidated(rule.Match, cleanPath) {
			continue
		}
		if rule.Policy != "" {
			route.Policy = rule.Policy
		}
		if rule.FreshFor != 0 {
			route.FreshFor = rule.FreshFor
		}
		if rule.BusyPolicy != "" {
			route.BusyPolicy = rule.BusyPolicy
		}
		if !rule.ExpireAfter.IsUnset() {
			route.ExpireAfter = rule.ExpireAfter
		}
	}
	return route, nil
}

func validatePolicy(policy *Policy) error {
	if err := filerepo.ValidatePassHeaders(policy.PassHeaders); err != nil {
		return err
	}
	if err := filerepo.ValidatePolicy(config.ModeFile, policy.DefaultPolicy); err != nil {
		return err
	}
	if err := filerepo.ValidateBusyPolicy(config.ModeFile, policy.BusyPolicy); err != nil {
		return err
	}
	for i, rule := range policy.Rules {
		if strings.TrimSpace(rule.Match) == "" {
			return fmt.Errorf("file rule %d: match is empty", i)
		}
		if !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("file rule %d: invalid match %q", i, rule.Match)
		}
		if rule.Policy != "" {
			if err := filerepo.ValidatePolicy(config.ModeFile, rule.Policy); err != nil {
				return err
			}
		}
		if err := filerepo.ValidateBusyPolicy(config.ModeFile, rule.BusyPolicy); err != nil {
			return err
		}
	}
	return nil
}
