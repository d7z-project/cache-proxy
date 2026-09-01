package cargo

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Options struct {
	AuthRequired      bool     `json:"authRequired,omitempty" yaml:"auth_required,omitempty"`
	AllowedCrateHosts []string `json:"allowedCrateHosts,omitempty" yaml:"allowed_crate_hosts,omitempty"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }

func (Driver) Mode() string { return config.ModeCargo }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var options Options
	if err := plan.Decode(&options); err != nil {
		return err
	}
	if err := validateOptions(plan.Name(), &options); err != nil {
		return err
	}
	if !plan.Enabled() {
		return nil
	}
	runtime := httpcache.RuntimeConfig{
		Mode:               config.ModeCargo,
		ExpireAfter:        plan.Retention(),
		MetadataTTL:        plan.MetadataTTL(),
		Upstreams:          plan.Upstreams(),
		Transport:          plan.Transport(),
		UpstreamGate:       plan.UpstreamGate(),
		AllowedTargetHosts: append([]string(nil), options.AllowedCrateHosts...),
		ResponseTransform:  httpcache.CargoResponseTransform,
	}
	h := httpcache.NewHandler(
		plan.Name(),
		runtime,
		plan.Store(),
		newResolver(&options),
		plan.Stats(),
	)
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), h)
}

func validateOptions(instance string, options *Options) error {
	seenHosts := make(map[string]struct{}, len(options.AllowedCrateHosts))
	for index, host := range options.AllowedCrateHosts {
		host = strings.TrimSpace(host)
		parsed, err := url.Parse("//" + host)
		if err != nil || host == "" || parsed.Host != host || parsed.Hostname() == "" || parsed.User != nil ||
			parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("instance %s: invalid cargo allowed_crate_hosts entry %q", instance, host)
		}
		host = strings.ToLower(parsed.Host)
		if _, exists := seenHosts[host]; exists {
			return fmt.Errorf("instance %s: duplicate cargo allowed_crate_hosts entry %q", instance, host)
		}
		seenHosts[host] = struct{}{}
		options.AllowedCrateHosts[index] = host
	}
	return nil
}
