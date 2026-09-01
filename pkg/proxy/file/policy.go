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
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Options struct {
	PassHeaders []string `yaml:"pass_headers,omitempty"`
	Rules       []Rule   `yaml:"rules,omitempty"`
}

type Rule struct {
	Match string `yaml:"match"`
	Class string `yaml:"class"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeFile }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var cfg Options
	if err := plan.Decode(&cfg); err != nil {
		return err
	}
	for _, header := range cfg.PassHeaders {
		if strings.TrimSpace(header) == "" || http.CanonicalHeaderKey(header) == "" {
			return fmt.Errorf("instance %s: invalid pass header %q", plan.Name(), header)
		}
	}
	for index, rule := range cfg.Rules {
		if strings.TrimSpace(rule.Match) == "" || !doublestar.ValidatePattern(rule.Match) {
			return fmt.Errorf("instance %s: file rule %d has invalid match", plan.Name(), index)
		}
		if rule.Class != "" && rule.Class != "metadata" && rule.Class != "content" && rule.Class != "passthrough" {
			return fmt.Errorf("instance %s: file rule %d has invalid class %q", plan.Name(), index, rule.Class)
		}
	}
	if !plan.Enabled() {
		return nil
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode: config.ModeFile, ExpireAfter: plan.Retention(), MetadataTTL: plan.MetadataTTL(),
		Upstreams: plan.Upstreams(), Transport: plan.Transport(), PassHeaders: cfg.PassHeaders, UpstreamGate: plan.UpstreamGate(),
	}, plan.Store(), fileResolver{rules: cfg.Rules}, plan.Stats())
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), handler)
}

type fileResolver struct{ rules []Rule }

func (r fileResolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "" || cleanPath == "." {
		return httpcache.Route{}, errors.New("path is required")
	}
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid file request path")
	}
	class := httpcache.ClassContent
	for _, rule := range r.rules {
		if !doublestar.MatchUnvalidated(rule.Match, cleanPath) {
			continue
		}
		switch rule.Class {
		case "metadata":
			class = httpcache.ClassMetadata
		case "passthrough":
			class = httpcache.ClassPassthrough
		case "content", "":
			class = httpcache.ClassContent
		}
	}
	return httpcache.Route{Class: class, ObjectPath: path.Join("file", cleanPath), UpstreamPath: cleanPath}, nil
}
