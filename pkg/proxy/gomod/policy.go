package gomod

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"golang.org/x/mod/module"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type SumDBConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Name    string `json:"name,omitempty" yaml:"name,omitempty"`
	URL     string `json:"url,omitempty" yaml:"url,omitempty"`
}

type Policy struct {
	SumDB                    *SumDBConfig `json:"sumdb,omitempty" yaml:"sumdb,omitempty"`
	GOPrivate                []string     `json:"goprivate,omitempty" yaml:"goprivate,omitempty"`
	DisableModuleFetchHeader bool         `json:"disableModuleFetchHeader,omitempty" yaml:"disable_module_fetch_header,omitempty"`
}

type Driver struct{}

func (Driver) Mode() string { return config.ModeGo }

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
	return json.RawMessage(data), nil
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
	if policy.SumDB == nil {
		policy.SumDB = &SumDBConfig{
			Enabled: true,
			Name:    "sum.golang.org",
			URL:     "https://sum.golang.org",
		}
		return
	}
	if !policy.SumDB.Enabled {
		policy.SumDB.Name = ""
		policy.SumDB.URL = ""
		return
	}
	if strings.TrimSpace(policy.SumDB.Name) == "" {
		policy.SumDB.Name = "sum.golang.org"
	}
	if strings.TrimSpace(policy.SumDB.URL) == "" {
		policy.SumDB.URL = "https://sum.golang.org"
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	policy := spec.Policy.(*Policy)
	if len(spec.Source.Upstreams) == 0 {
		return errors.New("go proxy requires at least one GOPROXY upstream")
	}
	for i, raw := range spec.Source.Upstreams {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("go upstream %d must be a valid absolute URL", i)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("go upstream %d must use http or https", i)
		}
	}
	if policy.SumDB != nil && policy.SumDB.Enabled {
		name := strings.TrimSpace(policy.SumDB.Name)
		if name == "" {
			return errors.New("go sumdb name is required when sumdb proxying is enabled")
		}
		if strings.ContainsAny(name, "\r\n\t ") {
			return errors.New("go sumdb name must not contain spaces or line breaks")
		}
		rawURL := strings.TrimSpace(policy.SumDB.URL)
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return errors.New("go sumdb upstream must be a valid absolute URL")
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return errors.New("go sumdb upstream must use http or https")
		}
	}
	for i, pattern := range policy.GOPrivate {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			return fmt.Errorf("go goprivate %d is empty", i)
		}
		if strings.ContainsAny(pattern, "\r\n") {
			return fmt.Errorf("go goprivate %d must not contain line breaks", i)
		}
	}
	return nil
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Duration {
	return 0
}

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler, err := NewHandler(name, spec.Meta, spec.Source, spec.Policy.(*Policy), store, stats)
	if err != nil {
		return nil, nil, err
	}
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	lookupPath = strings.TrimPrefix(path.Clean("/"+lookupPath), "/")
	if lookupPath == "." || lookupPath == "" {
		return proxy.Route{}, errors.New("path is required")
	}
	if modulePath, ok := modulePathFromTarget(lookupPath); ok && matchesPrivateModule(spec.Policy.(*Policy), modulePath) {
		return proxy.Route{Policy: "private-bypass"}, nil
	}
	return proxy.Route{ObjectPath: "go/" + lookupPath, Policy: "proxy-only"}, nil
}

func matchesPrivateModule(policy *Policy, modulePath string) bool {
	if policy == nil || len(policy.GOPrivate) == 0 || modulePath == "" {
		return false
	}
	patterns := make([]string, 0, len(policy.GOPrivate))
	for _, pattern := range policy.GOPrivate {
		pattern = strings.TrimSpace(pattern)
		if pattern != "" {
			patterns = append(patterns, pattern)
		}
	}
	if len(patterns) == 0 {
		return false
	}
	return module.MatchPrefixPatterns(strings.Join(patterns, ","), modulePath)
}

func modulePathFromTarget(target string) (string, bool) {
	target = strings.TrimPrefix(path.Clean("/"+target), "/")
	if target == "." || target == "" || strings.HasPrefix(target, "sumdb/") {
		return "", false
	}
	modulePath, _, ok := strings.Cut(target, "/@")
	if !ok || modulePath == "" {
		return "", false
	}
	unescaped, err := module.UnescapePath(modulePath)
	if err != nil || unescaped == "" {
		return "", false
	}
	return unescaped, true
}

func init() {
	proxydriver.Default.Register(Driver{})
}
