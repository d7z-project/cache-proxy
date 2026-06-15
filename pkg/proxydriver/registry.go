package proxydriver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

type ResolvedSpec struct {
	Name   string
	Meta   config.InstanceMeta
	Route  config.InstanceRoute
	Source config.InstanceSource
	Policy any
	Driver Driver
}

type Driver interface {
	Mode() string
	DecodeJSON(data json.RawMessage) (any, error)
	EncodeJSON(policy any) (json.RawMessage, error)
	DecodeYAML(data []byte) (any, error)
	EncodeYAML(policy any) ([]byte, error)
	ApplyDefaults(spec *ResolvedSpec)
	Validate(spec *ResolvedSpec) error
	DefaultFreshFor(spec *ResolvedSpec) config.Freshness
	NewHandler(name string, spec *ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error)
	Lookup(spec *ResolvedSpec, lookupPath string) (proxy.Route, error)
}

type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

var Default = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{drivers: map[string]Driver{}}
}

func (r *Registry) Register(driver Driver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.drivers[driver.Mode()]; exists {
		panic("duplicate proxy mode driver: " + driver.Mode())
	}
	r.drivers[driver.Mode()] = driver
}

func (r *Registry) Driver(mode string) (Driver, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	driver, ok := r.drivers[mode]
	return driver, ok
}

func (r *Registry) MustDriver(mode string) Driver {
	driver, ok := r.Driver(mode)
	if !ok {
		panic("unknown proxy mode driver: " + mode)
	}
	return driver
}

func (r *Registry) Resolve(spec config.InstanceSpec) (*ResolvedSpec, error) {
	driver, ok := r.Driver(spec.Meta.Mode)
	if !ok {
		return nil, fmt.Errorf("unsupported mode %q", spec.Meta.Mode)
	}
	policy, err := driver.DecodeJSON(spec.Policy)
	if err != nil {
		return nil, err
	}
	resolved := &ResolvedSpec{
		Name:   spec.Name,
		Meta:   spec.Meta,
		Route:  spec.Route,
		Source: spec.Source,
		Policy: policy,
		Driver: driver,
	}
	driver.ApplyDefaults(resolved)
	if err := driver.Validate(resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

func (r *Registry) Canonicalize(spec config.InstanceSpec) (config.InstanceSpec, *ResolvedSpec, error) {
	resolved, err := r.Resolve(spec)
	if err != nil {
		return config.InstanceSpec{}, nil, err
	}
	policy, err := resolved.Driver.EncodeJSON(resolved.Policy)
	if err != nil {
		return config.InstanceSpec{}, nil, err
	}
	return config.InstanceSpec{
		Name:   resolved.Name,
		Meta:   resolved.Meta,
		Route:  resolved.Route,
		Source: resolved.Source,
		Policy: policy,
	}, resolved, nil
}

func (r *Registry) ResolveFromYAML(spec config.InstanceSpec, policyData []byte) (*ResolvedSpec, error) {
	driver, ok := r.Driver(spec.Meta.Mode)
	if !ok {
		return nil, fmt.Errorf("unsupported mode %q", spec.Meta.Mode)
	}
	policy, err := driver.DecodeYAML(policyData)
	if err != nil {
		return nil, err
	}
	resolved := &ResolvedSpec{
		Name:   spec.Name,
		Meta:   spec.Meta,
		Route:  spec.Route,
		Source: spec.Source,
		Policy: policy,
		Driver: driver,
	}
	driver.ApplyDefaults(resolved)
	if err := driver.Validate(resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

func (r *Registry) EncodePolicyYAML(spec *ResolvedSpec) ([]byte, error) {
	return spec.Driver.EncodeYAML(spec.Policy)
}
