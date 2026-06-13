package gomod

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy struct {
	SumDB                    string   `json:"sumdb,omitempty" yaml:"sumdb,omitempty"`
	NoSumDB                  string   `json:"noSumDB,omitempty" yaml:"no_sumdb,omitempty"`
	ProxiedSumDBs            []string `json:"proxiedSumDBs,omitempty" yaml:"proxied_sumdbs,omitempty"`
	DisableModuleFetchHeader bool     `json:"disableModuleFetchHeader,omitempty" yaml:"disable_module_fetch_header,omitempty"`
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
	if strings.TrimSpace(policy.SumDB) == "" {
		policy.SumDB = "sum.golang.org"
	}
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	policy := spec.Policy.(*Policy)
	sumdb := strings.TrimSpace(policy.SumDB)
	if strings.ContainsAny(sumdb, "\r\n") {
		return errors.New("go sumdb must not contain line breaks")
	}
	for i, item := range policy.ProxiedSumDBs {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("go proxied_sumdbs %d is empty", i)
		}
		if strings.ContainsAny(item, "\r\n") {
			return fmt.Errorf("go proxied_sumdbs %d must not contain line breaks", i)
		}
	}
	if strings.ContainsAny(policy.NoSumDB, "\r\n") {
		return errors.New("go no_sumdb must not contain line breaks")
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
	return proxy.Route{ObjectPath: "go/" + lookupPath, Policy: "goproxy"}, nil
}

func init() {
	proxydriver.Default.Register(Driver{})
}
