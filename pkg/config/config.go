package config

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ModeFile = "file"
	ModeOCI  = "oci"
	ModeNPM  = "npm"

	PolicyBypass     = "bypass"
	PolicyImmutable  = "immutable"
	PolicyRevalidate = "revalidate"

	BusyPolicyBypass = "bypass"
	BusyPolicyStale  = "stale"
)

type Config struct {
	Version   int                       `json:"version" yaml:"version"`
	Server    ServerConfig              `json:"server" yaml:"server"`
	Storage   StorageConfig             `json:"storage" yaml:"storage"`
	Instances map[string]InstanceConfig `json:"instances" yaml:"instances"`
}

type ServerConfig struct {
	Metrics MetricsConfig `json:"metrics" yaml:"metrics"`
}

type MetricsConfig struct {
	Path string `json:"path" yaml:"path"`
}

type StorageConfig struct {
	GC GCConfig `json:"gc" yaml:"gc"`
}

type GCConfig struct {
	Blob Duration `json:"blob" yaml:"blob"`
}

type InstanceConfig struct {
	Mode        string           `json:"mode" yaml:"mode"`
	Listen      ListenConfig     `json:"listen" yaml:"listen"`
	Upstreams   []string         `json:"upstreams" yaml:"upstreams"`
	Transport   *TransportConfig `json:"transport,omitempty" yaml:"transport,omitempty"`
	Cache       CacheConfig      `json:"cache" yaml:"cache"`
	OCI         *OCIConfig       `json:"oci,omitempty" yaml:"oci,omitempty"`
	NPM         *NPMConfig       `json:"npm,omitempty" yaml:"npm,omitempty"`
	PassHeaders []string         `json:"passHeaders,omitempty" yaml:"pass_headers,omitempty"`
	ExpireAfter Duration         `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type ListenConfig struct {
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
	Bind string `json:"bind,omitempty" yaml:"bind,omitempty"`
}

type TransportConfig struct {
	Proxy     string   `json:"proxy,omitempty" yaml:"proxy,omitempty"`
	UserAgent string   `json:"ua,omitempty" yaml:"ua,omitempty"`
	Timeout   Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`
}

type CacheConfig struct {
	DefaultPolicy string      `json:"defaultPolicy" yaml:"default_policy"`
	FreshFor      Duration    `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	BusyPolicy    string      `json:"busyPolicy,omitempty" yaml:"busy_policy,omitempty"`
	Rules         []CacheRule `json:"rules" yaml:"rules"`
}

type CacheRule struct {
	Match       string   `json:"match" yaml:"match"`
	Policy      string   `json:"policy" yaml:"policy"`
	FreshFor    Duration `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter Duration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type OCIConfig struct {
	DefaultPolicy string          `json:"defaultPolicy" yaml:"default_policy"`
	Auth          *OCIAuthConfig  `json:"auth,omitempty" yaml:"auth,omitempty"`
	Rules         []OCICacheRule  `json:"rules" yaml:"rules"`
}

type OCICacheRule struct {
	Match          string   `json:"match" yaml:"match"`
	ResourcePolicy string   `json:"resourcePolicy" yaml:"resource_policy"`
	Policy         string   `json:"policy" yaml:"policy"`
	FreshFor       Duration `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter    Duration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type OCIAuthConfig struct {
	Type     string `json:"type" yaml:"type"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Token    string `json:"token,omitempty" yaml:"token,omitempty"`
}

type NPMConfig struct {
	DefaultPolicy string         `json:"defaultPolicy" yaml:"default_policy"`
	Rules         []NPMCacheRule `json:"rules" yaml:"rules"`
}

type NPMCacheRule struct {
	Match          string   `json:"match" yaml:"match"`
	ResourcePolicy string   `json:"resourcePolicy" yaml:"resource_policy"`
	Policy         string   `json:"policy" yaml:"policy"`
	FreshFor       Duration `json:"freshFor,omitempty" yaml:"fresh_for,omitempty"`
	ExpireAfter    Duration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }
func (d Duration) String() string          { return time.Duration(d).String() }

func (c *Config) Redacted() *Config {
	clone := *c
	clone.Instances = make(map[string]InstanceConfig, len(c.Instances))
	for name, inst := range c.Instances {
		clone.Instances[name] = inst.Redacted()
	}
	return &clone
}

func (i InstanceConfig) Redacted() InstanceConfig {
	if i.OCI != nil && i.OCI.Auth != nil {
		ociClone := *i.OCI
		authClone := *ociClone.Auth
		if authClone.Password != "" {
			authClone.Password = "***"
		}
		if authClone.Token != "" {
			authClone.Token = "***"
		}
		ociClone.Auth = &authClone
		inst := i
		inst.OCI = &ociClone
		return inst
	}
	return i
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	if text == "" {
		*d = 0
		return nil
	}
	parsed, parseErr := time.ParseDuration(text)
	if parseErr != nil {
		return parseErr
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Value == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	*d = Duration(parsed)
	return nil
}
