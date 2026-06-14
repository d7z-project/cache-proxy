package config

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ModeFile  = "file"
	ModeOCI   = "oci"
	ModeNPM   = "npm"
	ModeGo    = "go"
	ModeMaven = "maven"
	ModeCargo = "cargo"
	ModePyPI  = "pypi"

	PolicyBypass     = "bypass"
	PolicyImmutable  = "immutable"
	PolicyRevalidate = "revalidate"

	BusyPolicyBypass = "bypass"
	BusyPolicyStale  = "stale"
)

type GlobalConfig struct {
	Version int           `json:"version" yaml:"version"`
	Metrics MetricsConfig `json:"metrics" yaml:"metrics"`
	Storage StorageConfig `json:"storage" yaml:"storage"`
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

type InstanceSummary struct {
	Name       string `json:"name" yaml:"name"`
	Mode       string `json:"mode" yaml:"mode"`
	Enabled    bool   `json:"enabled" yaml:"enabled"`
	Path       string `json:"path,omitempty" yaml:"path,omitempty"`
	Bind       string `json:"bind,omitempty" yaml:"bind,omitempty"`
	PublicURL  string `json:"publicUrl,omitempty" yaml:"public_url,omitempty"`
	EntryKind  string `json:"entryKind,omitempty" yaml:"entry_kind,omitempty"`
	EntryLabel string `json:"entryLabel,omitempty" yaml:"entry_label,omitempty"`
	EntryURL   string `json:"entryUrl,omitempty" yaml:"entry_url,omitempty"`
}

type InstanceSpec struct {
	Name   string          `json:"name" yaml:"name"`
	Meta   InstanceMeta    `json:"meta" yaml:"meta"`
	Route  InstanceRoute   `json:"route" yaml:"route"`
	Source InstanceSource  `json:"source" yaml:"source"`
	Policy json.RawMessage `json:"policy" yaml:"-"`
}

type InstanceMeta struct {
	Mode        string   `json:"mode" yaml:"mode"`
	Enabled     bool     `json:"enabled" yaml:"enabled"`
	Description string   `json:"description,omitempty" yaml:"description,omitempty"`
	ExpireAfter Duration `json:"expireAfter,omitempty" yaml:"expire_after,omitempty"`
}

type InstanceRoute struct {
	Path      string `json:"path,omitempty" yaml:"path,omitempty"`
	Bind      string `json:"bind,omitempty" yaml:"bind,omitempty"`
	PublicURL string `json:"publicUrl,omitempty" yaml:"public_url,omitempty"`
}

type InstanceSource struct {
	Upstreams []string         `json:"upstreams" yaml:"upstreams"`
	Transport *TransportConfig `json:"transport,omitempty" yaml:"transport,omitempty"`
}

type TransportConfig struct {
	Proxy     string   `json:"proxy,omitempty" yaml:"proxy,omitempty"`
	UserAgent string   `json:"ua,omitempty" yaml:"ua,omitempty"`
	Timeout   Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`
}

type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }
func (d Duration) String() string          { return time.Duration(d).String() }

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
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return err
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

func CloneGlobal(cfg *GlobalConfig) *GlobalConfig {
	if cfg == nil {
		return nil
	}
	clone := *cfg
	return &clone
}

func CloneInstance(spec InstanceSpec) InstanceSpec {
	clone := spec
	clone.Source.Upstreams = append([]string(nil), spec.Source.Upstreams...)
	if spec.Source.Transport != nil {
		transport := *spec.Source.Transport
		clone.Source.Transport = &transport
	}
	if spec.Policy != nil {
		clone.Policy = append(json.RawMessage(nil), spec.Policy...)
	}
	return clone
}

func CloneInstances(instances map[string]InstanceSpec) map[string]InstanceSpec {
	if instances == nil {
		return map[string]InstanceSpec{}
	}
	clone := make(map[string]InstanceSpec, len(instances))
	for name, spec := range instances {
		clone[name] = CloneInstance(spec)
	}
	return clone
}

func (s InstanceSpec) Summary() InstanceSummary {
	return InstanceSummary{
		Name:      s.Name,
		Mode:      s.Meta.Mode,
		Enabled:   s.Meta.Enabled,
		Path:      s.Route.Path,
		Bind:      s.Route.Bind,
		PublicURL: s.Route.PublicURL,
	}
}
