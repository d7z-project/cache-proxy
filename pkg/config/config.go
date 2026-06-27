package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.in/yaml.v3"
)

const (
	ModeFile   = "file"
	ModeGit    = "git"
	ModeOCI    = "oci"
	ModeNPM    = "npm"
	ModeGo     = "go"
	ModeMaven  = "maven"
	ModeCargo  = "cargo"
	ModePyPI   = "pypi"
	ModeAPK    = "apk"
	ModeDEB    = "deb"
	ModeRPM    = "rpm"
	ModePacman = "pacman"

	PolicyBypass     = "bypass"
	PolicyImmutable  = "immutable"
	PolicyRevalidate = "revalidate"

	BusyPolicyBypass = "bypass"
	BusyPolicyStale  = "stale"
)

func ValidPolicy(v string) bool {
	return v == PolicyBypass || v == PolicyImmutable || v == PolicyRevalidate
}

func ValidBusyPolicy(v string) bool { return v == "" || v == BusyPolicyBypass || v == BusyPolicyStale }

const DefaultExpireAfter Expiration = Expiration(720 * time.Hour)

type Document struct {
	Server    ServerConfig  `yaml:"server"`
	Metrics   MetricsConfig `yaml:"metrics"`
	Storage   StorageConfig `yaml:"storage"`
	Instances []Instance    `yaml:"instances"`
}

type ServerConfig struct {
	Bind      string `yaml:"bind"`
	Backend   string `yaml:"backend"`
	PublicURL string `yaml:"public_url,omitempty"`
}

type MetricsConfig struct {
	Path  string `yaml:"path"`
	Token string `yaml:"token"`
}

type StorageConfig struct {
	GC      GCConfig      `yaml:"gc"`
	Cleanup CleanupConfig `yaml:"cleanup"`
}

type GCConfig struct {
	Blob Duration `yaml:"blob"`
}

type CleanupConfig struct {
	Enabled   bool     `yaml:"enabled"`
	Interval  Duration `yaml:"interval"`
	DryRun    bool     `yaml:"dry_run"`
	BatchSize int      `yaml:"batch_size"`
	Workers   int      `yaml:"workers"`
}

func DefaultCleanupConfig() CleanupConfig {
	return CleanupConfig{
		Enabled:   false,
		Interval:  Duration(6 * time.Hour),
		DryRun:    false,
		BatchSize: 500,
		Workers:   0,
	}
}

type Instance struct {
	Name    string     `yaml:"name"`
	Enabled bool       `yaml:"enabled"`
	File    *ModeBlock `yaml:"file,omitempty"`
	Git     *ModeBlock `yaml:"git,omitempty"`
	OCI     *ModeBlock `yaml:"oci,omitempty"`
	NPM     *ModeBlock `yaml:"npm,omitempty"`
	Go      *ModeBlock `yaml:"go,omitempty"`
	Maven   *ModeBlock `yaml:"maven,omitempty"`
	Cargo   *ModeBlock `yaml:"cargo,omitempty"`
	PyPI    *ModeBlock `yaml:"pypi,omitempty"`
	APK     *ModeBlock `yaml:"apk,omitempty"`
	DEB     *ModeBlock `yaml:"deb,omitempty"`
	RPM     *ModeBlock `yaml:"rpm,omitempty"`
	Pacman  *ModeBlock `yaml:"pacman,omitempty"`
}

type TransportConfig struct {
	Proxy     string         `yaml:"proxy,omitempty"`
	UserAgent string         `yaml:"ua,omitempty"`
	Timeout   Duration       `yaml:"timeout,omitempty"`
	Health    *health.Config `yaml:"health,omitempty"`
}

type SelectedMode struct {
	Name    string
	Enabled bool
	Mode    string
	Block   *ModeBlock
}

type ModeBlock struct {
	Node *yaml.Node
}

func (b *ModeBlock) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		b.Node = nil
		return nil
	}
	clone := *value
	b.Node = &clone
	return nil
}

func (b *ModeBlock) DecodeStrict(target any) error {
	if b == nil || b.Node == nil {
		return nil
	}
	data, err := yaml.Marshal(b.Node)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	return decoder.Decode(target)
}

func (i Instance) SelectMode() (SelectedMode, error) {
	candidates := []struct {
		mode  string
		block *ModeBlock
	}{
		{mode: ModeFile, block: i.File},
		{mode: ModeGit, block: i.Git},
		{mode: ModeOCI, block: i.OCI},
		{mode: ModeNPM, block: i.NPM},
		{mode: ModeGo, block: i.Go},
		{mode: ModeMaven, block: i.Maven},
		{mode: ModeCargo, block: i.Cargo},
		{mode: ModePyPI, block: i.PyPI},
		{mode: ModeAPK, block: i.APK},
		{mode: ModeDEB, block: i.DEB},
		{mode: ModeRPM, block: i.RPM},
		{mode: ModePacman, block: i.Pacman},
	}
	var selected SelectedMode
	for _, candidate := range candidates {
		if candidate.block == nil || candidate.block.Node == nil {
			continue
		}
		if selected.Mode != "" {
			return SelectedMode{}, fmt.Errorf("instance %q must define exactly one mode block", i.Name)
		}
		selected = SelectedMode{
			Name:    strings.TrimSpace(i.Name),
			Mode:    candidate.mode,
			Enabled: i.Enabled,
			Block:   candidate.block,
		}
	}
	if selected.Mode == "" {
		return SelectedMode{}, fmt.Errorf("instance %q must define one mode block", i.Name)
	}
	return selected, nil
}

func LoadFile(path string) (*Document, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return Decode(file)
}

func Decode(r io.Reader) (*Document, error) {
	var doc Document
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }
func (d Duration) String() string          { return time.Duration(d).String() }

func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

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

type Expiration time.Duration

const ExpirationNever Expiration = -1

func (e Expiration) Duration() time.Duration { return time.Duration(e) }
func (e Expiration) IsNever() bool           { return e == ExpirationNever }
func (e Expiration) IsUnset() bool           { return e == 0 }

func (e Expiration) String() string {
	if e == ExpirationNever {
		return "never"
	}
	if e == 0 {
		return ""
	}
	return time.Duration(e).String()
}

func (e Expiration) MarshalYAML() (any, error) {
	if e == ExpirationNever {
		return "never", nil
	}
	if e == 0 {
		return nil, nil
	}
	return time.Duration(e).String(), nil
}

func (e *Expiration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Value == "" {
		*e = 0
		return nil
	}
	return e.unmarshal(value.Value)
}

func (e *Expiration) unmarshal(text string) error {
	switch text {
	case "":
		*e = 0
		return nil
	case "never", "0", "none", "infinite":
		*e = ExpirationNever
		return nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return fmt.Errorf("invalid expiration %q: %w", text, err)
	}
	if parsed < 0 {
		return fmt.Errorf("expiration must not be negative: %q", text)
	}
	*e = Expiration(parsed)
	return nil
}

type Freshness time.Duration

const FreshnessForever Freshness = -1

func (f Freshness) Duration() time.Duration { return time.Duration(f) }
func (f Freshness) IsForever() bool         { return f == FreshnessForever }
func (f Freshness) IsUnset() bool           { return f == 0 }

func (f Freshness) String() string {
	if f == FreshnessForever {
		return "forever"
	}
	if f == 0 {
		return ""
	}
	return time.Duration(f).String()
}

func (f Freshness) MarshalYAML() (any, error) {
	if f == FreshnessForever {
		return "forever", nil
	}
	if f == 0 {
		return nil, nil
	}
	return time.Duration(f).String(), nil
}

func (f *Freshness) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Value == "" {
		*f = 0
		return nil
	}
	return f.unmarshal(value.Value)
}

func (f *Freshness) unmarshal(text string) error {
	switch text {
	case "":
		*f = 0
		return nil
	case "forever", "0", "always", "infinite":
		*f = FreshnessForever
		return nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return fmt.Errorf("invalid freshness %q: %w", text, err)
	}
	if parsed < 0 {
		return fmt.Errorf("freshness must not be negative: %q", text)
	}
	*f = Freshness(parsed)
	return nil
}
