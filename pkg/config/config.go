package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var validNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func ValidInstanceName(name string) bool {
	return name != "" && validNameRE.MatchString(name)
}

const (
	ModeFile    = "file"
	ModeGit     = "git"
	ModeOCI     = "oci"
	ModeNPM     = "npm"
	ModeGo      = "go"
	ModeMaven   = "maven"
	ModeCargo   = "cargo"
	ModePyPI    = "pypi"
	ModeFlatpak = "flatpak"
	ModeAPK     = "apk"
	ModeDEB     = "deb"
	ModeRPM     = "rpm"
	ModePacman  = "pacman"
)

func ValidateHTTPURL(raw string) error {
	_, err := parseHTTPURL(raw)
	return err
}

func parseHTTPURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" {
		return nil, errors.New("must be a valid absolute URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("must use http or https")
	}
	return parsed, nil
}

func ValidateHTTPUpstream(raw string) error {
	parsed, err := parseHTTPURL(raw)
	if err != nil {
		return err
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("must not contain a query or fragment")
	}
	return nil
}

const DefaultRetention Expiration = Expiration(720 * time.Hour)

type Document struct {
	Server    ServerConfig  `yaml:"server"`
	Metrics   MetricsConfig `yaml:"metrics"`
	Storage   StorageConfig `yaml:"storage"`
	Instances []Instance    `yaml:"instances"`
}

type ServerConfig struct {
	Bind      string             `yaml:"bind"`
	Backend   string             `yaml:"backend"`
	PublicURL string             `yaml:"public_url,omitempty"`
	Status    ServerStatusConfig `yaml:"status"`
}

type ServerStatusConfig struct {
	DiskSampleInterval Duration `yaml:"disk_sample_interval"`
	DiskHistoryWindow  Duration `yaml:"disk_history_window"`
	EventLimit         int      `yaml:"event_limit"`
}

type MetricsConfig struct {
	Path  string `yaml:"path"`
	Token string `yaml:"token"`
}

type StorageConfig struct {
	GC           GCConfig       `yaml:"gc"`
	Cleanup      CleanupConfig  `yaml:"cleanup"`
	OrphanPolicy string         `yaml:"orphan_policy,omitempty"`
	Download     DownloadConfig `yaml:"download"`
}

type GCConfig struct {
	Blob Duration `yaml:"blob"`
}

type CleanupConfig struct {
	DryRun    bool `yaml:"dry_run"`
	BatchSize int  `yaml:"batch_size"`
}

type DownloadConfig struct {
	MaxActive          int                           `yaml:"max_active"`
	MaxActivePerHost   int                           `yaml:"max_active_per_host"`
	MinInterval        Duration                      `yaml:"min_interval"`
	MaxCacheObjectSize ByteSize                      `yaml:"max_cache_object_size"`
	MaxActiveSpoolSize ByteSize                      `yaml:"max_active_spool_size"`
	Hosts              map[string]DownloadHostConfig `yaml:"hosts,omitempty"`
}

type DownloadHostConfig struct {
	MaxActive   int      `yaml:"max_active,omitempty"`
	MinInterval Duration `yaml:"min_interval,omitempty"`
}

func DefaultCleanupConfig() CleanupConfig {
	return CleanupConfig{
		DryRun:    false,
		BatchSize: 500,
	}
}

type Instance struct {
	Name       string           `yaml:"name"`
	Enabled    bool             `yaml:"enabled"`
	Mode       string           `yaml:"mode"`
	Path       string           `yaml:"path,omitempty"`
	Bind       string           `yaml:"bind,omitempty"`
	DisplayURL string           `yaml:"display_url,omitempty"`
	Upstream   string           `yaml:"upstream"`
	Transport  *TransportConfig `yaml:"transport,omitempty"`
	Options    *OptionsBlock    `yaml:"options,omitempty"`
}

type TransportConfig struct {
	Proxy              string   `yaml:"proxy,omitempty"`
	UserAgent          string   `yaml:"ua,omitempty"`
	DialTimeout        Duration `yaml:"dial_timeout,omitempty"`
	HeaderTimeout      Duration `yaml:"header_timeout,omitempty"`
	IdleBodyTimeout    Duration `yaml:"idle_body_timeout,omitempty"`
	MaxRequestDuration Duration `yaml:"max_request_duration,omitempty"`
	MaxIdleConns       int      `yaml:"max_idle_conns,omitempty"`
}

func ValidateTransport(transport *TransportConfig) error {
	if transport == nil {
		return nil
	}
	if rawProxy := strings.TrimSpace(transport.Proxy); rawProxy != "" {
		parsed, err := url.Parse(rawProxy)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return errors.New("proxy must be a valid absolute URL")
		}
		switch parsed.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return fmt.Errorf("proxy scheme %q is not supported", parsed.Scheme)
		}
	}
	for _, field := range []struct {
		name  string
		value Duration
	}{
		{name: "dial_timeout", value: transport.DialTimeout},
		{name: "header_timeout", value: transport.HeaderTimeout},
		{name: "idle_body_timeout", value: transport.IdleBodyTimeout},
		{name: "max_request_duration", value: transport.MaxRequestDuration},
	} {
		if field.value < 0 {
			return fmt.Errorf("%s must not be negative", field.name)
		}
	}
	if transport.MaxIdleConns < 0 {
		return errors.New("max_idle_conns must not be negative")
	}
	return nil
}

type SelectedMode struct {
	Name    string
	Enabled bool
	Mode    string
	Options *OptionsBlock
}

type OptionsBlock struct {
	Node *yaml.Node
}

func (b *OptionsBlock) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		b.Node = nil
		return nil
	}
	clone := *value
	b.Node = &clone
	return nil
}

func (b *OptionsBlock) DecodeStrict(target any) error {
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
	name := strings.TrimSpace(i.Name)
	if !ValidInstanceName(name) {
		return SelectedMode{}, fmt.Errorf("invalid instance name %q: must match %s", i.Name, validNameRE.String())
	}
	mode := strings.TrimSpace(i.Mode)
	if !validMode(mode) {
		return SelectedMode{}, fmt.Errorf("instance %q has unsupported mode %q", i.Name, mode)
	}
	i.Mode = mode
	if err := i.validateDeclaration(); err != nil {
		return SelectedMode{}, fmt.Errorf("instance %q: %w", i.Name, err)
	}
	return SelectedMode{Name: name, Enabled: i.Enabled, Mode: mode, Options: i.Options}, nil
}

func validMode(mode string) bool {
	switch mode {
	case ModeFile, ModeGit, ModeOCI, ModeNPM, ModeGo, ModeMaven, ModeCargo,
		ModePyPI, ModeFlatpak, ModeAPK, ModeDEB, ModeRPM, ModePacman:
		return true
	default:
		return false
	}
}

func (i Instance) validateDeclaration() error {
	if (strings.TrimSpace(i.Path) == "") == (strings.TrimSpace(i.Bind) == "") {
		return errors.New("must define exactly one of path or bind")
	}
	if i.Mode == ModeOCI && strings.TrimSpace(i.Bind) == "" {
		return errors.New("oci mode requires bind")
	}
	if i.Mode != ModeOCI && strings.TrimSpace(i.Path) == "" {
		return fmt.Errorf("%s mode requires path", i.Mode)
	}
	if strings.TrimSpace(i.Upstream) == "" {
		return errors.New("upstream is required")
	}
	if err := ValidateHTTPUpstream(i.Upstream); err != nil {
		return fmt.Errorf("upstream: %w", err)
	}
	if err := ValidateTransport(i.Transport); err != nil {
		return fmt.Errorf("transport: %w", err)
	}

	if i.Options != nil && i.Options.Node != nil {
		if i.Options.Node.Kind != yaml.MappingNode {
			return errors.New("options must be a mapping")
		}
		var options map[string]any
		data, err := yaml.Marshal(i.Options.Node)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(data, &options); err != nil {
			return fmt.Errorf("options must be a mapping: %w", err)
		}
		var fields []string
		switch i.Mode {
		case ModeFile:
			fields = []string{"pass_headers", "rules"}
		case ModeGit:
			fields = []string{"auth", "sync_interval", "operation_timeout"}
		case ModeOCI:
			fields = []string{"auth"}
		case ModeGo:
			fields = []string{"sumdb", "goprivate", "disable_module_fetch_header"}
		}
		allowed := make(map[string]struct{}, len(fields))
		for _, field := range fields {
			allowed[field] = struct{}{}
		}
		for key := range options {
			if _, ok := allowed[key]; !ok {
				return fmt.Errorf("options field %q is not supported by %s mode", key, i.Mode)
			}
		}
	}
	return nil
}

func LoadFile(path string) (*Document, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }() // read-only os.File; close error is harmless
	return Decode(file)
}

func Decode(r io.Reader) (*Document, error) {
	if r == nil {
		return nil, errors.New("configuration reader is nil")
	}
	var doc Document
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("configuration must contain exactly one YAML document")
		}
		return nil, err
	}
	for _, inst := range doc.Instances {
		if !ValidInstanceName(strings.TrimSpace(inst.Name)) {
			return nil, fmt.Errorf("invalid instance name %q: must match %s", inst.Name, validNameRE.String())
		}
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

type ByteSize int64

func (b ByteSize) Bytes() int64 { return int64(b) }

func (b ByteSize) String() string {
	value := int64(b)
	if value == 0 {
		return "0B"
	}
	for _, unit := range []struct {
		name string
		size int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if value%unit.size == 0 {
			return strconv.FormatInt(value/unit.size, 10) + unit.name
		}
	}
	return strconv.FormatInt(value, 10) + "B"
}

func (b ByteSize) MarshalYAML() (any, error) {
	if b <= 0 {
		return nil, errors.New("byte size must be positive")
	}
	return b.String(), nil
}

func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		*b = 0
		return nil
	}
	if value.Kind != yaml.ScalarNode || strings.TrimSpace(value.Value) == "" {
		return errors.New("byte size must be a non-empty scalar")
	}
	raw := strings.TrimSpace(value.Value)
	units := []struct {
		suffix     string
		multiplier int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}
	for _, unit := range units {
		suffix, multiplier := unit.suffix, unit.multiplier
		if !strings.HasSuffix(raw, suffix) {
			continue
		}
		number := strings.TrimSuffix(raw, suffix)
		parsed, err := strconv.ParseInt(number, 10, 64)
		if err != nil || parsed <= 0 || parsed > int64(^uint64(0)>>1)/multiplier {
			return fmt.Errorf("invalid byte size %q", value.Value)
		}
		*b = ByteSize(parsed * multiplier)
		return nil
	}
	return fmt.Errorf("invalid byte size %q: use B, KiB, MiB, GiB, or TiB", value.Value)
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
	switch value.Value {
	case "never", "0", "none", "infinite":
		*e = ExpirationNever
		return nil
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid expiration %q: %w", value.Value, err)
	}
	if parsed < 0 {
		return fmt.Errorf("expiration must not be negative: %q", value.Value)
	}
	*e = Expiration(parsed)
	return nil
}
