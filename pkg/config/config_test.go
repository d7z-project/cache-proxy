package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestExpirationYAML(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Expiration
		wantErr bool
	}{
		{name: "unset", input: "", want: 0},
		{name: "null", input: "null", want: 0},
		{name: "never", input: "never", want: ExpirationNever},
		{name: "zero", input: "0", want: ExpirationNever},
		{name: "duration", input: "720h", want: Expiration(720 * time.Hour)},
		{name: "negative", input: "-1h", wantErr: true},
		{name: "invalid", input: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Expiration
			err := yaml.Unmarshal([]byte(tt.input), &got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFreshnessYAML(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Freshness
		wantErr bool
	}{
		{name: "unset", input: "", want: 0},
		{name: "forever", input: "forever", want: FreshnessForever},
		{name: "zero", input: "0", want: FreshnessForever},
		{name: "duration", input: "5m", want: Freshness(5 * time.Minute)},
		{name: "negative", input: "-1m", wantErr: true},
		{name: "invalid", input: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Freshness
			err := yaml.Unmarshal([]byte(tt.input), &got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodeDocument(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
server:
  bind: 127.0.0.1:8080
  backend: /tmp/cache
metrics:
  path: /metrics
  token: secret
storage:
  gc:
    blob: 24h
  cleanup:
    enabled: true
    interval: 6h
    dry_run: true
    batch_size: 100
    workers: 4
instances:
  - name: files
    enabled: true
    file:
      expire_after: 720h
      route:
        path: /files
      upstreams:
        - https://example.com
      default_policy: immutable
      rules: []
`))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:8080", doc.Server.Bind)
	require.Equal(t, "/tmp/cache", doc.Server.Backend)
	require.Equal(t, "/metrics", doc.Metrics.Path)
	require.Equal(t, "secret", doc.Metrics.Token)
	require.Len(t, doc.Instances, 1)
	spec, err := doc.Instances[0].SelectMode()
	require.NoError(t, err)
	require.Equal(t, ModeFile, spec.Mode)
	require.True(t, spec.Enabled)
	require.NotNil(t, spec.Block)

	var cfg struct {
		ExpireAfter string `yaml:"expire_after"`
		Route       struct {
			Path string `yaml:"path"`
		} `yaml:"route"`
		Upstreams     []string `yaml:"upstreams"`
		DefaultPolicy string   `yaml:"default_policy"`
		Rules         []any    `yaml:"rules"`
	}
	require.NoError(t, spec.Block.DecodeStrict(&cfg))
	require.Equal(t, "720h", cfg.ExpireAfter)
	require.Equal(t, "/files", cfg.Route.Path)
	require.Equal(t, []string{"https://example.com"}, cfg.Upstreams)
	require.Equal(t, "immutable", cfg.DefaultPolicy)
}
