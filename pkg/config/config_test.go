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
	}
	require.NoError(t, spec.Block.DecodeStrict(&cfg))
	require.Equal(t, "720h", cfg.ExpireAfter)
	require.Equal(t, "/files", cfg.Route.Path)
	require.Equal(t, []string{"https://example.com"}, cfg.Upstreams)
	require.Equal(t, "immutable", cfg.DefaultPolicy)
}

func TestDecodePackageRepositoryConfig(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
instances:
  - name: linux
    enabled: true
    deb:
      expire_after: 720h
      route:
        path: /deb
      repositories:
        - url: https://deb.example.com/debian
          suites: [bookworm]
          components: [main]
          architectures: [amd64]
      refresh_interval: 1h
      refresh_timeout: 2m
      metadata_policy: revalidate
      artifact_policy: immutable
`))
	require.NoError(t, err)
	selected, err := doc.Instances[0].SelectMode()
	require.NoError(t, err)
	var block struct {
		ExpireAfter Expiration `yaml:"expire_after"`
		Route       struct {
			Path string `yaml:"path"`
		} `yaml:"route"`
		Repositories []struct {
			URL           string   `yaml:"url"`
			Suites        []string `yaml:"suites"`
			Components    []string `yaml:"components"`
			Architectures []string `yaml:"architectures"`
		} `yaml:"repositories"`
		RefreshInterval Duration `yaml:"refresh_interval"`
		RefreshTimeout  Duration `yaml:"refresh_timeout"`
		MetadataPolicy  string   `yaml:"metadata_policy"`
		ArtifactPolicy  string   `yaml:"artifact_policy"`
	}
	require.NoError(t, selected.Block.DecodeStrict(&block))
	require.Equal(t, Duration(time.Hour), block.RefreshInterval)
	require.Equal(t, Duration(2*time.Minute), block.RefreshTimeout)
	require.Len(t, block.Repositories, 1)
	require.Equal(t, "https://deb.example.com/debian", block.Repositories[0].URL)
	require.Equal(t, []string{"bookworm"}, block.Repositories[0].Suites)
}

func TestDecodePackageRepositoryConfigRejectsRules(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
instances:
  - name: linux
    enabled: true
    deb:
      expire_after: 720h
      route:
        path: /deb
      repositories:
        - url: https://deb.example.com/debian
          suites: [bookworm]
          components: [main]
          architectures: [amd64]
      rules: []
`))
	require.NoError(t, err)
	selected, err := doc.Instances[0].SelectMode()
	require.NoError(t, err)
	var block struct {
		ExpireAfter Expiration `yaml:"expire_after"`
		Route       struct {
			Path string `yaml:"path"`
		} `yaml:"route"`
		Repositories []struct {
			URL           string   `yaml:"url"`
			Suites        []string `yaml:"suites"`
			Components    []string `yaml:"components"`
			Architectures []string `yaml:"architectures"`
		} `yaml:"repositories"`
	}
	err = selected.Block.DecodeStrict(&block)
	require.Error(t, err)
	require.ErrorContains(t, err, "field rules not found")
}

func TestDecodeGoProxyConfig(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
instances:
  - name: golang
    enabled: true
    go:
      expire_after: 720h
      route:
        path: /go
      proxies:
        - https://proxy.golang.org
      module_policy: revalidate
      zip_policy: immutable
`))
	require.NoError(t, err)
	selected, err := doc.Instances[0].SelectMode()
	require.NoError(t, err)
	var block struct {
		ExpireAfter Expiration `yaml:"expire_after"`
		Route       struct {
			Path string `yaml:"path"`
		} `yaml:"route"`
		Proxies      []string `yaml:"proxies"`
		ModulePolicy string   `yaml:"module_policy"`
		ZipPolicy    string   `yaml:"zip_policy"`
	}
	require.NoError(t, selected.Block.DecodeStrict(&block))
	require.Equal(t, []string{"https://proxy.golang.org"}, block.Proxies)
	require.Equal(t, "revalidate", block.ModulePolicy)
	require.Equal(t, "immutable", block.ZipPolicy)
}

func TestDecodePyPIConfig(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
instances:
  - name: python
    enabled: true
    pypi:
      expire_after: 720h
      route:
        path: /pypi
      upstream: https://pypi.org
      index_policy: revalidate
      file_policy: immutable
      proxy_json: false
`))
	require.NoError(t, err)
	selected, err := doc.Instances[0].SelectMode()
	require.NoError(t, err)
	var block struct {
		ExpireAfter Expiration `yaml:"expire_after"`
		Route       struct {
			Path string `yaml:"path"`
		} `yaml:"route"`
		Upstream    string `yaml:"upstream"`
		IndexPolicy string `yaml:"index_policy"`
		FilePolicy  string `yaml:"file_policy"`
		ProxyJSON   *bool  `yaml:"proxy_json"`
	}
	require.NoError(t, selected.Block.DecodeStrict(&block))
	require.Equal(t, "https://pypi.org", block.Upstream)
	require.Equal(t, "revalidate", block.IndexPolicy)
	require.Equal(t, "immutable", block.FilePolicy)
	require.NotNil(t, block.ProxyJSON)
	require.False(t, *block.ProxyJSON)
}

func TestDecodeSimplifiedProxyConfigsRejectLegacyFields(t *testing.T) {
	tests := []struct {
		name        string
		document    string
		decodeBlock any
		wantError   string
	}{
		{
			name: "cargo rejects upstreams",
			document: `
instances:
  - name: crates
    enabled: true
    cargo:
      expire_after: 720h
      route:
        path: /cargo
      upstreams:
        - https://index.crates.io
`,
			decodeBlock: &struct {
				ExpireAfter Expiration `yaml:"expire_after"`
				Route       struct {
					Path string `yaml:"path"`
				} `yaml:"route"`
				Upstream string `yaml:"upstream"`
			}{},
			wantError: "field upstreams not found",
		},
		{
			name: "npm rejects unexpected rules field",
			document: `
instances:
  - name: npmjs
    enabled: true
    npm:
      expire_after: 720h
      route:
        path: /npm
      upstream: https://registry.npmjs.org
      unexpected_rules: []
`,
			decodeBlock: &struct {
				ExpireAfter Expiration `yaml:"expire_after"`
				Route       struct {
					Path string `yaml:"path"`
				} `yaml:"route"`
				Upstream string `yaml:"upstream"`
			}{},
			wantError: "field unexpected_rules not found",
		},
		{
			name: "maven rejects upstreams",
			document: `
instances:
  - name: central
    enabled: true
    maven:
      expire_after: 720h
      route:
        path: /maven
      upstreams:
        - https://repo1.maven.org/maven2
`,
			decodeBlock: &struct {
				ExpireAfter Expiration `yaml:"expire_after"`
				Route       struct {
					Path string `yaml:"path"`
				} `yaml:"route"`
				Upstream string `yaml:"upstream"`
			}{},
			wantError: "field upstreams not found",
		},
		{
			name: "pypi rejects artifact policy",
			document: `
instances:
  - name: python
    enabled: true
    pypi:
      expire_after: 720h
      route:
        path: /pypi
      upstream: https://pypi.org
      artifact_policy: immutable
`,
			decodeBlock: &struct {
				ExpireAfter Expiration `yaml:"expire_after"`
				Route       struct {
					Path string `yaml:"path"`
				} `yaml:"route"`
				Upstream   string `yaml:"upstream"`
				FilePolicy string `yaml:"file_policy"`
			}{},
			wantError: "field artifact_policy not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := Decode(strings.NewReader(tt.document))
			require.NoError(t, err)
			selected, err := doc.Instances[0].SelectMode()
			require.NoError(t, err)
			err = selected.Block.DecodeStrict(tt.decodeBlock)
			require.Error(t, err)
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}
