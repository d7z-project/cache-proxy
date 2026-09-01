package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDecodeCommonInstance(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
cache:
  metadata_ttl: 2m
  retention: 720h
instances:
  - name: packages
    enabled: true
    mode: npm
    path: /npm
    upstreams:
      - https://registry.npmjs.org
    transport:
      header_timeout: 30s
    options: {}
`))
	require.NoError(t, err)
	require.Equal(t, Duration(2*time.Minute), doc.Cache.MetadataTTL)
	require.Equal(t, Expiration(720*time.Hour), doc.Cache.Retention)
	require.Len(t, doc.Instances, 1)
	instance := doc.Instances[0]
	require.Equal(t, ModeNPM, instance.Mode)
	require.Equal(t, "/npm", instance.Path)
	require.Equal(t, []string{"https://registry.npmjs.org"}, instance.Upstreams)

	instance.MetadataTTL = doc.Cache.MetadataTTL
	instance.Retention = doc.Cache.Retention
	selected, err := instance.SelectMode()
	require.NoError(t, err)
	require.Equal(t, ModeNPM, selected.Mode)
	var options struct{}
	require.NoError(t, selected.Options.DecodeStrict(&options))
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	_, err := Decode(strings.NewReader(`
instances:
  - name: packages
    enabled: true
    mode: npm
    path: /npm
    upstreams: [https://registry.npmjs.org]
    mystery: true
`))
	require.ErrorContains(t, err, "field mystery not found")
}

func TestSelectModeValidatesCommonDeclaration(t *testing.T) {
	tests := []struct {
		name     string
		instance Instance
		contains string
	}{
		{name: "unknown mode", instance: Instance{Name: "x", Mode: "unknown", Path: "/x", Upstreams: []string{"https://example.test"}}, contains: "unsupported mode"},
		{name: "missing bind", instance: Instance{Name: "x", Mode: ModeNPM, Upstreams: []string{"https://example.test"}}, contains: "exactly one of path or bind"},
		{name: "both binds", instance: Instance{Name: "x", Mode: ModeOCI, Path: "/x", Bind: ":5000", Upstreams: []string{"https://example.test"}}, contains: "exactly one of path or bind"},
		{name: "no upstream", instance: Instance{Name: "x", Mode: ModeNPM, Path: "/x"}, contains: "at least one"},
		{name: "spaced mode still enforces origin count", instance: Instance{Name: "x", Mode: " npm ", Path: "/x", Upstreams: []string{"https://one.test", "https://two.test"}}, contains: "exactly one upstream"},
		{name: "bad upstream", instance: Instance{Name: "x", Mode: ModeNPM, Path: "/x", Upstreams: []string{"file:///tmp"}}, contains: "valid absolute URL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.instance.SelectMode()
			require.ErrorContains(t, err, test.contains)
		})
	}
}

func TestOptionsCannotOverrideCommonFields(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
instances:
  - name: packages
    enabled: true
    mode: npm
    path: /npm
    upstreams: [https://registry.npmjs.org]
    options:
      upstream: https://attacker.invalid
`))
	require.NoError(t, err)
	_, err = doc.Instances[0].SelectMode()
	require.ErrorContains(t, err, `options field "upstream" is not supported`)
}

func TestDurationAndExpiration(t *testing.T) {
	var duration Duration
	require.NoError(t, duration.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "3m"}))
	require.Equal(t, 3*time.Minute, duration.Duration())
	var expiration Expiration
	require.NoError(t, expiration.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "never"}))
	require.True(t, expiration.IsNever())
}
