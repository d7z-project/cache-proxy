package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestByteSizeStrictParsingAndOverflow(t *testing.T) {
	for input, expected := range map[string]int64{"1B": 1, "2KiB": 2 << 10, "3MiB": 3 << 20, "4GiB": 4 << 30, "5TiB": 5 << 40} {
		var value ByteSize
		require.NoError(t, yaml.Unmarshal([]byte(input), &value), input)
		require.Equal(t, expected, value.Bytes(), input)
	}
	for _, input := range []string{"0B", "-1B", "1KB", "1.5GiB", "9223372036854775807TiB", ""} {
		var value ByteSize
		err := yaml.Unmarshal([]byte(input), &value)
		if input == "" {
			require.NoError(t, err)
			require.Zero(t, value)
		} else {
			require.Error(t, err, input)
		}
	}
	var maximum ByteSize
	require.NoError(t, yaml.Unmarshal([]byte("9223372036854775807B"), &maximum))
	require.Equal(t, int64(^uint64(0)>>1), maximum.Bytes())
}

func TestDecodeCommonInstance(t *testing.T) {
	doc, err := Decode(strings.NewReader(`
instances:
  - name: packages
    enabled: true
    mode: npm
    path: /npm
    upstream: https://registry.npmjs.org
    transport:
      header_timeout: 30s
    options: {}
`))
	require.NoError(t, err)
	require.Len(t, doc.Instances, 1)
	instance := doc.Instances[0]
	require.Equal(t, ModeNPM, instance.Mode)
	require.Equal(t, "/npm", instance.Path)
	require.Equal(t, "https://registry.npmjs.org", instance.Upstream)

	selected, err := instance.SelectMode()
	require.NoError(t, err)
	require.Equal(t, ModeNPM, selected.Mode)
	var options struct{}
	require.NoError(t, selected.Options.DecodeStrict(&options))
}

func TestInstanceRefreshInterval(t *testing.T) {
	for _, interval := range []string{"0s", "-1s", "999ms", "1s", "30m"} {
		doc, err := Decode(strings.NewReader("instances:\n  - name: test\n    mode: deb\n    path: /deb\n    upstream: https://example.test\n    refresh:\n      interval: " + interval + "\n"))
		require.NoError(t, err)
		_, err = doc.Instances[0].SelectMode()
		if interval == "1s" || interval == "30m" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "at least 1s")
		}
	}
	_, err := Decode(strings.NewReader("instances:\n  - refresh:\n      unexpected: 1s\n"))
	require.Error(t, err)
	instance := Instance{Name: "git", Mode: ModeGit, Path: "/git", Upstream: "https://example.test", Refresh: &RefreshConfig{Interval: Duration(time.Minute)}}
	_, err = instance.SelectMode()
	require.ErrorContains(t, err, "sync_interval")
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	_, err := Decode(strings.NewReader(`
instances:
  - name: packages
    enabled: true
    mode: npm
    path: /npm
    upstream: https://registry.npmjs.org
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
		{name: "unknown mode", instance: Instance{Name: "x", Mode: "unknown", Path: "/x", Upstream: "https://example.test"}, contains: "unsupported mode"},
		{name: "missing bind", instance: Instance{Name: "x", Mode: ModeNPM, Upstream: "https://example.test"}, contains: "exactly one of path or bind"},
		{name: "both binds", instance: Instance{Name: "x", Mode: ModeOCI, Path: "/x", Bind: ":5000", Upstream: "https://example.test"}, contains: "exactly one of path or bind"},
		{name: "no upstream", instance: Instance{Name: "x", Mode: ModeNPM, Path: "/x"}, contains: "upstream is required"},
		{name: "bad upstream", instance: Instance{Name: "x", Mode: ModeNPM, Path: "/x", Upstream: "file:///tmp"}, contains: "valid absolute URL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.instance.SelectMode()
			require.ErrorContains(t, err, test.contains)
		})
	}
}

func TestDurationAndExpiration(t *testing.T) {
	var duration Duration
	require.NoError(t, duration.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "3m"}))
	require.Equal(t, 3*time.Minute, duration.Duration())
	var expiration Expiration
	require.NoError(t, expiration.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "never"}))
	require.True(t, expiration.IsNever())
	var size ByteSize
	require.NoError(t, size.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "2GiB"}))
	require.Equal(t, int64(2<<30), size.Bytes())
	for _, node := range []*yaml.Node{
		{Kind: yaml.MappingNode},
		{Kind: yaml.SequenceNode},
		{Kind: yaml.ScalarNode, Value: ""},
		{Kind: yaml.ScalarNode, Value: "9223372036854775807GiB"},
	} {
		require.Error(t, size.UnmarshalYAML(node))
	}
	_, err := ByteSize(0).MarshalYAML()
	require.Error(t, err)
}
