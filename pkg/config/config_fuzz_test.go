package config

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func FuzzConfigDecoders(f *testing.F) {
	f.Add(uint8(0), []byte("instances: []\n"))
	f.Add(uint8(0), []byte("instances:\n  - name: npm\n    enabled: true\n    npm:\n      upstream: https://registry.npmjs.org\n"))
	f.Add(uint8(0), []byte("instances: []\n---\ninstances: []\n"))
	f.Add(uint8(1), []byte("30s"))
	f.Add(uint8(2), []byte("never"))
	f.Add(uint8(3), []byte("forever"))

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		switch kind % 4 {
		case 0:
			doc, err := Decode(bytes.NewReader(data))
			if err != nil {
				return
			}
			require.NotNil(t, doc)
			for _, instance := range doc.Instances {
				selected, err := instance.SelectMode()
				if err != nil {
					continue
				}
				var block map[string]any
				_ = selected.Block.DecodeStrict(&block)
			}
		case 1:
			fuzzYAMLRoundTrip(t, data, new(Duration))
		case 2:
			fuzzYAMLRoundTrip(t, data, new(Expiration))
		case 3:
			fuzzYAMLRoundTrip(t, data, new(Freshness))
		}
	})
}

func fuzzYAMLRoundTrip(t *testing.T, data []byte, target any) {
	t.Helper()
	if yaml.Unmarshal(data, target) != nil {
		return
	}
	encoded, err := yaml.Marshal(target)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(encoded, target))
}
