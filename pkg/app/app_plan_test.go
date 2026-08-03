package app

import (
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestUpstreamGateConfigAppliesExactHostOverrides(t *testing.T) {
	download := config.DownloadConfig{
		MaxActive:        256,
		MaxActivePerHost: 16,
		Hosts: map[string]config.DownloadHostConfig{
			"Packages.D7Z.NET": {MaxActive: 32},
			"registry.example": {MaxActive: 8},
		},
	}

	gateConfig := upstreamGateConfig(download)
	require.Equal(t, 32, gateConfig.Hosts["packages.d7z.net"].MaxActive)
	require.Equal(t, 8, gateConfig.Hosts["registry.example"].MaxActive)
}

func TestValidateServerConfigRejectsInvalidDownloadSettings(t *testing.T) {
	tests := map[string]func(*config.Document){
		"negative global capacity": func(doc *config.Document) {
			doc.Storage.Download.MaxActive = -1
		},
		"host with scheme": func(doc *config.Document) {
			doc.Storage.Download.Hosts = map[string]config.DownloadHostConfig{"https://example.com": {MaxActive: 1}}
		},
		"duplicate normalized host": func(doc *config.Document) {
			doc.Storage.Download.Hosts = map[string]config.DownloadHostConfig{
				"EXAMPLE.com": {MaxActive: 1},
				"example.com": {MaxActive: 2},
			}
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			doc := testDocument(t.TempDir(), nil)
			mutate(doc)
			normalizeDocument(doc)
			require.Error(t, validateServerConfig(doc))
		})
	}
}
