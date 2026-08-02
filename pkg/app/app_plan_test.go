package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestUpstreamGateConfigAppliesExactHostOverrides(t *testing.T) {
	disabled := config.Duration(0)
	download := config.DownloadConfig{
		MaxActive:              256,
		MaxActivePerHost:       4,
		RequestIntervalPerHost: config.Duration(125 * time.Millisecond),
		ForegroundQueueWait:    config.Duration(3 * time.Second),
		Hosts: map[string]config.DownloadHostConfig{
			"Packages.D7Z.NET": {MaxActive: 32, RequestInterval: &disabled},
			"registry.example": {MaxActive: 8},
		},
	}

	gateConfig := upstreamGateConfig(download)
	require.Equal(t, 32, gateConfig.Hosts["packages.d7z.net"].MaxActive)
	require.Zero(t, gateConfig.Hosts["packages.d7z.net"].RequestInterval)
	require.Equal(t, 125*time.Millisecond, gateConfig.Hosts["registry.example"].RequestInterval)
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
