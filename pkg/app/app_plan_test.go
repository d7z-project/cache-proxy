package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestUpstreamGateConfigAppliesExactHostOverrides(t *testing.T) {
	download := config.DownloadConfig{
		MaxActive:        256,
		MaxActivePerHost: 16,
		MinInterval:      config.Duration(5 * time.Millisecond),
		Hosts: map[string]config.DownloadHostConfig{
			"Packages.D7Z.NET": {MaxActive: 32, MinInterval: config.Duration(20 * time.Millisecond)},
			"registry.example": {MaxActive: 8},
		},
	}

	gateConfig := upstreamGateConfig(download)
	require.Equal(t, 32, gateConfig.Hosts["packages.d7z.net"].MaxActive)
	require.Equal(t, 5*time.Millisecond, gateConfig.MinInterval)
	require.Equal(t, 20*time.Millisecond, gateConfig.Hosts["packages.d7z.net"].MinInterval)
	require.Equal(t, 8, gateConfig.Hosts["registry.example"].MaxActive)
}

func TestValidateRejectsNilDocument(t *testing.T) {
	require.ErrorContains(t, Validate(nil), "config document is nil")
}

func TestDisabledInstanceDoesNotRegisterRuntimeTasks(t *testing.T) {
	doc, err := config.Decode(strings.NewReader(`
server:
  backend: ` + t.TempDir() + `
instances:
  - name: disabled-cargo
    enabled: false
    cargo:
      upstream: https://index.example
`))
	require.NoError(t, err)

	application, err := Open(context.Background(), doc, "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, application.Close(context.Background())) })

	entry := application.entries["disabled-cargo"]
	require.NotNil(t, entry)
	require.Nil(t, entry.Runtime)
	for _, task := range application.scheduler.Snapshot() {
		require.NotEqual(t, "disabled-cargo", task.Key.Instance())
	}
}

func TestValidateServerConfigRejectsInvalidDownloadSettings(t *testing.T) {
	tests := map[string]func(*config.Document){
		"negative global capacity": func(doc *config.Document) {
			doc.Storage.Download.MaxActive = -1
		},
		"negative global interval": func(doc *config.Document) {
			doc.Storage.Download.MinInterval = -1
		},
		"negative host interval": func(doc *config.Document) {
			doc.Storage.Download.Hosts = map[string]config.DownloadHostConfig{"example.com": {MinInterval: -1}}
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

func TestValidateServerConfigRejectsInvalidPublicURL(t *testing.T) {
	for _, publicURL := range []string{"cache.example", "file:///tmp/cache", "https://cache.example/base?tenant=a", "https://cache.example/#status"} {
		doc := testDocument(t.TempDir(), nil)
		doc.Server.PublicURL = publicURL
		normalizeDocument(doc)
		require.Error(t, validateServerConfig(doc), publicURL)
	}
}

func TestPlanRejectsRoutesOverlappingStatusAPI(t *testing.T) {
	for _, route := range []string{"/-", "/-/status", "/-/status/custom"} {
		doc, err := config.Decode(strings.NewReader(`
instances:
  - name: files
    enabled: true
    file:
      route: { path: ` + route + ` }
      upstreams: [https://files.example]
`))
		require.NoError(t, err)
		require.ErrorContains(t, Validate(doc), "conflicts with status API", route)
	}
}
