package httpcache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteCargoConfigCarriesValidatedTemplate(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://cache.example/cargo/config.json", nil)
	output, err := rewriteCargoConfig(req, []byte(`{"dl":"https://cdn.example/{crate}/{sha256-checksum}.crate?source=index","api":"https://api.example"}`), true)
	require.NoError(t, err)
	var cfg map[string]any
	require.NoError(t, json.Unmarshal(output, &cfg))
	require.Equal(t, "https://api.example", cfg["api"])
	require.Equal(t, true, cfg["auth-required"])
	downloadURL := cfg["dl"].(string)
	require.True(t, strings.HasPrefix(downloadURL, "https://cache.example/cargo/api/v1/crates/{crate}/{version}/download/"))
	require.True(t, strings.HasSuffix(downloadURL, "/{sha256-checksum}"))
}

func TestRewriteCargoConfigLeavesUnsafeTemplateDirect(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://cache.example/cargo/config.json", nil)
	for _, downloadURL := range []string{
		"file:///tmp/crates/{crate}",
		"https://user:secret@cdn.example/{crate}",
		"https://{crate}.example/{version}",
		"https://cdn.example/{unsupported}",
	} {
		input, err := json.Marshal(map[string]string{"dl": downloadURL})
		require.NoError(t, err)
		output, err := rewriteCargoConfig(req, input, false)
		require.NoError(t, err)
		var cfg struct {
			DownloadURL string `json:"dl"`
		}
		require.NoError(t, json.Unmarshal(output, &cfg))
		require.Equal(t, downloadURL, cfg.DownloadURL)
	}
}

func TestCargoDownloadTemplateTokenIsBounded(t *testing.T) {
	_, _, err := EncodeCargoDownloadTemplate("https://cdn.example/" + strings.Repeat("a", maxCargoDownloadTemplateSize))
	require.Error(t, err)
	_, _, err = DecodeCargoDownloadTemplate(strings.Repeat("a", 16<<10))
	require.Error(t, err)
}
