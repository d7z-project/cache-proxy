package httpcache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzMetadataRewriters(f *testing.F) {
	f.Add(uint8(0), []byte(`{"name":"pkg","dist":{"tarball":"https://registry.example/pkg.tgz"}}`))
	f.Add(uint8(1), []byte(`{"files":[{"url":"../packages/pkg.whl"}]}`))
	f.Add(uint8(2), []byte(`{"dl":"https://static.example"}`))
	f.Add(uint8(0), []byte(`{"nested":[[[null]]]}`))

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if len(data) > 128<<10 {
			t.Skip()
		}
		req := httptest.NewRequest(http.MethodGet, "http://cache.example/proxy/config.json", nil)
		req.Header.Set("X-Cache-Proxy-Prefix", "/proxy")
		switch kind % 3 {
		case 0:
			var output bytes.Buffer
			err := RewriteNPMMetadata(bytes.NewReader(data), &output, []string{"https://registry.example"}, "https://cache.example/npm")
			if err == nil {
				require.True(t, json.Valid(output.Bytes()))
			}
		case 1:
			output, err := rewritePyPISimpleJSON(req, "https://pypi.example/simple/pkg/", data)
			if err == nil {
				require.True(t, json.Valid(output))
			}
		case 2:
			output, err := rewriteCargoConfig(req, data, kind&0x80 != 0)
			if err == nil {
				require.True(t, json.Valid(output))
			}
		}
	})
}
