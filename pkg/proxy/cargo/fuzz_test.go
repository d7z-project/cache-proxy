package cargo

import (
	"net/http"
	"testing"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func FuzzCrateDownloadRoute(f *testing.F) {
	token, _, _ := httpcache.EncodeCargoDownloadTemplate("https://cdn.example/{lowerprefix}/{crate}/{version}.crate")
	f.Add("example", "1.0.0", token, "")
	f.Add("crate_name", "2.0.0+meta", "invalid!", "")
	f.Fuzz(func(t *testing.T, crate, version, routeToken, checksum string) {
		if len(crate) > 512 || len(version) > 512 || len(routeToken) > 16<<10 || len(checksum) > 256 {
			t.Skip()
		}
		requestPath := "/api/v1/crates/" + crate + "/" + version + "/download/" + routeToken
		if checksum != "" {
			requestPath += "/" + checksum
		}
		req, err := http.NewRequest(http.MethodGet, requestPath, nil)
		if err != nil {
			return
		}
		route, err := newResolver(&Policy{}).Resolve(req)
		if err == nil && route.TargetURL == "" {
			t.Fatal("resolved crate route has no target URL")
		}
	})
}
