package git

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzGitReadRequest(f *testing.F) {
	f.Add(http.MethodGet, "/info/refs", "service=git-upload-pack", "version=2")
	f.Add(http.MethodPost, "/git-upload-pack", "", "")
	f.Add(http.MethodPost, "/git-receive-pack", "", "")
	f.Fuzz(func(t *testing.T, method, requestPath, rawQuery, protocol string) {
		if len(method) > 32 || len(requestPath) > 4096 || len(rawQuery) > 4096 || len(protocol) > 1024 {
			t.Skip()
		}
		request := &http.Request{Method: method, URL: &url.URL{Path: requestPath, RawQuery: rawQuery}, Header: http.Header{"Git-Protocol": {protocol}}}
		if isGitReadRequest(request) {
			require.Contains(t, []string{http.MethodGet, http.MethodHead, http.MethodPost}, method)
			if method == http.MethodPost {
				require.Equal(t, "/git-upload-pack", requestPath)
			}
		}
		_ = shouldProxyGitRead(request)
	})
}
