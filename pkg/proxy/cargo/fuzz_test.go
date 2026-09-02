package cargo

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzCargoSparseIndex(f *testing.F) {
	f.Add("3/f/foo", "foo", `{"name":"foo","vers":"1.0.0","cksum":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}`+"\n")
	f.Add("ab/cd/abcd", "abcd", "{}\n")
	f.Fuzz(func(t *testing.T, indexPath, crateName, body string) {
		if len(indexPath) > 4096 || len(crateName) > 1024 || len(body) > 64<<10 {
			t.Skip()
		}
		_, _ = parseSparseIndex(strings.NewReader(body), indexPath)
		_, _ = cargoDownloadURL("https://example.test/{prefix}/{crate}/{version}/{sha256-checksum}", crateName, "1.0.0", strings.Repeat("0", 64))
	})
}

func FuzzCargoGitReadRequest(f *testing.F) {
	f.Add(http.MethodGet, "info/refs", "service=git-upload-pack")
	f.Add(http.MethodPost, "git-receive-pack", "")
	f.Fuzz(func(t *testing.T, method, cleaned, rawQuery string) {
		if len(method) > 32 || len(cleaned) > 4096 || len(rawQuery) > 4096 {
			t.Skip()
		}
		request := &http.Request{Method: method, URL: &url.URL{RawQuery: rawQuery}, Header: make(http.Header)}
		if isCargoGitReadRequest(cleaned, request) || isCargoGitUploadPack(method, cleaned) {
			require.Contains(t, []string{http.MethodGet, http.MethodHead, http.MethodPost}, method)
			if method == http.MethodPost {
				require.Equal(t, "git-upload-pack", cleaned)
			}
		}
	})
}
