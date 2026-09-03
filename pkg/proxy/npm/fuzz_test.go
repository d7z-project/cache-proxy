package npm

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzNPMRequestPath(f *testing.F) {
	for _, seed := range []string{"/", "/browse/", "/@scope/pkg", "/@scope%2fpkg", "/pkg", "/a/../b", "/a%2fb"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 4096 {
			t.Skip()
		}
		target, err := url.ParseRequestURI(raw)
		if err != nil {
			return
		}
		cleaned, err := npmRequestPath(target)
		if err == nil {
			_, _ = packageFromPath(cleaned)
		}
	})
}

func FuzzNPMAuditRequest(f *testing.F) {
	f.Add(http.MethodPost, "-/npm/v1/security/advisories/bulk")
	f.Add(http.MethodPut, "pkg")
	f.Fuzz(func(t *testing.T, method, cleaned string) {
		if len(method) > 32 || len(cleaned) > 4096 {
			t.Skip()
		}
		if isNPMAuditRequest(method, cleaned) {
			require.Equal(t, http.MethodPost, method)
			require.Contains(t, []string{"-/npm/v1/security/advisories/bulk", "-/npm/v1/security/audits/quick"}, cleaned)
		}
	})
}
