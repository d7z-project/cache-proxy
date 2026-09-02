package transport

import (
	"net/url"
	"testing"
)

func FuzzJoinURL(f *testing.F) {
	f.Add("scope/%40name", "a=1")
	f.Add("dists/trixie/InRelease", "")
	base, _ := url.Parse("https://example.test/root")
	f.Fuzz(func(t *testing.T, escapedPath, query string) {
		if len(escapedPath) > 4096 || len(query) > 4096 {
			t.Skip()
		}
		result, err := JoinURL(base, escapedPath, query)
		if err == nil && (result.Scheme != base.Scheme || result.Host != base.Host || result.RawQuery != query) {
			t.Fatalf("join escaped origin: %s", result)
		}
	})
}
