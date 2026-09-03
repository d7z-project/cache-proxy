package storeio

import (
	"net/url"
	"strings"
	"testing"
)

func FuzzCleanRelative(f *testing.F) {
	for _, seed := range []string{"dists/trixie/InRelease", "/simple/pkg/", "a/../b", "a//b", "a\\b", "a\x00b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		cleaned, err := CleanRelative(value)
		if err == nil {
			if cleaned != value {
				t.Fatalf("path was normalized: input %q output %q", value, cleaned)
			}
			if cleaned == "" || cleaned[0] == '/' {
				t.Fatalf("invalid successful result %q", cleaned)
			}
			if second, secondErr := CleanRelative(cleaned); secondErr != nil || second != cleaned {
				t.Fatalf("result is not stable: %q, %v", second, secondErr)
			}
		}
	})
}

func FuzzCleanURLPath(f *testing.F) {
	for _, seed := range []string{
		"/dists/trixie/InRelease",
		"/github.com/!owner/module/@v/list",
		"/pkg_1%2bpve.deb",
		"/pkg_1%2Bpve.deb",
		"/a%41",
		"/a%2eb",
		"/a%20b",
		"/a%2fb",
		"/a/%2e%2e/b",
		"/",
		"/repository/",
	} {
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
		cleaned, err := CleanURLPath(target)
		if err != nil {
			return
		}
		if strings.Contains(cleaned, "\\") || strings.ContainsRune(cleaned, '\x00') || strings.HasPrefix(cleaned, "/") {
			t.Fatalf("accepted unsafe path: %q -> %q", raw, cleaned)
		}
		roundTrip := &url.URL{Path: "/" + cleaned}
		second, secondErr := CleanURLPath(roundTrip)
		if secondErr != nil || second != cleaned {
			t.Fatalf("accepted path is not semantically stable: %q -> %q -> %q, %v", raw, cleaned, second, secondErr)
		}
	})
}
