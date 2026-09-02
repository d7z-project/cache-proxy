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
	for _, seed := range []string{"/dists/trixie/InRelease", "/github.com/!owner/module/@v/list", "/a%20b", "/a%2fb", "/a/../b"} {
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
		if strings.Contains(cleaned, "\\") || strings.ContainsRune(cleaned, '\x00') || canonicalEscapedPath("/"+cleaned) != target.EscapedPath() {
			t.Fatalf("accepted path did not round-trip: %q -> %q", raw, cleaned)
		}
	})
}
