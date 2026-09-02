package pypi

import (
	"net/url"
	"testing"
)

func FuzzPyPIPathAndHash(f *testing.F) {
	f.Add("simple/zope-interface/", "sha256=abcdef")
	f.Add("simple/", "sha512=00")
	f.Fuzz(func(t *testing.T, path, fragment string) {
		if len(path) > 4096 || len(fragment) > 4096 {
			t.Skip()
		}
		_, _ = simpleProject(path)
		_, _ = hashFromFragment(fragment)
		if target, err := url.ParseRequestURI(path); err == nil {
			_, _ = pypiRequestPath(target)
		}
	})
}
