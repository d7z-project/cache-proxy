package pacman

import "testing"

func FuzzPacmanDatabaseClassification(f *testing.F) {
	for _, seed := range []string{"core/os/x86_64/core.db", "core/os/x86_64/core.db.tar.zst", "extra.files.sig", "pkg.tar.zst"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		index := isIndexRequest(value)
		anchor := isDatabaseAnchor(value)
		if anchor && !index {
			t.Fatal("database anchor must be an index request")
		}
	})
}
