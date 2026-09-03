package pacman

import (
	"strings"
	"testing"
)

func FuzzPacmanDatabaseClassification(f *testing.F) {
	for _, seed := range []string{"core/os/x86_64/core.db", "core/os/x86_64/core.db.tar.zst", "extra.files.sig", "pkg.tar.zst"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		if isPacmanDatabasePath(value) && (value == "" || strings.HasSuffix(value, "/")) {
			t.Fatal("directory classified as a database")
		}
		if isPacmanArtifactPath(value) && (value == "" || strings.HasSuffix(value, "/")) {
			t.Fatal("directory classified as an artifact")
		}
	})
}
