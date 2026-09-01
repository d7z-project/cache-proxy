package pacman

import (
	"path"
	"strings"
	"testing"
)

func FuzzMetadataName(f *testing.F) {
	f.Add("core.db.tar.zst")
	f.Add("package.pkg.tar.zst.sig")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}
		name := path.Base(input)
		for _, marker := range []string{".db", ".files"} {
			_ = strings.Contains(name, marker) && !strings.Contains(name, ".pkg.tar.")
		}
	})
}
