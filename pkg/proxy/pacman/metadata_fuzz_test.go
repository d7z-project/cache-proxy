package pacman

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzParseDesc(f *testing.F) {
	f.Add("%FILENAME%\npackage-1.0-1-x86_64.pkg.tar.zst\n")
	f.Add("%NAME%\npackage\n%FILENAME%\n")
	f.Add("%FILENAME%\n../../escape\n")

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 128<<10 {
			t.Skip()
		}
		filename, err := parseDesc(strings.NewReader(input))
		if err == nil {
			require.LessOrEqual(t, len(filename), len(input))
		}
	})
}
