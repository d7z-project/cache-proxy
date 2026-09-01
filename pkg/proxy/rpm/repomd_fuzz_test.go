package rpm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzRepomd(f *testing.F) {
	f.Add(`<repomd><data type="primary"><checksum type="sha256">abc</checksum><location href="repodata/primary.xml"/></data></repomd>`)
	f.Add(`<repomd><data><location href="../../escape"/>`)
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		items, err := parseRepomdReader(strings.NewReader(input))
		if err == nil {
			require.LessOrEqual(t, len(items), len(input)+1)
		}
	})
}

func FuzzRPMPathClassification(f *testing.F) {
	f.Add("repo/repodata/repomd.xml")
	f.Add("repo/Packages/a.rpm")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}
		_, _, _ = repomdAnchor(input)
		_ = classifyPath(input)
	})
}
