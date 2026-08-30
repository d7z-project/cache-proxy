package rpm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func FuzzRPMMetadataParsers(f *testing.F) {
	f.Add(uint8(0), `<repomd><data type="primary"><checksum type="sha256">abc</checksum><location href="repodata/primary.xml"/></data></repomd>`)
	f.Add(uint8(1), `<metadata><package><location href="Packages/p/pkg.rpm"/><checksum>abc</checksum></package></metadata>`)
	f.Add(uint8(0), `<repomd><data><location href="../../escape"/>`)

	f.Fuzz(func(t *testing.T, kind uint8, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		if kind%2 == 0 {
			items, err := parseRepomdReader(strings.NewReader(input))
			if err == nil {
				require.LessOrEqual(t, len(items), len(input)+1)
			}
			return
		}
		paths := &filerepo.PathIndexBuilder{}
		_, err := parsePrimary(strings.NewReader(input), paths, "repo")
		if err == nil {
			for _, item := range paths.Finalize() {
				require.True(t, httpcache.SafePath(item))
			}
		}
	})
}
