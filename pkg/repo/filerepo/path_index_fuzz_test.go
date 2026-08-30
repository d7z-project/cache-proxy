package filerepo

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func FuzzPathIndexBuilder(f *testing.F) {
	f.Add("pool/main/p/pkg.deb\npool/main/p/pkg.deb.sig")
	f.Add("../../escape\n/absolute\nvalid/path")
	f.Add("duplicate\nduplicate\n./duplicate")

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 128<<10 {
			t.Skip()
		}
		var builder PathIndexBuilder
		for item := range strings.FieldsSeq(input) {
			builder.Add(item)
		}
		paths := builder.Finalize()
		require.True(t, slices.IsSorted(paths))
		for index, item := range paths {
			require.True(t, httpcache.SafePath(item))
			if index > 0 {
				require.NotEqual(t, paths[index-1], item)
			}
		}
	})
}
