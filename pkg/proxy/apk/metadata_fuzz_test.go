package apk

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func FuzzParseIndex(f *testing.F) {
	f.Add("P:busybox\nV:1.36.1-r0\n\n")
	f.Add("P:../../escape\nV:1.0\n\n")
	f.Add("P:package\nV:\n\n")

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		paths := &filerepo.PathIndexBuilder{}
		_, err := parseIndex("v3.20/main/x86_64", strings.NewReader(input), paths)
		if err == nil {
			for _, item := range paths.Finalize() {
				require.True(t, httpcache.SafePath(item))
			}
		}
	})
}
