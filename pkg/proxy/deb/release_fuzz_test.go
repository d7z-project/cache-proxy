package deb

import (
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func FuzzReleaseManifest(f *testing.F) {
	f.Add("Acquire-By-Hash: yes\nSHA256:\n " + strings.Repeat("a", 64) + " 12 main/binary-amd64/Packages.xz\n")
	f.Add("SHA256:\n invalid entry\n")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		manifest, err := parseReleaseManifest(strings.NewReader(input))
		if err != nil {
			return
		}
		paths := make([]string, 0, len(manifest.Entries))
		for _, entry := range manifest.Entries {
			require.True(t, httpcache.SafePath(entry.Path))
			require.GreaterOrEqual(t, entry.Size, int64(0))
			digest, err := hex.DecodeString(entry.SHA256)
			require.NoError(t, err)
			require.Len(t, digest, 32)
			paths = append(paths, entry.Path)
		}
		require.True(t, slices.IsSorted(paths))
	})
}

func FuzzDebianPathClassification(f *testing.F) {
	f.Add("dists/trixie/main/binary-amd64/Packages.xz")
	f.Add("pool/main/h/hello.deb")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}
		_, _, _ = distributionAnchor(input)
		_ = classifyPath(input)
	})
}
