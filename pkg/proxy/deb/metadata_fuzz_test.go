package deb

import (
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func FuzzDebianMetadataParsers(f *testing.F) {
	f.Add(uint8(0), "Acquire-By-Hash: yes\nSHA256:\n "+strings.Repeat("a", 64)+" 12 main/binary-amd64/Packages.xz\n")
	f.Add(uint8(1), "Package: hello\nFilename: pool/main/h/hello_1.0_amd64.deb\n\n")
	f.Add(uint8(2), "Package: hello\nDirectory: pool/main/h/hello\nChecksums-Sha256:\n abc 1 hello.dsc\n\n")
	f.Add(uint8(0), "SHA256:\n invalid entry\n")

	f.Fuzz(func(t *testing.T, kind uint8, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		switch kind % 3 {
		case 0:
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
		case 1, 2:
			paths := &filerepo.PathIndexBuilder{}
			var err error
			if kind%3 == 1 {
				_, err = parsePackages(strings.NewReader(input), paths, 0)
			} else {
				_, err = parseSources(strings.NewReader(input), paths, 0)
			}
			if err == nil {
				for _, item := range paths.Finalize() {
					require.True(t, httpcache.SafePath(item))
				}
			}
		}
	})
}
