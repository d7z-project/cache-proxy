package deb

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func FuzzReleaseManifest(f *testing.F) {
	f.Add("Acquire-By-Hash: yes\nSHA256:\n " + strings.Repeat("a", 64) + " 12 main/binary-amd64/Packages.xz\n")
	f.Add("Architectures: amd64 arm64\nSHA256:\n " + strings.Repeat("a", 64) + " 12 main/binary-amd64/Packages.gz\n " + strings.Repeat("b", 64) + " 10 main/binary-arm64/Packages.gz\nSHA512:\n " + strings.Repeat("c", 128) + " 12 main/binary-amd64/Packages.gz\n")
	f.Add("Suite: ./\nSHA256:\n " + strings.Repeat("d", 64) + " 4 Packages.gz\n " + strings.Repeat("e", 64) + " 7 Contents-all.gz\n")
	f.Add("SHA256:\n " + strings.Repeat("f", 64) + " 9 vendor/dists/stable/main/Sources.zst\n " + strings.Repeat("1", 64) + " 9 vendor/dists/stable/main/by-hash/SHA256/" + strings.Repeat("1", 64) + "\n")
	f.Add("SHA256:\n " + strings.Repeat("a", 64) + " 1 ../Packages\n")
	f.Add("SHA256:\n " + strings.Repeat("a", 64) + " 1 main/Packages\n " + strings.Repeat("b", 64) + " 2 main/Packages\n")
	var manyEntries strings.Builder
	manyEntries.WriteString("SHA256:\n")
	for range 1024 {
		manyEntries.WriteString(" " + strings.Repeat("2", 64) + " 1 main/Packages\n")
	}
	f.Add(manyEntries.String())
	f.Add("SHA256:\n invalid entry\n")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		manifest, err := parseReleaseManifest(context.Background(), strings.NewReader(input))
		if err != nil {
			return
		}
		paths := make([]string, 0, len(manifest.Entries))
		for _, entry := range manifest.Entries {
			_, err := storeio.CleanRelative(entry.Path)
			require.NoError(t, err)
			require.GreaterOrEqual(t, entry.Size, int64(0))
			if entry.SHA256 != "" {
				digest, err := hex.DecodeString(entry.SHA256)
				require.NoError(t, err)
				require.Len(t, digest, 32)
			}
			if entry.SHA512 != "" {
				digest, err := hex.DecodeString(entry.SHA512)
				require.NoError(t, err)
				require.Len(t, digest, 64)
			}
			require.True(t, entry.SHA256 != "" || entry.SHA512 != "")
			paths = append(paths, entry.Path)
		}
		require.True(t, slices.IsSorted(paths))
		for index := 1; index < len(paths); index++ {
			require.NotEqual(t, paths[index-1], paths[index])
		}
	})
}

func FuzzDebianPathClassification(f *testing.F) {
	f.Add("dists/trixie/main/binary-amd64/Packages.xz")
	f.Add("dists/trixie/main/source/Sources.gz")
	f.Add("dists/trixie/main/Contents-arm64.gz")
	f.Add("dists/trixie/main/i18n/Translation-en.xz")
	f.Add("dists/trixie/main/binary-amd64/Packages.diff/Index")
	f.Add("dists/trixie/main/binary-amd64/by-hash/SHA256/" + strings.Repeat("a", 64))
	f.Add("flat/Packages.gz")
	f.Add("dists/trixie/InRelease/")
	f.Add("dists/trixie/main/Packages%2Fescape")
	f.Add("pool/main/h/hello.deb")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}
		anchor := isAnchorPath(input)
		metadata := isMetadataPath(input)
		if strings.HasSuffix(input, "/") {
			require.False(t, anchor)
			require.False(t, metadata)
		}
		if anchor {
			require.True(t, metadata)
		}
	})
}
