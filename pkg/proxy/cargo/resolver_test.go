package cargo

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
)

func TestCrateTargetURLTracksCachedConfigUpdates(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.MkdirAll("cargo/cargo/index", 0o755))

	resolver := newResolver(&Policy{}, store, "cargo")
	path := "api/v1/crates/MyCrate/1.2.3/download"
	require.Empty(t, resolver.crateTargetURL(ctx, path))

	_, err = store.Put(ctx, "cargo", "cargo/index/config.json", strings.NewReader(`{"dl":"https://registry.example/api"}`), nil)
	require.NoError(t, err)
	require.Equal(t, "https://registry.example/api/MyCrate/1.2.3/download", resolver.crateTargetURL(ctx, path))

	_, err = store.Put(ctx, "cargo", "cargo/index/config.json", strings.NewReader(`{"dl":"https://cdn.example/{lowerprefix}/{crate}/{crate}-{version}.crate"}`), nil)
	require.NoError(t, err)
	require.Equal(t, "https://cdn.example/my/cr/MyCrate/MyCrate-1.2.3.crate", resolver.crateTargetURL(ctx, path))
}

func TestCrateTargetURLResolvesChecksumMarkerFromCachedIndex(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.MkdirAll("cargo/cargo/index", 0o755))
	_, err = store.Put(ctx, "cargo", "cargo/index/config.json", strings.NewReader(`{"dl":"https://cdn.example/{crate}/{sha256-checksum}.crate"}`), nil)
	require.NoError(t, err)
	require.NoError(t, store.MkdirAll("cargo/cargo/index/ex/am", 0o755))
	checksum := strings.Repeat("ab", 32)
	_, err = store.Put(ctx, "cargo", "cargo/index/ex/am/example", strings.NewReader(
		`{"name":"example","vers":"0.9.0","cksum":"`+strings.Repeat("cd", 32)+`"}`+"\n"+
			`{"name":"example","vers":"1.0.0","cksum":"`+checksum+`"}`+"\n",
	), nil)
	require.NoError(t, err)

	resolver := newResolver(&Policy{}, store, "cargo")
	require.Equal(t, "https://cdn.example/example/"+checksum+".crate", resolver.crateTargetURL(ctx, "api/v1/crates/example/1.0.0/download"))
	require.Empty(t, resolver.crateTargetURL(ctx, "api/v1/crates/example/2.0.0/download"))
}

func TestCratePrefix(t *testing.T) {
	require.Equal(t, "1", cratePrefix("a"))
	require.Equal(t, "2", cratePrefix("ab"))
	require.Equal(t, "3/a", cratePrefix("abc"))
	require.Equal(t, "My/Cr", cratePrefix("MyCrate"))
	require.Equal(t, "缓存/代理", cratePrefix("缓存代理包"))
}
