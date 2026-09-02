package deb

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReleaseRequiresStrongChecksumEntry(t *testing.T) {
	_, err := parseReleaseManifest(context.Background(), strings.NewReader("Origin: Debian\nSHA256:\n"))
	require.Error(t, err)

	manifest, err := parseReleaseManifest(context.Background(), strings.NewReader("SHA512:\n "+strings.Repeat("a", 128)+" 0 main/Packages\n"))
	require.NoError(t, err)
	require.Len(t, manifest.Entries, 1)
	require.Empty(t, manifest.Entries[0].SHA256)
	require.Len(t, manifest.Entries[0].SHA512, 128)

	_, err = parseReleaseManifest(context.Background(), strings.NewReader("SHA256:\n malformed\n"))
	require.Error(t, err)
}

func TestReleaseAndClearsignedPayloadNormalizeEqually(t *testing.T) {
	plain := "Origin: Debian\nAcquire-By-Hash: yes\nSHA256:\n " + strings.Repeat("a", 64) + " 12 main/Packages.xz\n"
	signed := "-----BEGIN PGP SIGNED MESSAGE-----\nHash: SHA256\n\n" + plain + "-----BEGIN PGP SIGNATURE-----\nsignature\n-----END PGP SIGNATURE-----\n"
	left, err := parseReleaseManifest(context.Background(), strings.NewReader(plain))
	require.NoError(t, err)
	right, err := parseReleaseManifest(context.Background(), strings.NewReader(signed))
	require.NoError(t, err)
	require.True(t, releaseManifestsEqual(left, right))

	_, err = parseReleaseManifest(context.Background(), strings.NewReader(strings.TrimSuffix(signed, "-----END PGP SIGNATURE-----\n")))
	require.ErrorContains(t, err, "incomplete clearsigned Release signature")
}

func TestReleaseParserHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := parseReleaseManifest(ctx, strings.NewReader("SHA256:\n"))
	require.ErrorIs(t, err, context.Canceled)
}
