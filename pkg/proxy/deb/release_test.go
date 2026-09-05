package deb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReleaseTime(t *testing.T) {
	for _, zone := range []string{"UTC", "GMT", "+0000", "Z"} {
		for _, clock := range []string{"06 Sep 2026 08:04:05", "6 Sep 2026 8:4:5"} {
			value := "Sun, " + clock + " " + zone
			t.Run(value, func(t *testing.T) {
				parsed, err := parseReleaseTime(value)
				require.NoError(t, err)
				require.Equal(t, time.Date(2026, time.September, 6, 8, 4, 5, 0, time.UTC), parsed)
			})
		}
	}
	parsed, err := parseReleaseTime("Sat, 12 Sep 2026 18:34:00 UTC")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, time.September, 12, 18, 34, 0, 0, time.UTC), parsed)
}

func TestReleaseTimeRejectsInvalidDates(t *testing.T) {
	for _, value := range []string{
		"", "not a date", strings.Repeat("x", 129),
		"Sat, 12 Sep 2026 18:34:00", "Sat, 12 Sep 2026 18:34:00 UTC extra",
		"Sat, 12 Sep 2026 18:34:00 +0100", "Sat, 12 Sep 2026 18:34:00 -0000",
		"Sat, 12 Sep 2026 18:34:00 PST", "Sat, 12 Sep 2026 18:34:00 unknown",
		"Sat, 31 Sep 2026 18:34:00 UTC", "Sat, 12 Sep 2026 24:34:00 UTC",
		"Sat, 12 Sep 2026 18:60:00 UTC", "Sat, 12 Sep 2026 18:34:60 UTC",
		"Sat, 12 Sep 2026 18:34:00.5 UTC",
	} {
		t.Run(value, func(t *testing.T) {
			_, err := parseReleaseTime(value)
			require.Error(t, err)
		})
	}
}

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
