package storeio

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanRelativeDoesNotRewriteSegments(t *testing.T) {
	for _, value := range []string{"a/../b", "a/./b", "a//b", "../a", "/../a", "a/"} {
		_, err := CleanRelative(value)
		require.Error(t, err, value)
	}
	cleaned, err := CleanRelative("dists/trixie/InRelease")
	require.NoError(t, err)
	require.Equal(t, "dists/trixie/InRelease", cleaned)
	_, err = CleanRelative("/dists/trixie/InRelease")
	require.Error(t, err)
}

func TestCleanURLPathRejectsEncodedSeparators(t *testing.T) {
	for _, raw := range []string{"/a%2fb", "/a%5cb", "/a%00b", "/a/%2e%2e/b", "/a%41", "/a%2eb", "/a%2fb"} {
		target, err := url.ParseRequestURI(raw)
		require.NoError(t, err)
		_, err = CleanURLPath(target)
		require.Error(t, err, raw)
	}
}

func TestDecodeCanonicalURLPathPreservesProtocolPunctuationAndTrailingSlash(t *testing.T) {
	target, err := url.ParseRequestURI("/github.com/!owner/module/@v/list/")
	require.NoError(t, err)
	decoded, err := DecodeCanonicalURLPath(target)
	require.NoError(t, err)
	require.Equal(t, "/github.com/!owner/module/@v/list/", decoded)
}
