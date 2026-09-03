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
	for _, raw := range []string{"/a%2fb", "/a%2Fb", "/a%5cb", "/a%00b", "/a/%2e%2e/b", "/a\\b"} {
		target, err := url.ParseRequestURI(raw)
		require.NoError(t, err)
		_, err = CleanURLPath(target)
		require.Error(t, err, raw)
	}
}

func TestCleanURLPathAcceptsEquivalentPercentEncodings(t *testing.T) {
	for raw, expected := range map[string]string{
		"/pkg_1%2bpve.deb": "pkg_1+pve.deb",
		"/pkg_1%2Bpve.deb": "pkg_1+pve.deb",
		"/a%41":            "aA",
		"/a%2eb":           "a.b",
		"/caf%C3%A9":       "caf\xc3\xa9",
	} {
		target, err := url.ParseRequestURI(raw)
		require.NoError(t, err)
		cleaned, err := CleanURLPath(target)
		require.NoError(t, err, raw)
		require.Equal(t, expected, cleaned)
	}
}

func TestCleanURLPathPreservesRootAndDirectorySemantics(t *testing.T) {
	for raw, expected := range map[string]string{
		"/":           "",
		"/repo/":      "repo/",
		"/repo/path/": "repo/path/",
	} {
		target, err := url.ParseRequestURI(raw)
		require.NoError(t, err)
		cleaned, err := CleanURLPath(target)
		require.NoError(t, err)
		require.Equal(t, expected, cleaned)
	}
}

func TestDecodeURLPathPreservesProtocolPunctuationAndTrailingSlash(t *testing.T) {
	target, err := url.ParseRequestURI("/github.com/!owner/module/@v/list/")
	require.NoError(t, err)
	decoded, err := DecodeURLPath(target)
	require.NoError(t, err)
	require.Equal(t, "/github.com/!owner/module/@v/list/", decoded)
}
