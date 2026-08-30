package utils

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsBrowserRequest(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		fetchMode string
		want      bool
	}{
		{name: "chrome", userAgent: "Mozilla/5.0 Chrome/126.0.0.0 Safari/537.36", want: true},
		{name: "firefox", userAgent: "Mozilla/5.0 Firefox/127.0", want: true},
		{name: "fetch metadata", userAgent: "custom-browser", fetchMode: "navigate", want: true},
		{name: "curl", userAgent: "curl/8.8.0"},
		{name: "package manager", userAgent: "Debian APT-HTTP/1.3"},
		{name: "mozilla compatible client", userAgent: "Mozilla/5.0 compatible-client"},
		{name: "empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("User-Agent", test.userAgent)
			if test.fetchMode != "" {
				req.Header.Set("Sec-Fetch-Mode", test.fetchMode)
			}
			require.Equal(t, test.want, IsBrowserRequest(req))
		})
	}
}

func TestRequestUserAgentPrecedence(t *testing.T) {
	browser := httptest.NewRequest(http.MethodGet, "/", nil)
	browser.Header.Set("User-Agent", "Mozilla/5.0 Firefox/127.0")
	nonBrowser := httptest.NewRequest(http.MethodGet, "/", nil)
	nonBrowser.Header.Set("User-Agent", "curl/8.8.0")

	client := DefaultHTTPClientWrapper()
	userAgent, forwarded := client.RequestUserAgent(browser)
	require.Equal(t, browser.UserAgent(), userAgent)
	require.True(t, forwarded)

	userAgent, forwarded = client.RequestUserAgent(nonBrowser)
	require.Equal(t, DefaultUserAgent, userAgent)
	require.False(t, forwarded)

	client.UserAgent = "forced/1"
	client.UserAgentConfigured = true
	userAgent, forwarded = client.RequestUserAgent(browser)
	require.Equal(t, "forced/1", userAgent)
	require.False(t, forwarded)
}

func TestVariesByUserAgent(t *testing.T) {
	require.True(t, VariesByUserAgent("Accept-Encoding, user-agent"))
	require.True(t, VariesByUserAgent("Accept-Encoding", "*"))
	require.False(t, VariesByUserAgent("Accept-Encoding", "Origin"))
}
