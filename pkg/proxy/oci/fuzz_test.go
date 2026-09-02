package oci

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzOCIRequestParsers(f *testing.F) {
	f.Add(uint8(0), `Bearer realm="https://auth.example/token",service="registry",scope="repository:library/alpine:pull"`)
	f.Add(uint8(1), "/v2/library/alpine/manifests/latest")
	f.Add(uint8(1), "/v2/library/alpine/blobs/sha256:"+strings.Repeat("a", 64))

	f.Fuzz(func(t *testing.T, kind uint8, input string) {
		if len(input) > 64<<10 {
			t.Skip()
		}
		switch kind % 2 {
		case 0:
			challenge, ok := parseOCIChallenge(input)
			if ok && strings.EqualFold(challenge.scheme, "bearer") {
				require.NotEmpty(t, challenge.realm)
			}
		case 1:
			options := &Options{}
			target, parseErr := url.ParseRequestURI(input)
			if parseErr != nil {
				return
			}
			result, err := resolveRequest(&http.Request{URL: target}, options)
			if err == nil && result.upstreamPath != "" {
				require.True(t, safePath(result.upstreamPath))
			}
		}
	})
}
