package oci

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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
			policy := &Policy{DefaultPolicy: config.PolicyBypass, BusyPolicy: config.BusyPolicyJoin}
			result, err := resolveRequest(&http.Request{URL: &url.URL{Path: input}}, policy)
			if err == nil && result.upstreamPath != "" {
				require.True(t, httpcache.SafePath(result.upstreamPath))
			}
		}
	})
}
