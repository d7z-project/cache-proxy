package gomod

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/module"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func FuzzParseModuleRequest(f *testing.F) {
	f.Add("github.com/example/project/@v/v1.2.3.zip")
	f.Add("github.com/example/project/@v/list")
	f.Add("example.com/Upper!case/@v/v0.0.1.mod")
	f.Add("../escape/@v/v1.0.0.info")

	f.Fuzz(func(t *testing.T, target string) {
		if len(target) > 64<<10 {
			t.Skip()
		}
		result, err := parseModuleRequest(target)
		if err != nil {
			return
		}
		require.NoError(t, module.CheckPath(result.modulePath))
		require.NotEmpty(t, result.cacheKey)
		require.True(t, httpcache.SafePath(result.cacheKey))
	})
}
