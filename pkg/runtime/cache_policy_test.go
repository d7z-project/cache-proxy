package runtime

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCachePolicyFreshness(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		control  string
		lifetime time.Duration
		strict   bool
	}{
		{"max-age=120, s-maxage=10", 10 * time.Second, false},
		{"s-maxage=10, max-age=120", 10 * time.Second, false},
		{"MAX-AGE = \"20\"", 20 * time.Second, false},
		{"max-age=0", 0, false},
		{"no-cache=\"set-cookie\", max-age=20", 20 * time.Second, true},
		{"max-age=1, max-age=2", 2 * time.Second, true},
		{"max-age=18446744073709551615", time.Minute, true},
	} {
		t.Run(tc.control, func(t *testing.T) {
			p := ParseCachePolicy(http.Header{"Cache-Control": {tc.control}}, now, time.Minute)
			require.Equal(t, tc.lifetime, p.Lifetime)
			require.Equal(t, tc.strict, p.NoCache)
		})
	}
	header := http.Header{"Date": {now.Add(-time.Minute).Format(http.TimeFormat)}, "Age": {"80"}, "Cache-Control": {"max-age=90"}}
	require.Equal(t, 85*time.Second, ResponseAge(header, now, now.Add(5*time.Second)))
	require.False(t, ResponseFresh(header, now.Add(-20*time.Second), time.Minute))
	header.Set("Age", "18446744073709551615")
	require.Equal(t, time.Duration(math.MaxInt64), ResponseAge(header, now, now))
}

func TestCachePolicyValidationAndStale(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Cache-Control", "max-age = \"0\"")
	require.True(t, RequestForcesRevalidation(req))
	require.False(t, StaleAllowed(req, nil))
	req.Header.Set("Cache-Control", "x-no-cache-suffix")
	require.False(t, RequestForcesRevalidation(req))
	for _, control := range []string{"no-store", "no-cache", "must-revalidate", "proxy-revalidate", "s-maxage=60"} {
		require.False(t, StaleAllowed(req, http.Header{"Cache-Control": {control}}))
	}
	require.True(t, StaleAllowed(req, http.Header{"Cache-Control": {"max-age=60"}}))
	merged := MergeRevalidationHeader(http.Header{"Age": {"300"}, "Date": {time.Now().Format(http.TimeFormat)}, "Etag": {`"body"`}}, http.Header{"Cache-Control": {"no-cache"}})
	require.Empty(t, merged.Get("Age"))
	require.Empty(t, merged.Get("Date"))
	require.Equal(t, `"body"`, merged.Get("ETag"))
}

func TestCachePolicyExpiryAndBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	header := http.Header{"Date": {now.Format(http.TimeFormat)}, "Expires": {now.Add(2 * time.Minute).Format(http.TimeFormat)}}
	require.Equal(t, time.Minute, ParseCachePolicy(header, now, time.Minute).Lifetime)
	require.Equal(t, 2*time.Minute, ParseCachePolicy(header, now, 0).Lifetime)
	header.Set("Cache-Control", "max-age=20")
	require.Equal(t, 20*time.Second, ParseCachePolicy(header, now, time.Minute).Lifetime)
	header.Set("Cache-Control", strings.Repeat("x,", 257))
	require.True(t, ParseCachePolicy(header, now, time.Minute).NoStore)
	header.Set("Cache-Control", strings.Repeat("x", 64<<10+1))
	require.True(t, ParseCachePolicy(header, now, time.Minute).NoStore)
	require.False(t, ResponseFresh(nil, time.Time{}, time.Hour))
}

func FuzzCachePolicy(f *testing.F) {
	f.Add("max-age=60, s-maxage=20", "10", int64(12))
	f.Add("no-cache", "18446744073709551615", int64(-1))
	f.Fuzz(func(t *testing.T, control, age string, seconds int64) {
		if len(control)+len(age) > 70<<10 {
			t.Skip()
		}
		now := time.Unix(seconds%2_000_000_000, 0)
		header := http.Header{"Cache-Control": {control}, "Age": {age}}
		p := ParseCachePolicy(header, now, time.Hour)
		require.GreaterOrEqual(t, p.Lifetime, time.Duration(0))
		require.LessOrEqual(t, p.Lifetime, time.Hour)
		require.GreaterOrEqual(t, ResponseAge(header, now, now.Add(time.Hour)), time.Duration(0))
	})
}
