package config

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestExpirationYAML(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		want     Expiration
		wantErr  bool
	}{
		{"unset", "", 0, false},
		{"unset null", "null", 0, false},
		{"never keyword", "never", ExpirationNever, false},
		{"zero keyword", "0", ExpirationNever, false},
		{"none keyword", "none", ExpirationNever, false},
		{"infinite keyword", "infinite", ExpirationNever, false},
		{"1h", "1h", Expiration(time.Hour), false},
		{"720h", "720h", Expiration(720 * time.Hour), false},
		{"negative error", "-1h", 0, true},
		{"invalid", "abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Expiration
			err := yaml.Unmarshal([]byte(tt.yaml), &got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExpirationJSON(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		want     Expiration
		wantErr  bool
	}{
		{"unset", `""`, 0, false},
		{"never", `"never"`, ExpirationNever, false},
		{"zero", `"0"`, ExpirationNever, false},
		{"1h", `"1h"`, Expiration(time.Hour), false},
		{"invalid", `"abc"`, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Expiration
			err := json.Unmarshal([]byte(tt.json), &got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExpirationMarshalYAML(t *testing.T) {
	tests := []struct {
		name  string
		exp   Expiration
		want  any
	}{
		{"unset", 0, nil},
		{"never", ExpirationNever, "never"},
		{"1h", Expiration(time.Hour), "1h0m0s"},
		{"720h", Expiration(720 * time.Hour), "720h0m0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.exp.MarshalYAML()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExpirationMarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		exp   Expiration
		want  string
	}{
		{"unset", 0, `""`},
		{"never", ExpirationNever, `"never"`},
		{"1h", Expiration(time.Hour), `"1h0m0s"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.exp.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}

func TestExpirationHelpers(t *testing.T) {
	t.Run("IsNever", func(t *testing.T) {
		require.True(t, ExpirationNever.IsNever())
		require.False(t, Expiration(0).IsNever())
		require.False(t, Expiration(time.Hour).IsNever())
	})

	t.Run("IsUnset", func(t *testing.T) {
		require.True(t, Expiration(0).IsUnset())
		require.False(t, ExpirationNever.IsUnset())
		require.False(t, Expiration(time.Hour).IsUnset())
	})

	t.Run("Duration", func(t *testing.T) {
		require.Equal(t, time.Duration(0), Expiration(0).Duration())
		require.Equal(t, time.Hour, Expiration(time.Hour).Duration())
		require.Equal(t, time.Duration(-1), ExpirationNever.Duration())
	})
}

func TestFreshnessYAML(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		want     Freshness
		wantErr  bool
	}{
		{"unset", "", 0, false},
		{"forever keyword", "forever", FreshnessForever, false},
		{"zero keyword", "0", FreshnessForever, false},
		{"always keyword", "always", FreshnessForever, false},
		{"infinite keyword", "infinite", FreshnessForever, false},
		{"1h", "1h", Freshness(time.Hour), false},
		{"negative error", "-1h", 0, true},
		{"invalid", "abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Freshness
			err := yaml.Unmarshal([]byte(tt.yaml), &got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFreshnessHelpers(t *testing.T) {
	t.Run("IsForever", func(t *testing.T) {
		require.True(t, FreshnessForever.IsForever())
		require.False(t, Freshness(0).IsForever())
		require.False(t, Freshness(time.Hour).IsForever())
	})

	t.Run("IsUnset", func(t *testing.T) {
		require.True(t, Freshness(0).IsUnset())
		require.False(t, FreshnessForever.IsUnset())
		require.False(t, Freshness(time.Hour).IsUnset())
	})

	t.Run("Duration", func(t *testing.T) {
		require.Equal(t, time.Duration(0), Freshness(0).Duration())
		require.Equal(t, time.Hour, Freshness(time.Hour).Duration())
		require.Equal(t, time.Duration(-1), FreshnessForever.Duration())
	})
}

func TestFreshnessMarshalYAML(t *testing.T) {
	tests := []struct {
		name  string
		f     Freshness
		want  any
	}{
		{"unset", 0, nil},
		{"forever", FreshnessForever, "forever"},
		{"1h", Freshness(time.Hour), "1h0m0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.f.MarshalYAML()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExpirationYAMLRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		exp  Expiration
	}{
		{"unset", Expiration(0)},
		{"never", ExpirationNever},
		{"1h", Expiration(time.Hour)},
		{"720h", Expiration(720 * time.Hour)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(tt.exp)
			require.NoError(t, err)

			var got Expiration
			err = yaml.Unmarshal(data, &got)
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}

func TestFreshnessYAMLRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		f    Freshness
	}{
		{"unset", Freshness(0)},
		{"forever", FreshnessForever},
		{"1h", Freshness(time.Hour)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(tt.f)
			require.NoError(t, err)

			var got Freshness
			err = yaml.Unmarshal(data, &got)
			require.NoError(t, err)
			require.Equal(t, tt.f, got)
		})
	}
}
