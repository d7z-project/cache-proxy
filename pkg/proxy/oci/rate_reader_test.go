package oci

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMinimumRateReadCloser(t *testing.T) {
	tests := []struct {
		name           string
		data           string
		bytesPerSecond int64
		window         time.Duration
		wait           time.Duration
		wantErr        error
	}{
		{name: "fast enough", data: strings.Repeat("x", 4096), bytesPerSecond: 1024, window: 100 * time.Millisecond, wait: 150 * time.Millisecond},
		{name: "too slow", data: strings.Repeat("x", 10), bytesPerSecond: 4096, window: 50 * time.Millisecond, wait: 100 * time.Millisecond, wantErr: errReadRateTooSlow},
		{name: "grace period", data: strings.Repeat("x", 10), bytesPerSecond: 4096, window: time.Hour},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := newMinimumRateReadCloser(io.NopCloser(strings.NewReader(test.data)))
			reader.bytesPerSecond = test.bytesPerSecond
			reader.window = test.window
			defer func() { require.NoError(t, reader.Close()) }()
			time.Sleep(test.wait)
			body, err := io.ReadAll(reader)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.data, string(body))
		})
	}
}
