package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzStatusHistoryWindow(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5}, uint8(3), 1)
	f.Add([]byte{}, uint8(0), 0)
	f.Add([]byte{1, 2, 3}, uint8(1), -1)
	f.Fuzz(func(t *testing.T, values []byte, capacity uint8, limit int) {
		if len(values) > 256 {
			t.Skip()
		}
		items := make([]byte, 1+int(capacity%64))
		var expected []byte
		next, filled := 0, false
		for _, value := range values {
			items[next] = value
			next = (next + 1) % len(items)
			filled = filled || next == 0
			expected = append(expected, value)
			if len(expected) > len(items) {
				expected = expected[1:]
			}
		}
		if limit > 0 && limit < len(expected) {
			expected = expected[len(expected)-limit:]
		}
		actual := ringSnapshot(items, next, filled, limit)
		require.Equal(t, expected, actual)
		if len(actual) > 0 {
			actual[0] ^= 0xff
			require.Equal(t, expected, ringSnapshot(items, next, filled, limit))
		}
	})
}
