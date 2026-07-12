package utils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaitGroupContextAllowsNilContext(t *testing.T) {
	var wg sync.WaitGroup
	//lint:ignore SA1012 This test verifies nil context fallback behavior.
	require.NoError(t, WaitGroupContext(nil, &wg))
}
