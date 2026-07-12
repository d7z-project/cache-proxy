package utils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaitGroupContextAllowsNilContext(t *testing.T) {
	var wg sync.WaitGroup
	require.NoError(t, WaitGroupContext(nil, &wg))
}
