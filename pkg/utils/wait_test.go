package utils

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaitGroupContextReturnsCancellation(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, WaitGroupContext(ctx, &wg), context.Canceled)
	wg.Done()
}
