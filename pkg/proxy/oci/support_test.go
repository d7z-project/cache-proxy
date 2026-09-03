package oci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReferenceLocksSerializeSameKey(t *testing.T) {
	locks := &referenceLocks{}
	first := locks.Get("/test")
	require.Same(t, first, locks.Get("/test"))
	require.NotSame(t, first, locks.Get("/other"))

	first.Lock()
	acquired := make(chan struct{})
	go func() {
		locks.Get("/test").Lock()
		close(acquired)
		locks.Get("/test").Unlock()
	}()

	select {
	case <-acquired:
		t.Fatal("lock acquired while held")
	case <-time.After(50 * time.Millisecond):
	}
	first.Unlock()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("lock was not acquired after release")
	}
}
