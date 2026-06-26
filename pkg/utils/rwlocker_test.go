package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRWLockGroupReturnsConsistentLocks(t *testing.T) {
	group := NewRWLockGroup()
	l1 := group.Get("/test")
	l2 := group.Get("/test")
	require.Same(t, l1, l2)

	l3 := group.Get("/other")
	require.NotSame(t, l1, l3)
}

func TestRWLockGroupExclusiveLock(t *testing.T) {
	group := NewRWLockGroup()
	l := group.Get("/lock")
	l.Lock()

	acquired := make(chan struct{})
	go func() {
		l.Lock()
		close(acquired)
		l.Unlock()
	}()

	select {
	case <-acquired:
		t.Fatal("lock should not be acquired while held")
	case <-time.After(50 * time.Millisecond):
	}

	l.Unlock()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("lock should be acquired after release")
	}
}
