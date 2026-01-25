package utils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	group := NewRWLockGroup()
	l1 := group.Get("/test")
	l2 := group.Get("/test")
	assert.Same(t, l1, l2)

	l3 := group.Get("/other")
	assert.NotSame(t, l1, l3)

	// Basic lock test
	l1.Lock()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		l2.Lock()
		l2.Unlock()
	}()
	l1.Unlock()
	wg.Wait()
}
