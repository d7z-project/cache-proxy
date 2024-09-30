package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	group := NewRWLockGroup()
	open := group.Open("/test")
	lock := open.Lock(true)
	assert.True(t, open.TryLock(true))
	lock.Close()
	assert.True(t, open.TryLock(true))
}
