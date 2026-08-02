package utils

import (
	"sync"
)

const shardCount = 4096

type RWLockGroup struct {
	locks [shardCount]sync.RWMutex
}

func NewRWLockGroup() *RWLockGroup {
	return &RWLockGroup{}
}

func (g *RWLockGroup) Get(key string) *sync.RWMutex {
	const offset32 = 2166136261
	const prime32 = 16777619
	hash := uint32(offset32)
	for i := range len(key) {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return &g.locks[hash%shardCount]
}
