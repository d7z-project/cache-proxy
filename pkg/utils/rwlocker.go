package utils

import (
	"hash/fnv"
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
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &g.locks[h.Sum32()%shardCount]
}
