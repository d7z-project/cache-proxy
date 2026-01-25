package utils

import "sync"

type RWLockGroup struct {
	group sync.Map
}

func NewRWLockGroup() *RWLockGroup {
	return &RWLockGroup{}
}

func (g *RWLockGroup) Get(key string) *sync.RWMutex {
	actual, _ := g.group.LoadOrStore(key, &sync.RWMutex{})
	return actual.(*sync.RWMutex)
}
