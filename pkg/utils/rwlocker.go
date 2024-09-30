package utils

import "sync"

type RWLockGroup struct {
	group sync.Map
}

func NewRWLockGroup() *RWLockGroup {
	return &RWLockGroup{
		sync.Map{},
	}
}

func (g *RWLockGroup) Open(key string) *RWLocker {
	actual, _ := g.group.LoadOrStore(key, &RWLocker{
		locker:       sync.RWMutex{},
		switchLocker: sync.Mutex{},
	})
	locker := actual.(*RWLocker)
	return locker
}

type RWLocker struct {
	locker       sync.RWMutex
	switchLocker sync.Mutex
}

func (rw *RWLocker) TryLock(read bool) bool {
	rw.switchLocker.Lock()
	defer rw.switchLocker.Unlock()
	if read {
		return rw.locker.TryRLock()
	} else {
		return rw.locker.TryLock()
	}
}

func (rw *RWLocker) Lock(read bool) *LockerContent {
	rw.switchLocker.Lock()
	defer rw.switchLocker.Unlock()
	if read {
		rw.locker.RLock()
		return &LockerContent{
			rw,
			true,
			func() {
				rw.locker.RUnlock()
			},
		}
	} else {
		rw.locker.Lock()
		return &LockerContent{
			rw,
			false,
			func() {
				rw.locker.Unlock()
			},
		}
	}
}

type LockerContent struct {
	locker *RWLocker
	rLock  bool
	close  func()
}

func (c *LockerContent) AsRLocker() {
	if !c.rLock {
		c.locker.switchLocker.Lock()
		defer c.locker.switchLocker.Unlock()
		if !c.rLock {
			c.close()
			c.rLock = true
			c.locker.locker.RLock()
			c.close = func() {
				c.locker.locker.RUnlock()
			}
		}
	}
}

func (c *LockerContent) AsLocker() {
	if c.rLock {
		c.locker.switchLocker.Lock()
		defer c.locker.switchLocker.Unlock()
		if c.rLock {
			c.close()
			c.rLock = false
			c.locker.locker.Lock()
			c.close = func() {
				c.locker.locker.Unlock()
			}
		}
	}
}

func (c *LockerContent) Close() {
	c.close()
}
