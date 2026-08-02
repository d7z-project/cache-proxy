package httpcache

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var ErrDownloadLimit = errors.New("download limit reached")

type downloadWaiter struct {
	instance string
	ready    chan struct{}
	granted  bool
	element  *list.Element
}

type DownloadLimiter struct {
	mu        sync.Mutex
	max       int
	perMax    int
	active    int
	perActive map[string]int
	waiters   list.List
	perWait   map[string]int
}

func NewDownloadLimiter(maxActive, maxPerInstance int) *DownloadLimiter {
	if maxActive <= 0 || maxPerInstance <= 0 {
		return nil
	}
	return &DownloadLimiter{
		max:       maxActive,
		perMax:    maxPerInstance,
		perActive: map[string]int{},
		perWait:   map[string]int{},
	}
}

func (l *DownloadLimiter) Acquire(ctx context.Context, instance string) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	l.mu.Lock()
	if l.waiters.Len() == 0 && l.hasCapacityLocked(instance) {
		l.activateLocked(instance)
		l.mu.Unlock()
		return l.releaseFunc(instance), nil
	}
	if l.waiters.Len() >= l.max || l.perWait[instance] >= l.perMax {
		l.mu.Unlock()
		return nil, ErrDownloadLimit
	}
	waiter := &downloadWaiter{instance: instance, ready: make(chan struct{})}
	waiter.element = l.waiters.PushBack(waiter)
	l.perWait[instance]++
	l.grantWaitersLocked()
	l.mu.Unlock()

	select {
	case <-waiter.ready:
		return l.releaseFunc(instance), nil
	case <-ctx.Done():
		l.mu.Lock()
		if waiter.granted {
			l.mu.Unlock()
			l.release(instance)
		} else {
			l.removeWaiterLocked(waiter)
			l.mu.Unlock()
		}
		return nil, errors.Join(ErrDownloadLimit, ctx.Err())
	}
}

func (l *DownloadLimiter) hasCapacityLocked(instance string) bool {
	return l.active < l.max && l.perActive[instance] < l.perMax
}

func (l *DownloadLimiter) activateLocked(instance string) {
	l.active++
	l.perActive[instance]++
}

func (l *DownloadLimiter) removeWaiterLocked(waiter *downloadWaiter) {
	if waiter.element == nil {
		return
	}
	l.waiters.Remove(waiter.element)
	waiter.element = nil
	l.perWait[waiter.instance]--
	if l.perWait[waiter.instance] == 0 {
		delete(l.perWait, waiter.instance)
	}
}

func (l *DownloadLimiter) grantWaitersLocked() {
	for l.active < l.max {
		var selected *downloadWaiter
		for element := l.waiters.Front(); element != nil; element = element.Next() {
			waiter := element.Value.(*downloadWaiter)
			if l.perActive[waiter.instance] < l.perMax {
				selected = waiter
				break
			}
		}
		if selected == nil {
			return
		}
		l.removeWaiterLocked(selected)
		l.activateLocked(selected.instance)
		selected.granted = true
		close(selected.ready)
	}
}

func (l *DownloadLimiter) releaseFunc(instance string) func() {
	var once sync.Once
	return func() {
		once.Do(func() { l.release(instance) })
	}
}

func (l *DownloadLimiter) release(instance string) {
	l.mu.Lock()
	l.active--
	l.perActive[instance]--
	if l.perActive[instance] == 0 {
		delete(l.perActive, instance)
	}
	l.grantWaitersLocked()
	l.mu.Unlock()
}

func (l *DownloadLimiter) Update(maxActive, maxPerInstance int) {
	if l == nil || maxActive <= 0 || maxPerInstance <= 0 {
		return
	}
	l.mu.Lock()
	l.max = maxActive
	l.perMax = maxPerInstance
	l.grantWaitersLocked()
	l.mu.Unlock()
}
