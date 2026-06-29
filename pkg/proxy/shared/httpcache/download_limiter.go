package httpcache

import (
	"context"
	"errors"
	"sync"
)

var ErrDownloadLimit = errors.New("download limit reached")

type DownloadLimiter struct {
	mu        sync.Mutex
	max       int
	perMax    int
	active    int
	perActive map[string]int
}

func NewDownloadLimiter(maxActive, maxPerInstance int) *DownloadLimiter {
	if maxActive <= 0 || maxPerInstance <= 0 {
		return nil
	}
	return &DownloadLimiter{
		max:       maxActive,
		perMax:    maxPerInstance,
		perActive: map[string]int{},
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
	defer l.mu.Unlock()
	if l.active >= l.max || l.perActive[instance] >= l.perMax {
		return nil, ErrDownloadLimit
	}
	l.active++
	l.perActive[instance]++
	return func() {
		l.mu.Lock()
		l.active--
		l.perActive[instance]--
		if l.perActive[instance] <= 0 {
			delete(l.perActive, instance)
		}
		l.mu.Unlock()
	}, nil
}

func (l *DownloadLimiter) Update(maxActive, maxPerInstance int) {
	if l == nil || maxActive <= 0 || maxPerInstance <= 0 {
		return
	}
	l.mu.Lock()
	l.max = maxActive
	l.perMax = maxPerInstance
	l.mu.Unlock()
}
