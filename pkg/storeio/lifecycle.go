package storeio

import (
	"context"
	"errors"
	"sync"
)

var ErrLifecycleClosed = errors.New("handler lifecycle is closed")

// Lifecycle owns request-independent background work. Admission and closing
// share one lock so Add can never race a zero-count Wait.
type Lifecycle struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	wait    sync.WaitGroup
	done    chan struct{}
	closing bool
}

func NewLifecycle() *Lifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &Lifecycle{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (l *Lifecycle) Context() context.Context { return l.ctx }

func (l *Lifecycle) Begin() (context.Context, func(), error) {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return nil, nil, ErrLifecycleClosed
	}
	l.wait.Add(1)
	l.mu.Unlock()
	var once sync.Once
	return l.ctx, func() { once.Do(l.wait.Done) }, nil
}

func (l *Lifecycle) Go(run func(context.Context)) error {
	ctx, done, err := l.Begin()
	if err != nil {
		return err
	}
	go func() {
		defer done()
		run(ctx)
	}()
	return nil
}

func (l *Lifecycle) Close(ctx context.Context) error {
	l.mu.Lock()
	if !l.closing {
		l.closing = true
		l.cancel()
		go func() {
			l.wait.Wait()
			close(l.done)
		}()
	}
	done := l.done
	l.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
