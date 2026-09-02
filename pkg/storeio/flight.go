package storeio

import (
	"context"
	"sync"
)

type FlightGroup struct {
	mu      sync.Mutex
	flights map[string]*Flight
}

type Flight struct {
	done chan struct{}
	err  error
}

func (g *FlightGroup) Begin(key string) (*Flight, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.flights == nil {
		g.flights = make(map[string]*Flight)
	}
	if existing := g.flights[key]; existing != nil {
		return existing, false
	}
	current := &Flight{done: make(chan struct{})}
	g.flights[key] = current
	return current, true
}

func (g *FlightGroup) Wait(ctx context.Context, current *Flight) error {
	if current == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-current.done:
		return current.err
	}
}

func (g *FlightGroup) Finish(key string, current *Flight, err error) {
	if current == nil {
		return
	}
	g.mu.Lock()
	if g.flights[key] == current {
		current.err = err
		delete(g.flights, key)
		close(current.done)
	}
	g.mu.Unlock()
}

func (g *FlightGroup) Do(ctx context.Context, key string, run func() error) error {
	current, leader := g.Begin(key)
	if !leader {
		return g.Wait(ctx, current)
	}
	err := run()
	g.Finish(key, current, err)
	return err
}
