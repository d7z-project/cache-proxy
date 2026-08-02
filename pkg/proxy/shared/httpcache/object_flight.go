package httpcache

import "sync"

type objectFlight struct {
	done chan struct{}
	once sync.Once
	err  error
}

func (f *objectFlight) finish(err error) {
	f.once.Do(func() {
		f.err = err
		close(f.done)
	})
}

type objectFlights struct {
	mu      sync.Mutex
	flights map[string]*objectFlight
}

func (g *objectFlights) begin(key string) (*objectFlight, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if flight := g.flights[key]; flight != nil {
		return flight, false
	}
	if g.flights == nil {
		g.flights = make(map[string]*objectFlight)
	}
	flight := &objectFlight{done: make(chan struct{})}
	g.flights[key] = flight
	return flight, true
}

func (g *objectFlights) finish(key string, flight *objectFlight, err error) {
	flight.finish(err)
	g.mu.Lock()
	if g.flights[key] == flight {
		delete(g.flights, key)
	}
	g.mu.Unlock()
}

func (g *objectFlights) active(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.flights[key] != nil
}
