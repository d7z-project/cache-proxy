package httpcache

import (
	"context"
	"sync"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type objectFlight struct {
	mu        sync.Mutex
	ready     chan struct{}
	done      chan struct{}
	readyOnce sync.Once
	doneOnce  sync.Once
	spool     *growingFile
	response  *utils.ResponseWrapper
	err       error
}

func newObjectFlight() *objectFlight {
	return &objectFlight{ready: make(chan struct{}), done: make(chan struct{})}
}

func (f *objectFlight) publish(spool *growingFile, response *utils.ResponseWrapper) {
	f.mu.Lock()
	f.spool = spool
	f.response = &utils.ResponseWrapper{StatusCode: response.StatusCode, Headers: copyHeadersMap(response.Headers)}
	f.mu.Unlock()
	f.readyOnce.Do(func() { close(f.ready) })
}

func (f *objectFlight) subscribe(ctx context.Context) (*utils.ResponseWrapper, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-f.ready:
	}
	f.mu.Lock()
	spool, template, err := f.spool, f.response, f.err
	f.mu.Unlock()
	if spool == nil || template == nil {
		return nil, false, err
	}
	reader, err := spool.Reader()
	if err != nil {
		return nil, false, err
	}
	return &utils.ResponseWrapper{StatusCode: template.StatusCode, Headers: copyHeadersMap(template.Headers), Body: reader}, true, nil
}

func (f *objectFlight) finish(err error) {
	f.doneOnce.Do(func() {
		f.mu.Lock()
		f.err = err
		f.mu.Unlock()
		f.readyOnce.Do(func() { close(f.ready) })
		close(f.done)
	})
}

func (f *objectFlight) resultError() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
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
	flight := newObjectFlight()
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
