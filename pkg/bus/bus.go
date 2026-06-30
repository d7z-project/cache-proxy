package bus

import (
	"log/slog"
	"sync"
	"time"
)

type EventType string

const (
	EventMetadataDiscovered EventType = "metadata_discovered"
	EventMetadataRemoved    EventType = "metadata_removed"
)

type Event struct {
	Type      EventType
	Timestamp time.Time
	Payload   any
}

type MetadataDiscoveredPayload struct {
	Instance string
	SubPath  string
}

type MetadataRemovedPayload struct {
	Instance string
	SubPath  string
}

type Bus struct {
	mu   sync.RWMutex
	subs map[EventType][]chan Event
}

func New() *Bus {
	return &Bus{subs: map[EventType][]chan Event{}}
}

func (b *Bus) Subscribe(types ...EventType) <-chan Event {
	ch := make(chan Event, 128)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range types {
		b.subs[t] = append(b.subs[t], ch)
	}
	return ch
}

func (b *Bus) Publish(evt Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	evt.Timestamp = time.Now()
	for _, ch := range b.subs[evt.Type] {
		select {
		case ch <- evt:
		default:
			slog.Debug("bus event dropped", "type", evt.Type, "reason", "subscriber full")
		}
	}
}
