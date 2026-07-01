package bus

import (
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type EventType string

const (
	EventMetadataDiscovered EventType = "metadata_discovered"
	EventMetadataRemoved    EventType = "metadata_removed"
	EventUpstreamState      EventType = "upstream_state"
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

type UpstreamStatePayload struct {
	Instance string
	Mode     string
	Upstream string
	From     string
	To       string
	Reason   string
	Detail   string
}

type Bus struct {
	mu   sync.RWMutex
	subs map[EventType][]chan Event
	m    *metrics
}

func New() *Bus {
	return NewWithRegisterer(nil)
}

func NewWithRegisterer(reg prometheus.Registerer) *Bus {
	return &Bus{subs: map[EventType][]chan Event{}, m: newMetrics(reg)}
}

func (b *Bus) Subscribe(types ...EventType) <-chan Event {
	ch := make(chan Event, 128)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range types {
		b.subs[t] = append(b.subs[t], ch)
		if b.m != nil {
			b.m.subscribers.WithLabelValues(string(t)).Set(float64(len(b.subs[t])))
		}
	}
	return ch
}

func (b *Bus) Publish(evt Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	evt.Timestamp = time.Now()
	eventType := string(evt.Type)
	if b.m != nil {
		b.m.published.WithLabelValues(eventType).Inc()
	}
	subs := b.subs[evt.Type]
	if len(subs) == 0 {
		if b.m != nil {
			b.m.dropped.WithLabelValues(eventType, "no_subscriber").Inc()
		}
		return
	}
	delivered := 0
	for _, ch := range subs {
		select {
		case ch <- evt:
			delivered++
		default:
			slog.Debug("bus event dropped", "type", evt.Type, "reason", "subscriber full")
			if b.m != nil {
				b.m.dropped.WithLabelValues(eventType, "subscriber_full").Inc()
			}
		}
	}
	if b.m != nil {
		b.m.delivered.WithLabelValues(eventType).Add(float64(delivered))
	}
}

func (b *Bus) Unsubscribe(ch <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for t, old := range b.subs {
		filtered := make([]chan Event, 0, len(old))
		for _, sub := range old {
			if sub != ch {
				filtered = append(filtered, sub)
			}
		}
		if len(filtered) == 0 {
			delete(b.subs, t)
		} else {
			b.subs[t] = filtered
		}
		if b.m != nil {
			b.m.subscribers.WithLabelValues(string(t)).Set(float64(len(filtered)))
		}
	}
}
