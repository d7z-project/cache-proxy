package bus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBusSubscribeAndPublish(t *testing.T) {
	b := New()
	ch := b.Subscribe(EventMetadataDiscovered)

	b.Publish(Event{
		Type:    EventMetadataDiscovered,
		Payload: MetadataDiscoveredPayload{Instance: "test", SubPath: "path"},
	})

	select {
	case evt := <-ch:
		require.Equal(t, EventMetadataDiscovered, evt.Type)
		p, ok := evt.Payload.(MetadataDiscoveredPayload)
		require.True(t, ok)
		require.Equal(t, "test", p.Instance)
		require.Equal(t, "path", p.SubPath)
		require.False(t, evt.Timestamp.IsZero())
	case <-time.After(time.Second):
		t.Fatal("expected event not received")
	}
}

func TestBusMultiSubscriber(t *testing.T) {
	b := New()
	ch1 := b.Subscribe(EventMetadataDiscovered)
	ch2 := b.Subscribe(EventMetadataDiscovered)

	b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})

	for i, ch := range []<-chan Event{ch1, ch2} {
		select {
		case evt := <-ch:
			require.Equal(t, EventMetadataDiscovered, evt.Type)
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive event", i)
		}
	}
}

func TestBusMultiTypeSubscription(t *testing.T) {
	b := New()
	ch := b.Subscribe(EventMetadataDiscovered, EventMetadataRemoved)

	b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "d"}})
	b.Publish(Event{Type: EventMetadataRemoved, Payload: MetadataRemovedPayload{Instance: "r"}})

	types := map[EventType]bool{}
	for i := 0; i < 2; i++ {
		select {
		case evt := <-ch:
			types[evt.Type] = true
		case <-time.After(time.Second):
			t.Fatal("expected event not received")
		}
	}
	require.True(t, types[EventMetadataDiscovered])
	require.True(t, types[EventMetadataRemoved])
}

func TestBusDroppedEventOnFullChannel(t *testing.T) {
	b := New()
	ch := b.Subscribe(EventMetadataDiscovered)

	for i := 0; i < 130; i++ {
		b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})
	}

	time.Sleep(50 * time.Millisecond)
	drained := 0
	for {
		select {
		case <-ch:
			drained++
		default:
			goto done
		}
	}
done:
	require.LessOrEqual(t, drained, 128, "channel should not exceed buffer")
}

func TestBusTimestampSetOnPublish(t *testing.T) {
	b := New()
	ch := b.Subscribe(EventMetadataDiscovered)
	before := time.Now()
	b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})

	select {
	case evt := <-ch:
		require.False(t, evt.Timestamp.Before(before))
	case <-time.After(time.Second):
		t.Fatal("expected event not received")
	}
}

func TestBusSeparateEventTypesNotMixed(t *testing.T) {
	b := New()
	ch1 := b.Subscribe(EventMetadataDiscovered)
	b.Subscribe(EventMetadataRemoved) // no consumer for this

	b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})

	select {
	case evt := <-ch1:
		require.Equal(t, EventMetadataDiscovered, evt.Type)
	case <-time.After(time.Second):
		t.Fatal("expected event not received")
	}
}

func TestBusPublishWithNoSubscribers(t *testing.T) {
	b := New()
	require.NotPanics(t, func() {
		b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})
	})
}

func TestBusConcurrentPublish(t *testing.T) {
	b := New()
	ch := b.Subscribe(EventMetadataDiscovered)

	go func() {
		for i := 0; i < 100; i++ {
			b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})
		}
	}()

	received := 0
	timeout := time.After(3 * time.Second)
	for received < 100 {
		select {
		case <-ch:
			received++
		case <-timeout:
			t.Fatalf("only received %d/100 events", received)
		}
	}
}

func TestBusNilPayload(t *testing.T) {
	b := New()
	ch := b.Subscribe(EventMetadataDiscovered)
	b.Publish(Event{Type: EventMetadataDiscovered, Payload: nil})

	select {
	case evt := <-ch:
		require.Nil(t, evt.Payload)
	case <-time.After(time.Second):
		t.Fatal("event not received")
	}
}

func TestBusSameTypeMultipleSubscribe(t *testing.T) {
	b := New()
	b.Subscribe(EventMetadataDiscovered)
	b.Subscribe(EventMetadataDiscovered)
	b.Publish(Event{Type: EventMetadataDiscovered, Payload: MetadataDiscoveredPayload{Instance: "x"}})
}
