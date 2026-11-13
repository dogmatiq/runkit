package pages

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
)

const (
	// keepAliveInterval is the time between the comments that keep an idle
	// live-update connection open through proxies.
	keepAliveInterval = 15 * time.Second

	// reconnectDelay is how long a page waits before reconnecting after its
	// live-update connection breaks, such as while the server restarts.
	reconnectDelay = 1 * time.Second
)

// liveEvent is a server-sent event that tells open pages that something
// changed.
type liveEvent struct {
	// Name is the event's name. "observed" means that a new observation is
	// ready, and "heartbeat" means that a node sent a heartbeat.
	Name string

	// Data is the event's data. For a heartbeat, it is the node's ID.
	Data string
}

// Broadcaster sends live-update events to every open page.
type Broadcaster struct {
	m    sync.Mutex
	subs map[chan liveEvent]struct{}
}

// Observed tells every open page that a new observation is ready.
func (b *Broadcaster) Observed() {
	b.publish(
		liveEvent{
			Name: "observed",
		},
	)
}

// Heartbeat tells every open page that the node with the given ID sent a
// heartbeat.
func (b *Broadcaster) Heartbeat(nodeID *uuidpb.UUID) {
	b.publish(
		liveEvent{
			Name: "heartbeat",
			Data: nodeID.AsString(),
		},
	)
}

// serve sends live-update events to one open page until it disconnects.
func (b *Broadcaster) serve(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}

	events := b.subscribe()
	defer b.unsubscribe(events)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	// Tell the page how soon to reconnect if the connection breaks, and which
	// version of the pages this server renders. A page that a different
	// version rendered reloads itself.
	if _, err := fmt.Fprintf(
		w,
		"retry: %d\nevent: hello\ndata: %s\n\n",
		reconnectDelay.Milliseconds(),
		pagesVersion,
	); err != nil {
		return
	}

	flusher.Flush()

	keepAlive := time.NewTicker(keepAliveInterval)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case <-keepAlive.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}

		case e := <-events:
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Name, e.Data); err != nil {
				return
			}
		}

		flusher.Flush()
	}
}

func (b *Broadcaster) publish(e liveEvent) {
	b.m.Lock()
	defer b.m.Unlock()

	for events := range b.subs {
		// A page that is not keeping up misses the event. Every event only
		// tells the page to look again, so a later event makes up for it.
		select {
		case events <- e:
		default:
		}
	}
}

func (b *Broadcaster) subscribe() chan liveEvent {
	b.m.Lock()
	defer b.m.Unlock()

	if b.subs == nil {
		b.subs = map[chan liveEvent]struct{}{}
	}

	events := make(chan liveEvent, 64)
	b.subs[events] = struct{}{}

	return events
}

func (b *Broadcaster) unsubscribe(events chan liveEvent) {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.subs, events)
}
