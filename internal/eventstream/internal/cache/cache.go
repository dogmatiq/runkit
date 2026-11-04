package cache

import (
	"fmt"

	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/envelopepb"
)

// Cache holds the most recent events from one event stream that match a known
// set of message types.
//
// The cache holds a contiguous segment of the stream, [begin, end), within
// which it holds every event of those types and no others, so the events it
// holds may have gaps between their offsets. The cache's segment ends where
// the last events appended to the cache ended, and begins after the last
// event that the cache evicted.
//
// A cache has one writer, which resets it with [Cache.Reset] and appends to it
// with [Cache.Append], and any number of readers, which observe its segment
// through [Cache.Segment].
type Cache struct {
	capacity int
	segment  xsync.Observable[Segment]
}

// New returns an empty cache. It holds at most the given number of events, and
// its segment begins and ends at the given offset.
func New(capacity int, begin uint64) *Cache {
	if capacity <= 0 {
		panic("capacity must be positive")
	}

	c := &Cache{
		capacity: capacity,
	}

	c.Reset(begin)

	return c
}

// Segment returns an observable of the segment that the cache holds.
func (c *Cache) Segment() xsync.ObservableR[Segment] {
	return &c.segment
}

// Reset empties the cache, so that its segment begins and ends at the given
// offset.
func (c *Cache) Reset(begin uint64) {
	c.segment.Store(Segment{
		Begin: begin,
		End:   begin,
	})
}

// Append adds the events in the range [begin, end) of the stream to the
// cache.
//
// The range must be contiguous with the cache's segment, so the given begin
// offset must be the offset at which that segment currently ends. The segment
// then extends to the given end offset.
//
// If the cache is then over capacity, it evicts its oldest events, and its
// segment begins after the last of them.
//
// Each envelope must have the [envelopepb.EventStreamPosition] extension set,
// and the envelopes must be in offset order.
func (c *Cache) Append(begin, end uint64, envelopes []*envelopepb.Envelope) {
	prev, _ := c.segment.Load()

	if begin != prev.End {
		panic(fmt.Sprintf(
			"range begins at offset %d, but the cache ends at offset %d",
			begin,
			prev.End,
		))
	}

	next := Segment{
		Begin: prev.Begin,
		End:   end,
	}

	if len(envelopes) == 0 {
		// Nothing is evicted, so the events held are shared with the previous
		// snapshot rather than copied; snapshots are never modified.
		next.events = prev.events
	} else if evict := len(envelopes) - c.capacity; evict > 0 {
		// The new events alone exceed the capacity, so every event held is
		// evicted, along with the oldest of the new events.
		next.Begin = offsetOf(envelopes[evict-1]) + 1
		next.events = make([]*envelopepb.Envelope, 0, c.capacity)
		envelopes = envelopes[evict:]
	} else if evict := len(prev.events) + len(envelopes) - c.capacity; evict > 0 {
		// The oldest of the events held are evicted to make room.
		next.Begin = offsetOf(prev.events[evict-1]) + 1
		next.events = make([]*envelopepb.Envelope, 0, c.capacity)
		next.events = append(next.events, prev.events[evict:]...)
	} else {
		// There is enough room to hold all of the new events along with the
		// events already held, so nothing needs to be evicted.
		next.events = make([]*envelopepb.Envelope, 0, len(prev.events)+len(envelopes))
		next.events = append(next.events, prev.events...)
	}

	next.events = append(next.events, envelopes...)

	c.segment.Store(next)
}

// offsetOf returns the offset of the event in env.
func offsetOf(env *envelopepb.Envelope) uint64 {
	return envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset()
}
