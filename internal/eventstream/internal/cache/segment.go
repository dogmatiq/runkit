package cache

import (
	"sort"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
)

// Segment is the segment of the stream that a [Cache] holds, as an immutable
// snapshot.
type Segment struct {
	// Begin and End bound the segment, [Begin, End).
	Begin, End uint64

	// events are the events held, in offset order. Every event is within the
	// segment.
	events []*envelopepb.Envelope
}

// Relation describes where a checkpoint lies in relation to a [Segment].
//
// Its values follow the convention of [cmp.Compare]: negative if the checkpoint
// is before the segment, zero if it is within it, and positive if it is after
// it.
type Relation int

const (
	// Before means that the checkpoint is before the segment, so the events
	// from the checkpoint must be read from history.
	Before Relation = -1

	// Within means that the checkpoint is inside the segment, so the segment
	// holds the events from the checkpoint.
	Within Relation = 0

	// After means that the checkpoint is at or beyond the end of the segment,
	// so there is nothing to deliver until the cache is appended to past it.
	After Relation = +1
)

// Read returns the events in the segment from the given checkpoint.
//
// Only events of the given types are returned. It also returns where the
// checkpoint lies in relation to the segment, and the events are meaningful
// only if the checkpoint is [Within] it.
func (s Segment) Read(
	cp uint64,
	types *uuidcollection.Set,
) ([]*envelopepb.Envelope, Relation) {
	switch {
	case cp < s.Begin:
		return nil, Before
	case cp >= s.End:
		return nil, After
	}

	first := sort.Search(
		len(s.events),
		func(i int) bool {
			return offsetOf(s.events[i]) >= cp
		},
	)

	var envelopes []*envelopepb.Envelope

	for _, env := range s.events[first:] {
		if types.Has(env.GetBody().GetMessage().GetTypeId()) {
			envelopes = append(envelopes, env)
		}
	}

	return envelopes, Within
}
