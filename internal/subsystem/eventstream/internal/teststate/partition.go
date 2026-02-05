package teststate

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xrapid"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"pgregory.net/rapid"
)

// Partition represents the state of a single partition of the event stream.
type Partition struct {
	ID             *uuidpb.UUID
	NextOffset     eventstream.Offset
	AppendRequests []eventstream.AppendRequest
	Events         []*envelopepb.Envelope
}

// EventsGen returns a generator that produces events from this partition.
func (p *Partition) EventsGen(t *rapid.T) *rapid.Generator[*envelopepb.Envelope] {
	if p.NextOffset == 0 {
		t.Skip("stream partition is empty")
	}

	return rapid.SampledFrom(p.Events)
}

// OffsetsGen returns a generator that produces offsets of events on this
// partition.
func (p *Partition) OffsetsGen(t *rapid.T) *rapid.Generator[eventstream.Offset] {
	if p.NextOffset == 0 {
		t.Skip("stream partition is empty")
	}
	return xrapid.Uint64Range(0, p.NextOffset-1)
}

// AppendRequestsGen returns a generator that produces prior successful
// [eventstream.AppendRequest] that target this partition.
func (p *Partition) AppendRequestsGen(t *rapid.T) *rapid.Generator[eventstream.AppendRequest] {
	if len(p.AppendRequests) == 0 {
		t.Skip("stream partition is empty")
	}

	return rapid.SampledFrom(p.AppendRequests)
}

// FindOffset returns the offset of the event with the given message ID,
// or false if the event is not present on this partition.
func (p *Partition) FindOffset(messageID *uuidpb.UUID) (eventstream.Offset, bool) {
	for i, env := range p.Events {
		if env.MessageId.Equal(messageID) {
			return eventstream.Offset(i), true
		}
	}

	return 0, false
}

// FindAppendRequest returns the [eventstream.AppendRequest] that contains the
// event with the given message ID, or false if the event is not present on this
// partition.
func (p *Partition) FindAppendRequest(messageID *uuidpb.UUID) (eventstream.AppendRequest, bool) {
	for _, req := range p.AppendRequests {
		for _, env := range req.EventEnvelopes {
			if env.MessageId.Equal(messageID) {
				return req, true
			}
		}
	}

	return eventstream.AppendRequest{}, false
}

// update updates the partition's state to reflect the occurrence of the events
// described by the given request/response exchange.
func (p *Partition) update(t *rapid.T, req eventstream.AppendRequest, res eventstream.AppendResponse) {
	if res.Offsets.Begin < p.NextOffset {
		// This is a response to a retried request.
		return
	}

	if res.Offsets.Begin > p.NextOffset {
		t.Fatalf(
			"cannot append to partition %s at offset %d, partition is at offset %d",
			p.ID,
			res.Offsets.Begin,
			p.NextOffset,
		)
	}

	for i, env := range req.EventEnvelopes {
		t.Logf(
			"appended %s to partition %s at offset %d",
			env.MessageId,
			p.ID,
			res.Offsets.Begin+eventstream.Offset(i),
		)
	}

	p.NextOffset += eventstream.Offset(len(req.EventEnvelopes))
	p.AppendRequests = append(p.AppendRequests, req)
	p.Events = append(p.Events, req.EventEnvelopes...)
}

func (p *Partition) String() string {
	return p.ID.AsString()
}

// GoString returns a string representation of the partition ID. This produces
// friendlier log messages when drawing a partition from [EventStream.Partitions].
func (p *Partition) GoString() string {
	return p.ID.AsString()
}
