package teststate

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"pgregory.net/rapid"
)

// Partition represents the state of a single partition of the event stream.
type Partition struct {
	ID                   *uuidpb.UUID
	NextOffset           uint64
	AppendEventsRequests []eventstream.AppendEventsRequest
	Events               []*envelopepb.Envelope
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
func (p *Partition) OffsetsGen(t *rapid.T) *rapid.Generator[uint64] {
	if p.NextOffset == 0 {
		t.Skip("stream partition is empty")
	}

	return rapid.Uint64Range(0, p.NextOffset-1)
}

// AppendEventsRequestsGen returns a generator that produces prior successful
// [eventstream.AppendEventsRequest] that target this partition.
func (p *Partition) AppendEventsRequestsGen(t *rapid.T) *rapid.Generator[eventstream.AppendEventsRequest] {
	if len(p.AppendEventsRequests) == 0 {
		t.Skip("stream partition is empty")
	}

	return rapid.SampledFrom(p.AppendEventsRequests)
}

// FindOffset returns the offset of the event with the given message ID,
// or false if the event is not present on this partition.
func (p *Partition) FindOffset(messageID *uuidpb.UUID) (uint64, bool) {
	for i, env := range p.Events {
		if env.MessageId.Equal(messageID) {
			return uint64(i), true
		}
	}

	return 0, false
}

// FindAppendEventsRequest returns the [eventstream.AppendEventsRequest] that
// contains the event with the given message ID, or false if the event is not
// present on this partition.
func (p *Partition) FindAppendEventsRequest(messageID *uuidpb.UUID) (eventstream.AppendEventsRequest, bool) {
	for _, req := range p.AppendEventsRequests {
		for _, env := range req.EventEnvelopes {
			if env.MessageId.Equal(messageID) {
				return req, true
			}
		}
	}

	return eventstream.AppendEventsRequest{}, false
}

// append updates the partition's state to reflect the occurrence of the events
// described by the given request/response exchange.
func (p *Partition) append(t *rapid.T, req eventstream.AppendEventsRequest, res eventstream.AppendEventsResponse) {
	if res.BeginOffset < p.NextOffset {
		// This is a response to a retried request.
		// We assume at this point that it's already been validated as expected.
		return
	}

	if res.BeginOffset > p.NextOffset {
		t.Fatalf(
			"cannot append to partition %s at offset %d, partition is at offset %d",
			p.ID,
			res.BeginOffset,
			p.NextOffset,
		)
	}

	for i, env := range req.EventEnvelopes {
		t.Logf(
			"appended %s to partition %s at offset %d",
			env.MessageId,
			p.ID,
			res.BeginOffset+uint64(i),
		)
	}

	p.NextOffset += uint64(len(req.EventEnvelopes))
	p.AppendEventsRequests = append(p.AppendEventsRequests, req)
	p.Events = append(p.Events, req.EventEnvelopes...)
}

func (p *Partition) String() string {
	return p.ID.AsString()
}

// GoString returns a string representation of the partition ID. This produces
// friendlier log messages when drawing a partition from [Subsystem.Partitions].
func (p *Partition) GoString() string {
	return p.ID.AsString()
}
