package teststate

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"pgregory.net/rapid"
)

// Stream represents the state of a single event stream.
type Stream struct {
	ID                   *uuidpb.UUID
	NextOffset           uint64
	AppendEventsRequests []eventstream.AppendEventsRequest
	Events               []*envelopepb.Envelope
}

// EventsGen returns a generator that produces events from the stream.
func (s *Stream) EventsGen(t *rapid.T) *rapid.Generator[*envelopepb.Envelope] {
	if s.NextOffset == 0 {
		t.Skip("stream is empty")
	}

	return rapid.SampledFrom(s.Events)
}

// OffsetsGen returns a generator that produces offsets of events on the stream.
func (s *Stream) OffsetsGen(t *rapid.T) *rapid.Generator[uint64] {
	if s.NextOffset == 0 {
		t.Skip("stream is empty")
	}

	return rapid.Uint64Range(0, s.NextOffset-1)
}

// AppendEventsRequestsGen returns a generator that produces prior successful
// [eventstream.AppendEventsRequest] that target this stream.
func (s *Stream) AppendEventsRequestsGen(t *rapid.T) *rapid.Generator[eventstream.AppendEventsRequest] {
	if len(s.AppendEventsRequests) == 0 {
		t.Skip("stream is empty")
	}

	return rapid.SampledFrom(s.AppendEventsRequests)
}

// FindOffset returns the offset of the event with the given message ID,
// or false if the event is not present on the stream.
func (s *Stream) FindOffset(messageID *uuidpb.UUID) (uint64, bool) {
	for i, env := range s.Events {
		if env.MessageId.Equal(messageID) {
			return uint64(i), true
		}
	}

	return 0, false
}

// FindAppendEventsRequest returns the [eventstream.AppendEventsRequest] that
// contains the event with the given message ID, or false if the event is not
// present on the stream.
func (s *Stream) FindAppendEventsRequest(messageID *uuidpb.UUID) (eventstream.AppendEventsRequest, bool) {
	for _, req := range s.AppendEventsRequests {
		for _, env := range req.Events {
			if env.MessageId.Equal(messageID) {
				return req, true
			}
		}
	}

	return eventstream.AppendEventsRequest{}, false
}

// append updates the stream's state to reflect the occurrence of the events
// described by the given request/response exchange.
func (s *Stream) append(t *rapid.T, req eventstream.AppendEventsRequest, res eventstream.AppendEventsResponse) {
	if res.BeginOffset < s.NextOffset {
		// This is a response to a retried request.
		// We assume at this point that it's already been validated as expected.
		return
	}

	if res.BeginOffset > s.NextOffset {
		t.Fatalf(
			"[%s] cannot append @%d, stream is @%d",
			s.ID,
			res.BeginOffset,
			s.NextOffset,
		)
	}

	for i, env := range req.Events {
		t.Logf(
			"[%s] appended %s @%d",
			s.ID,
			env.MessageId,
			res.BeginOffset+uint64(i),
		)
	}

	s.NextOffset += uint64(len(req.Events))
	s.AppendEventsRequests = append(s.AppendEventsRequests, req)
	s.Events = append(s.Events, req.Events...)
}

func (s *Stream) String() string {
	return s.ID.AsString()
}

// GoString returns a string representation of the stream ID. This produces
// friendlier log messages when drawing a stream from [Subsystem.Streams].
func (s *Stream) GoString() string {
	return s.ID.AsString()
}
