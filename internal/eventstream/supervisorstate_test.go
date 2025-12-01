package eventstream_test

import (
	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// state represents the state of the eventstream system within the a test.
type state struct {
	Journals memoryjournal.BinaryStore
	Streams  maps.Proto[*uuidpb.UUID, *streamState]

	Shutdown       chan struct{}
	AppendEvents   chan AppendEvents
	EventsAppended chan EventsAppended
}

func (s *state) drawExistingStream(t *rapid.T) *streamState {
	if s.Streams.Len() == 0 {
		t.Skip("there are no existing streams")
	}

	return xrapid.
		SampledFromValuesOf(&s.Streams).
		Draw(t, "existing stream")
}

func (s *state) sendAppendEvents(t *rapid.T, req AppendEvents, want AppendEventsReply) {
	if req.Reply != nil {
		panic("test misuse: don't set AppendEvents.Reply")
	}

	ch := make(chan AppendEventsReply, 1)
	req.Reply = ch

	select {
	case <-t.Context().Done():
		t.Fatalf("[%s] context canceled while waiting to send AppendEvents request: %s", req.StreamID, t.Context().Err())

	case s.AppendEvents <- req:
		t.Logf("[%s] sent AppendEvents request", req.StreamID)
	}

	select {
	case <-t.Context().Done():
		t.Fatalf("[%s] context canceled while waiting for AppendEventsReply: %s", req.StreamID, t.Context().Err())

	case got, ok := <-ch:
		if ok {
			t.Logf("[%s] received AppendEventsReply", req.StreamID)
		} else {
			t.Logf("[%s] AppendEventsReply request was rejected", req.StreamID)
		}

		if got.BeginOffset != want.BeginOffset || got.EndOffset != want.EndOffset {
			t.Fatalf(
				"[%s] AppendEventsReply has unexpected offset range: got [%d, %d), want [%d, %d)",
				req.StreamID,
				got.BeginOffset,
				got.EndOffset,
				want.BeginOffset,
				want.EndOffset,
			)
		}

		if got.Deduplicated != want.Deduplicated {
			t.Fatalf(
				"[%s] AppendEventsReply has unexpected deduplication flag: got %t, want %t",
				req.StreamID,
				got.Deduplicated,
				want.Deduplicated,
			)
		}
	}
}

func (s *state) awaitEventsAppended(t *rapid.T, want EventsAppended) {
	select {
	case <-t.Context().Done():
		t.Fatalf("[%s] context canceled while waiting for EventsAppended notification: %s", want.StreamID, t.Context().Err())

	case got := <-s.EventsAppended:
		t.Logf("[%s] received EventsAppended notification", want.StreamID)

		if !got.StreamID.Equal(want.StreamID) {
			t.Fatalf(
				"[%s] EventsAppended has unexpected stream ID: got %s, want %s",
				want.StreamID,
				got.StreamID,
				want.StreamID,
			)
		}

		if got.Offset != want.Offset {
			t.Fatalf(
				"[%s] EventsAppended has unexpected offset: got %d, want %d",
				want.StreamID,
				got.Offset,
				want.Offset,
			)
		}

		if len(got.Events) != len(want.Events) {
			t.Fatalf(
				"[%s] EventsAppended has unexpected number of events: got %d, want %d",
				want.StreamID,
				len(got.Events),
				len(want.Events),
			)
		}

		for idx, gotEvent := range got.Events {
			wantEvent := want.Events[idx]

			if !proto.Equal(gotEvent, wantEvent) {
				t.Fatalf(
					"[%s] EventsAppended has unexpected event at index %d:\ngot %s\nwant %s",
					want.StreamID,
					idx,
					dapper.Format(gotEvent),
					dapper.Format(wantEvent),
				)
			}
		}

		stream, ok := s.Streams.TryGet(got.StreamID)

		if !ok {
			stream = &streamState{
				ID: got.StreamID,
			}
			s.Streams.Set(got.StreamID, stream)
		}

		stream.Append(t, got)
	}
}

func (s *state) ensureNoEventsAppended(t *rapid.T, streamID *uuidpb.UUID) {
	select {
	case <-s.EventsAppended:
		t.Fatalf("[%s] unexpected EventsAppended notification received", streamID)
	default:
	}
}

// streamState represents the state of a single event stream within a
// [subsystemState].
type streamState struct {
	ID      *uuidpb.UUID
	Events  []*envelopepb.Envelope
	Appends []EventsAppended
}

func (s *streamState) DrawOffset(t *rapid.T) uint64 {
	return rapid.
		Uint64Range(0, s.NextOffset()-1).
		Draw(t, "offset of existing event")
}

func (s *streamState) NextOffset() uint64 {
	if len(s.Appends) == 0 {
		return 0
	}

	last := s.Appends[len(s.Appends)-1]
	return last.Offset + uint64(len(last.Events))
}

func (s *streamState) OffsetOf(messageID *uuidpb.UUID) (uint64, bool) {
	for _, op := range s.Appends {
		for i, env := range op.Events {
			if env.MessageId.Equal(messageID) {
				return op.Offset + uint64(i), true
			}
		}
	}

	return 0, false
}

func (s *streamState) Append(t *rapid.T, x EventsAppended) {
	for i, env := range x.Events {
		t.Logf(
			"[%s] appended event @%d: %s",
			x.StreamID,
			x.Offset+uint64(i),
			env.MessageId,
		)
	}

	s.Appends = append(s.Appends, x)
	s.Events = append(s.Events, x.Events...)
}

func (s *streamState) String() string {
	return s.ID.AsString()
}

// GoString returns a string representation of the stream ID. This produces
// friendlier log messages when drawing a stream from the [subsystemState].
func (s *streamState) GoString() string {
	return s.ID.AsString()
}
