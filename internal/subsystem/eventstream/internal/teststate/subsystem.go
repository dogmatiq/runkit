package teststate

import (
	"context"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// Subsystem represents the Subsystem of the eventstream system within the a
// test.
type Subsystem struct {
	// Context is the context in which the subsystem is running.
	Context context.Context

	// Journals is the in-memory journal store used to persist event streams.
	Journals memoryjournal.BinaryStore

	// Streams is the set of event streams known to the subsystem. This
	// represents the _expected_ state of the streams based on prior
	// observations.
	Streams maps.Proto[*uuidpb.UUID, *Stream]

	// Requests is used to send [eventstream.AppendEventsRequest] requests to a
	// supervisor.
	Requests chan<- eventstream.AppendEventsRequest

	// Notifications is used to receive [eventstream.EventsAppendedNotification]
	// notifications from supervisors.
	Notifications <-chan eventstream.EventsAppendedNotification
}

// StreamsGen returns a generator that yields existing streams.
func (s *Subsystem) StreamsGen(t *rapid.T) *rapid.Generator[*Stream] {
	if s.Streams.Len() == 0 {
		t.Skip("stream is empty")
	}

	return xrapid.SampledFromSeq(s.Streams.Values())
}

// SendAppendEventsRequest sends an [eventstream.AppendEventsRequest] request to the
// supervisor.
func (s *Subsystem) SendAppendEventsRequest(t *rapid.T, req eventstream.AppendEventsRequest) {
	select {
	case <-s.Context.Done():
		t.Fatalf("[%s] context cancelled while sending AppendEventsRequest: %s", req.StreamID, context.Cause(s.Context))
	case s.Requests <- req:
		t.Logf("[%s] sent AppendEventsRequest request", req.StreamID)
	}
}

// ExpectAppendEventsResponse waits for and verifies an
// [eventstream.AppendEventsResponse] from the supervisor.
func (s *Subsystem) ExpectAppendEventsResponse(
	t *rapid.T,
	req eventstream.AppendEventsRequest,
	res <-chan eventstream.AppendEventsResponse,
	want eventstream.AppendEventsResponse,
) {
	if res == nil {
		panic("test misuse: response channel is nil")
	}

	select {
	case <-s.Context.Done():
		t.Fatalf("[%s] context cancelled while waiting for AppendEventsResponse: %s", req.StreamID, context.Cause(s.Context))
	case got, ok := <-res:
		if !ok {
			t.Fatalf("[%s] AppendEventsRequest was rejected", req.StreamID)
		}

		t.Logf("[%s] received AppendEventsResponse", req.StreamID)

		if got.BeginOffset != want.BeginOffset || got.EndOffset != want.EndOffset {
			t.Fatalf(
				"[%s] AppendEventsResponse has unexpected offset range: got [%d, %d), want [%d, %d)",
				req.StreamID,
				got.BeginOffset,
				got.EndOffset,
				want.BeginOffset,
				want.EndOffset,
			)
		}

		if got.Deduplicated != want.Deduplicated {
			t.Fatalf(
				"[%s] AppendEventsResponse has unexpected deduplication flag: got %t, want %t",
				req.StreamID,
				got.Deduplicated,
				want.Deduplicated,
			)
		}
	}
}

// ExpectEventsAppendedNotification waits for and verifies an [eventstream.EventsAppendedNotification]
// notification from the supervisor.
func (s *Subsystem) ExpectEventsAppendedNotification(t *rapid.T, want eventstream.EventsAppendedNotification) {
	select {
	case <-s.Context.Done():
		t.Fatalf("[%s] context cancelled while waiting for EventsAppendedNotification: %s", want.StreamID, context.Cause(s.Context))
	case got, ok := <-s.Notifications:
		if !ok {
			t.Fatalf("[%s] Notifications channel was closed", want.StreamID)
		}

		t.Logf("[%s] received EventsAppendedNotification", want.StreamID)

		if !got.StreamID.Equal(want.StreamID) {
			t.Fatalf(
				"[%s] EventsAppendedNotification has unexpected stream ID: got %s, want %s",
				want.StreamID,
				got.StreamID,
				want.StreamID,
			)
		}

		if got.Offset != want.Offset {
			t.Fatalf(
				"[%s] EventsAppendedNotification has unexpected offset: got %d, want %d",
				want.StreamID,
				got.Offset,
				want.Offset,
			)
		}

		if len(got.Events) != len(want.Events) {
			t.Fatalf(
				"[%s] EventsAppendedNotification has unexpected number of events: got %d, want %d",
				want.StreamID,
				len(got.Events),
				len(want.Events),
			)
		}

		for idx, gotEvent := range got.Events {
			wantEvent := want.Events[idx]

			if !proto.Equal(gotEvent, wantEvent) {
				t.Fatalf(
					"[%s] EventsAppendedNotification has unexpected event at index %d:\ngot %s\nwant %s",
					want.StreamID,
					idx,
					dapper.Format(gotEvent),
					dapper.Format(wantEvent),
				)
			}
		}

		stream, ok := s.Streams.TryGet(got.StreamID)

		if !ok {
			stream = &Stream{
				ID: got.StreamID,
			}
			s.Streams.Set(got.StreamID, stream)
		}

		stream.append(t, got)
	}
}

// ExpectNoEventsAppendedNotification verifies that no [eventstream.EventsAppendedNotification]
// notification has been published for the given stream.
func (s *Subsystem) ExpectNoEventsAppendedNotification(t *rapid.T, streamID *uuidpb.UUID) {
	select {
	case <-s.Notifications:
		t.Fatalf("[%s] unexpected EventsAppendedNotification received", streamID)
	default:
	}
}
