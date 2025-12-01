package teststate

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"pgregory.net/rapid"
)

// Stream represents the state of a single event stream.
type Stream struct {
	ID         *uuidpb.UUID
	NextOffset uint64
	Events     []*envelopepb.Envelope
	Appends    []eventstream.EventsAppendedNotification
}

// EventsGen returns a generator that produces events from the stream.
func (s *Stream) EventsGen() *rapid.Generator[*envelopepb.Envelope] {
	return rapid.Custom(
		func(t *rapid.T) *envelopepb.Envelope {
			if s.NextOffset == 0 {
				t.Skip("stream is empty")
			}

			return rapid.SampledFrom(s.Events).Draw(t, "event")
		},
	)
}

// OffsetsGen returns a generator that produces offsets of events on the stream.
func (s *Stream) OffsetsGen() *rapid.Generator[uint64] {
	return rapid.Custom(
		func(t *rapid.T) uint64 {
			if s.NextOffset == 0 {
				t.Skip("stream is empty")
			}

			return rapid.Uint64Range(0, s.NextOffset-1).Draw(t, "offset")
		},
	)
}

// func (s *Stream) DrawOffset(t *rapid.T) uint64 {
// 	return rapid.
// 		Uint64Range(0, s.NextOffset()-1).
// 		Draw(t, "offset of existing event")
// }

func (s *Stream) OffsetOf(messageID *uuidpb.UUID) (uint64, bool) {
	for _, op := range s.Appends {
		for i, env := range op.Events {
			if env.MessageId.Equal(messageID) {
				return op.Offset + uint64(i), true
			}
		}
	}

	return 0, false
}

// append updates the stream's state to reflect the occurrence of the events
// described by the given [eventstream.EventsAppendedNotification] notification.
func (s *Stream) append(t *rapid.T, x eventstream.EventsAppendedNotification) {
	for i, env := range x.Events {
		t.Logf(
			"[%s@%d] appended %s",
			x.StreamID,
			x.Offset+uint64(i),
			env.MessageId,
		)
	}

	s.NextOffset += uint64(len(x.Events))
	s.Events = append(s.Events, x.Events...)
	s.Appends = append(s.Appends, x)
}

func (s *Stream) String() string {
	return s.ID.AsString()
}

// GoString returns a string representation of the stream ID. This produces
// friendlier log messages when drawing a stream from [Subsystem.Streams].
func (s *Stream) GoString() string {
	return s.ID.AsString()
}
