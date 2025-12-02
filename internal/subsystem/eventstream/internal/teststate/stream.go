package teststate

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"pgregory.net/rapid"
)

// Stream represents the state of a single event stream.
type Stream struct {
	ID            *uuidpb.UUID
	NextOffset    uint64
	Events        []*envelopepb.Envelope
	Notifications []eventstream.EventsAppendedNotification
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

// NotificationsGen returns a generator that produces
// [eventstream.EventsAppendedNotification] that pertain to the stream.
func (s *Stream) NotificationsGen(t *rapid.T) *rapid.Generator[eventstream.EventsAppendedNotification] {
	if len(s.Notifications) == 0 {
		t.Skip("stream is empty")
	}

	return rapid.SampledFrom(s.Notifications)
}

// FindOffset returns the offset of the event with the given message ID,
// or false if the event is not present on the stream.
func (s *Stream) FindOffset(messageID *uuidpb.UUID) (uint64, bool) {
	for _, op := range s.Notifications {
		for i, env := range op.Events {
			if env.MessageId.Equal(messageID) {
				return op.Offset + uint64(i), true
			}
		}
	}

	return 0, false
}

// FindNotification returns the [eventstream.EventsAppendedNotification] that
// contains the event with the given message ID, or false if the event is not
// present on the stream.
func (s *Stream) FindNotification(messageID *uuidpb.UUID) (eventstream.EventsAppendedNotification, bool) {
	for _, op := range s.Notifications {
		for _, env := range op.Events {
			if env.MessageId.Equal(messageID) {
				return op, true
			}
		}
	}

	return eventstream.EventsAppendedNotification{}, false
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
	s.Notifications = append(s.Notifications, x)
}

func (s *Stream) String() string {
	return s.ID.AsString()
}

// GoString returns a string representation of the stream ID. This produces
// friendlier log messages when drawing a stream from [Subsystem.Streams].
func (s *Stream) GoString() string {
	return s.ID.AsString()
}
