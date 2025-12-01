package eventstream

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// EventsAppendedNotification is a notification sent when events have been
// appended to a stream.
type EventsAppendedNotification struct {
	// StreamID is the ID of the event stream to which the events were appended.
	StreamID *uuidpb.UUID

	// Offset is the offset within the stream at which the first event was
	// appended.
	Offset uint64

	// Events is the set of events that were appended.
	Events []*envelopepb.Envelope
}

// AppendEventsRequest is a request to append events to an event stream.
type AppendEventsRequest struct {
	// StreamID is the ID of the event stream to which the events are appended.
	StreamID *uuidpb.UUID

	// Events is the set of events to append to the stream.
	// If empty, no events are appended.
	Events []*envelopepb.Envelope

	// DeduplicationHint is the lowest offset within the stream at which these
	// events may have already been appended. The stream may ignore any events
	// before this offset when deduplicating the events.
	DeduplicationHint uint64

	// Response is the channel to which the [AppendEventsResponse] is sent.
	// It is closed if the request is rejected.
	Response chan<- AppendEventsResponse
}

// AppendEventsResponse is the successful result of an [AppendEventsRequest]
// request.
type AppendEventsResponse struct {
	// [BeginOffset, EndOffset) is the half-open range describing the offsets of
	// the appended events within the stream.
	//
	// BeginOffset is the offset of the first event in the [AppendRequest], and
	// EndOffset is the offset after the last event in the [AppendRequest].
	//
	// If the [AppendRequest] contained no events, then BeginOffset and EndOffset
	// are both set to the offset at which the next appended event would be
	// written.
	BeginOffset, EndOffset uint64

	// Deduplicated is true if the events were appended by a prior
	// [AppendRequest] and hence deduplicated.
	Deduplicated bool
}
