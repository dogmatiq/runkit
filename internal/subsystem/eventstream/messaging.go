package eventstream

import (
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Offset is the position of an event within a stream partition. The first event
// in the partition is always at offset zero.
type Offset uint64

// OffsetRange describes a contiguous range of offsets within an event stream
// partition as a half-open interval [begin, end).
type OffsetRange struct {
	// Begin is the offset of the first event in the range.
	Begin Offset

	// End is the offset after the last event in the range.
	End Offset
}

// IsEmpty returns true if the range contains no offsets.
func (r OffsetRange) IsEmpty() bool {
	return r.End <= r.Begin
}

// AppendRequest is a request to append events to a specific partition of the
// application's event stream.
type AppendRequest struct {
	// PartitionID is the UUID of the event stream partition to which the events
	// are appended.
	PartitionID *uuidpb.UUID

	// EventEnvelopes is the set of events to append to the stream partition.
	// If empty, no events are appended.
	EventEnvelopes []*envelopepb.Envelope

	// LowestPossibleOffset is the lowest offset within the partition at which
	// these events may have already been appended.
	//
	// Any events before this offset are not considered when deduplicating
	// events.
	LowestPossibleOffset Offset

	// Response is the channel to which the corresponding [AppendResponse]
	// is sent.
	Response chan<- AppendResponse
}

// AppendResponse is the result of an [AppendRequest].
type AppendResponse struct {
	// FirstEventMessageID is the message ID of the first event in the
	// corresponding [AppendRequest].
	FirstEventMessageID *uuidpb.UUID

	// Ok is true if the events were appended successfully.
	Ok bool

	// Offsets describes the offsets of the appended events within the stream
	// partition. The values are undefined if Ok is false.
	Offsets OffsetRange
}

func validateAppendRequest(req AppendRequest) {
	if err := req.PartitionID.Validate(); err != nil {
		panic(fmt.Sprintf("eventstream.AppendRequest.PartitionID is invalid: %v", err))
	}

	if len(req.EventEnvelopes) == 0 {
		panic("eventstream.AppendRequest.EventEnvelopes is empty")
	}

	for i, env := range req.EventEnvelopes {
		if err := env.Validate(); err != nil {
			panic(fmt.Sprintf("eventstream.AppendRequest.EventEnvelopes[%d] is invalid: %v", i, err))
		}
	}

	if req.Response == nil {
		panic("eventstream.AppendRequest.Response is nil")
	}
}

func validateAppendResponse(res AppendResponse) {
	if err := res.FirstEventMessageID.Validate(); err != nil {
		panic(fmt.Sprintf("eventstream.AppendResponse.FirstEventMessageID is invalid: %v", err))
	}

	if res.Ok && res.Offsets.IsEmpty() {
		panic("eventstream.AppendResponse.Offsets must not be empty if Ok is true")
	}
}
