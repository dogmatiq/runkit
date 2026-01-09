package eventstream

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

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
	LowestPossibleOffset uint64

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

	// [BeginOffset, EndOffset) is the half-open range describing the offsets of
	// the appended events within the stream.
	//
	// BeginOffset is the offset of the first event in the [AppendRequest], and
	// EndOffset is the offset after the last event in the [AppendRequest].
	//
	// The values are undefined if Ok is false.
	BeginOffset, EndOffset uint64
}

// validateAppendRequest returns an error if the given request is malformed. Any
// error indicates a bug within the engine.
func validateAppendRequest(req AppendRequest) error {
	if err := req.PartitionID.Validate(); err != nil {
		return xerrors.Bug("AppendRequest.PartitionID is invalid: %w", err)
	}

	if len(req.EventEnvelopes) == 0 {
		return xerrors.Bug("AppendRequest.EventEnvelopes is empty")
	}

	if req.Response == nil {
		return xerrors.Bug("AppendRequest.Response is nil")
	}

	return nil
}
