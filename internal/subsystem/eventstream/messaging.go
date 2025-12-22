package eventstream

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

// AppendEventsRequest is a request to append events to a specific partition of
// the event stream.
type AppendEventsRequest struct {
	// ID is a unique identifier for this request.
	ID *uuidpb.UUID

	// PartitionID is the UUID of the partition to which the events are
	// appended.
	PartitionID *uuidpb.UUID

	// Events is the set of events to append to the stream partition.
	// If empty, no events are appended.
	Events []*envelopepb.Envelope

	// DeduplicationHint is the lowest offset within the partition at which
	// these events may have already been appended.
	//
	// Any events before this offset are not considered when deduplicating
	// events.
	DeduplicationHint uint64

	// Response is the channel to which the corresponding [AppendEventsResponse]
	// is sent.
	Response chan<- AppendEventsResponse
}

// AppendEventsResponse is the successful result of an [AppendEventsRequest]
// request.
type AppendEventsResponse struct {
	// RequestID is the unique identifier of the corresponding
	// [AppendEventsRequest].
	RequestID *uuidpb.UUID

	// [BeginOffset, EndOffset) is the half-open range describing the offsets of
	// the appended events within the stream.
	//
	// BeginOffset is the offset of the first event in the [AppendRequest], and
	// EndOffset is the offset after the last event in the [AppendRequest].
	//
	// If both offsets are zero, the request was not processed.
	BeginOffset, EndOffset uint64
}

// validateAppendEventsRequest returns an error if the given request is
// malformed. Any error indicates a bug within the engine.
func validateAppendEventsRequest(req AppendEventsRequest) error {
	if err := req.ID.Validate(); err != nil {
		return xerrors.Bug("AppendEventsRequest.ID is invalid: %w", err)
	}

	if err := req.PartitionID.Validate(); err != nil {
		return xerrors.Bug("AppendEventsRequest.PartitionID is invalid: %w", err)
	}

	if len(req.Events) == 0 {
		return xerrors.Bug("AppendEventsRequest.Events is empty")
	}

	if req.Response == nil {
		return xerrors.Bug("AppendEventsRequest.Response is nil")
	}

	return nil
}
