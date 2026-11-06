package eventstream

import (
	"context"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// Sink is the destination to which a [Consumer] dispatches segments of event
// streams. It stores a checkpoint offset for each stream, and resumes from
// that offset when consumption starts again.
//
// The consumer calls the sink's methods sequentially for each stream, but may
// call them concurrently for different streams. Other consumers may consume
// the same stream at the same time. The sink must coordinate checkpoint
// updates with the effects of handling events so that concurrent consumption
// does not apply an event more than once.
type Sink interface {
	// Workload returns the rendezvous workload used to choose which node
	// consumes the stream with the given ID. It must return the same value
	// for a given stream on every node. Streams with the same workload are
	// assigned to the same node.
	Workload(streamID *uuidpb.UUID) *uuidpb.UUID

	// EventTypeIDs returns the IDs of the event types that the sink consumes.
	// The set must not be empty or modified after it is returned. Every type
	// must be one that [Catalog.Watch] was given for the stream's source.
	EventTypeIDs() *uuidcollection.Set

	// CheckpointOffset returns the offset from which consumption of the stream
	// with the given ID resumes. It returns zero for a stream that has not
	// been consumed before.
	//
	// The consumer calls this method at the start of each consumption attempt,
	// including attempts after a failure.
	CheckpointOffset(
		ctx context.Context,
		streamID *uuidpb.UUID,
	) (uint64, error)

	// ConsumeSegment handles the events in the given segment and returns the new
	// checkpoint offset for its stream.
	//
	// checkpoint is the offset returned by CheckpointOffset at the start of
	// the attempt, or by the previous successful call to ConsumeSegment for
	// this stream. It may be lower than segment.Begin if the sink does not
	// store checkpoint updates for events that it does not handle.
	//
	// The returned offset may likewise be lower than segment.End, even
	// when the segment holds no events. The consumer's position in the stream
	// advances to segment.End independently of the sink's checkpoint.
	//
	// If ok is false, another consumer has changed the checkpoint offset.
	// The consumer stops this attempt and reloads the checkpoint before
	// retrying. This is an expected concurrency conflict, not a failure to
	// load or handle an event.
	//
	// If an error is returned, some events may already have been handled and
	// their checkpoint updates stored. The consumer stops this attempt and
	// calls CheckpointOffset again before retrying; it does not use the offset
	// returned alongside the error.
	ConsumeSegment(
		ctx context.Context,
		checkpoint uint64,
		segment Segment,
	) (next uint64, ok bool, err error)
}
