package eventstream

import (
	"context"
	"errors"

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
//
// A sink may wrap [ErrPersistentFailure] for failures that need an external
// change before retrying is likely to succeed, or [ErrFatalFailure] when
// consumption must stop. Other errors are retried after a shorter delay.
type Sink interface {
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

	// ConsumeSegment handles the events in segment and returns the new
	// checkpoint offset for its stream.
	//
	// checkpoint is the offset returned by CheckpointOffset at the start of
	// the attempt, or by the previous successful call to ConsumeSegment for
	// this stream. It may be lower than segment.Begin if the sink does not
	// store checkpoint updates for events that it does not handle.
	//
	// The returned offset may likewise be lower than segment.End, including
	// when the segment holds no events. The consumer's position in the stream
	// advances to segment.End independently of the sink's checkpoint.
	//
	// If an error is returned, some events may already have been handled and
	// their checkpoint updates stored. The consumer stops this attempt and
	// calls CheckpointOffset again before retrying; it does not use the offset
	// returned alongside the error.
	ConsumeSegment(
		ctx context.Context,
		checkpoint uint64,
		segment Segment,
	) (uint64, error)
}

var (
	// ErrPersistentFailure indicates that consumption failed because of a
	// condition that is unlikely to change soon, such as invalid event data.
	// The consumer retries these failures with a longer backoff.
	ErrPersistentFailure = errors.New("persistent consumption failure")

	// ErrFatalFailure indicates that consumption cannot continue. The consumer
	// stops consuming all of its streams and returns the error.
	ErrFatalFailure = errors.New("fatal consumption failure")
)
