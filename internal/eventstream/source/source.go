package source

import (
	"context"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// Source is where an event stream's events come from: this application's
// database, or a remote engine.
type Source interface {
	// Watch delivers events to fn as they are appended to the source's
	// streams.
	//
	// Only events of the given types are delivered, in contiguous segments. A
	// nil set of types means events of every type. It blocks until ctx is
	// canceled or it is unable to continue, and may be called again if it
	// fails.
	//
	// fn may be called concurrently for different streams, but a stream's
	// segments are delivered one at a time, in order.
	//
	// A stream's first segment, or one that does not begin where the previous
	// segment of that stream ended, begins a new delivery of the stream from
	// the segment's begin offset.
	Watch(
		ctx context.Context,
		types *uuidcollection.Set,
		fn func(context.Context, Segment),
	) error

	// Read delivers events from a stream to fn, beginning at the given
	// offset.
	//
	// Only events of the given types are delivered, in contiguous segments,
	// until the end of the stream as it stands, at which point it returns
	// nil. It returns fn's error, unchanged, if fn returns one.
	Read(
		ctx context.Context,
		streamID *uuidpb.UUID,
		types *uuidcollection.Set,
		begin uint64,
		fn func(context.Context, Segment) error,
	) error
}
