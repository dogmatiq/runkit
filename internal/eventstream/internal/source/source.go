package source

import (
	"context"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// Source is where an event stream's events come from: this application's
// database, or a remote engine.
type Source interface {
	// Watch delivers the events of the given types from each of the source's
	// streams to fn in contiguous segments as they are appended to the
	// stream. A nil set of types means events of every type.
	//
	// It blocks until ctx is canceled or it is unable to continue. It may be
	// called again if it fails.
	//
	// A stream's first segment, or one that does not begin where the previous
	// segment of that stream ended, begins a new delivery of the stream from
	// the segment's begin offset.
	Watch(
		ctx context.Context,
		types *uuidcollection.Set,
		fn func(context.Context, Segment),
	) error

	// Read delivers the events of the given types from the given stream to
	// fn, in contiguous segments beginning at the given offset, until it
	// reaches the end of the stream as it stands, then returns nil.
	//
	// It returns fn's error, unchanged, if fn returns one.
	Read(
		ctx context.Context,
		streamID *uuidpb.UUID,
		types *uuidcollection.Set,
		begin uint64,
		fn func(context.Context, Segment) error,
	) error
}
