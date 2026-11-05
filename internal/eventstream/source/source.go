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
	// Only events of the given types are delivered. A nil set of types means
	// events of every type. It panics if the set is empty.
	//
	// Each stream's delivery begins at the stream's end, with a first segment
	// that may be empty. Every later segment of the stream begins where the
	// previous one ended, for as long as the call lasts. A later call may begin
	// a stream after events that this call never delivered; a caller that needs
	// them reads them with [Source.Read].
	//
	// fn may be called concurrently for different streams, but a stream's
	// segments are delivered one at a time, in order.
	//
	// It blocks until ctx is canceled or it is unable to continue, and may be
	// called again once it returns. It returns fn's error, unchanged, if fn
	// returns one, and a [ConsumerTooSlowError] if the caller can not keep up.
	Watch(
		ctx context.Context,
		types *uuidcollection.Set,
		fn func(Segment) error,
	) error

	// Read delivers events from a stream to fn, beginning at the given
	// offset.
	//
	// Only events of the given types are delivered, in contiguous segments,
	// until the end of the stream as it stands, at which point it returns
	// nil. A nil set of types means events of every type. It panics if the
	// set is empty. It returns fn's error, unchanged, if fn returns one.
	Read(
		ctx context.Context,
		streamID *uuidpb.UUID,
		types *uuidcollection.Set,
		begin uint64,
		fn func(Segment) error,
	) error
}

// panicIfEmpty panics if types is an empty set. A nil set means every type, but
// an empty set matches none, and an empty filter is invalid on the wire.
func panicIfEmpty(types *uuidcollection.Set) {
	if types != nil && types.IsEmpty() {
		panic("message types must not be empty")
	}
}
