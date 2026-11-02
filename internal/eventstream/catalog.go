package eventstream

import (
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
)

// Catalog is a set of event streams and the events on them.
type Catalog interface {
	// StreamIDs returns the IDs of the known event streams, and a channel that
	// is closed when that set changes.
	//
	// The returned set must not be modified.
	StreamIDs() (*uuidcollection.Set, <-chan struct{})

	// Subscribe registers ch to receive batches of events of the given types
	// from the stream with the given ID, starting at checkpoint.
	//
	// ch should be buffered. Events are delivered in offset order, and a
	// subscriber that does not keep up never misses events.
	//
	// It returns a function that unsubscribes.
	Subscribe(
		streamID *uuidpb.UUID,
		checkpoint uint64,
		types *uuidcollection.Set,
		ch chan<- Batch,
	) (unsubscribe func())
}

// Batch is a set of events delivered to a subscriber of a [Catalog], along with
// the checkpoint from which the subscriber should resume afterwards.
//
// A batch may contain no events, indicating only that the checkpoint has
// advanced past events that are not of a type the subscriber is interested in.
type Batch = scanner.Batch
