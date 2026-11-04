package eventstream

import (
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
)

// LegacyCatalog is a set of event streams and the events on them.
type LegacyCatalog interface {
	// StreamIDs returns the IDs of the known event streams, and a channel that
	// is closed when that set changes.
	//
	// The returned set must not be modified.
	StreamIDs() (*uuidcollection.Set, <-chan struct{})

	// Subscribe registers ch to receive events from a specific stream starting
	// at the given checkpoint offset. Events are delivered in offset order.
	//
	// types is the set of message types to deliver. It must not be empty.
	//
	// lost is closed if the catalog can no longer deliver to the subscription.
	Subscribe(
		streamID *uuidpb.UUID,
		checkpoint uint64,
		types *uuidcollection.Set,
		ch chan<- Batch,
	) (lost <-chan struct{}, unsubscribe func())
}

// Batch is a set of events delivered to a [LegacyCatalog] subscription, along with
// the checkpoint from which to resume afterwards.
//
// A batch may contain no events, indicating only that the checkpoint has
// advanced past events that are not of a subscribed type.
type Batch = scanner.Batch
