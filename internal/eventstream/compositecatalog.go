package eventstream

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// CompositeCatalog is a [Catalog] that combines other catalogs, which may be
// added and removed while it runs. A catalog's streams are removed along with
// it.
//
// More than one catalog may offer the same stream, in which case it is
// unspecified which of them serves each subscription.
type CompositeCatalog struct {
	streamIDs xsync.Observable[*uuidcollection.Set]
	routes    atomic.Pointer[uuidcollection.Map[*compositeMember]]

	once        sync.Once
	add, remove chan *compositeMember

	done    xsync.Latch
	changed chan struct{}
	members map[*compositeMember]struct{}
}

// compositeMember is a catalog that has been added to a [CompositeCatalog].
type compositeMember struct {
	Catalog Catalog

	// Gone is set when the member is removed.
	Gone xsync.Latch
}

// StreamIDs returns the IDs of the available event streams, and a channel that
// is closed when that set changes.
//
// The returned set must not be modified.
func (c *CompositeCatalog) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return c.streamIDs.Load()
}

// Subscribe registers ch to receive events from a specific stream starting at
// the given checkpoint offset, via the catalog that offers the stream. Events
// are delivered in offset order.
//
// types is the set of message types to deliver. It must not be empty.
//
// lost is closed if the catalog can no longer deliver to the subscription: if
// no catalog offers the stream, or if the catalog that offers it loses the
// subscription. A catalog that is removed from the composite is expected to be
// stopped, at which point its subscriptions are lost.
func (c *CompositeCatalog) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidcollection.Set,
	ch chan<- Batch,
) (lost <-chan struct{}, unsubscribe func()) {
	if m, ok := c.routes.Load().Get(streamID); ok {
		return m.Catalog.Subscribe(streamID, checkpoint, types, ch)
	}

	closed := make(chan struct{})
	close(closed)

	return closed, func() {}
}

// Add makes the event streams offered by cat available from the composite.
//
// It returns a function that removes cat, along with its streams.
//
// It blocks until [CompositeCatalog.Run] has been called. If the composite has
// stopped, it has no effect.
func (c *CompositeCatalog) Add(cat Catalog) (remove func()) {
	c.init()

	m := &compositeMember{
		Catalog: cat,
	}

	select {
	case <-c.done.Chan():
		return func() {}
	case c.add <- m:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			m.Gone.Set()

			select {
			case <-c.done.Chan():
			case c.remove <- m:
			}
		})
	}
}

func (c *CompositeCatalog) init() {
	c.once.Do(func() {
		c.add = make(chan *compositeMember)
		c.remove = make(chan *compositeMember)
	})
}

// Run tracks the available event streams until ctx is canceled.
//
// It must be called at most once.
func (c *CompositeCatalog) Run(ctx context.Context) error {
	c.init()
	defer c.done.Set()

	c.changed = make(chan struct{}, 1)
	c.members = map[*compositeMember]struct{}{}

	for {
		c.publish()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-c.add:
			c.addMember(ctx, m)
		case m := <-c.remove:
			delete(c.members, m)
		case <-c.changed:
		}
	}
}

// addMember adds m and starts watching its catalog for changes, until m is
// removed or ctx is canceled.
func (c *CompositeCatalog) addMember(ctx context.Context, m *compositeMember) {
	c.members[m] = struct{}{}

	// The change channel is always loaded before the publication that follows
	// it: here before the first, and below before each one that a change
	// prompts. A change that lands between loading and publishing is included
	// in the publication, and one that lands after it closes the channel.
	_, changed := m.Catalog.StreamIDs()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.Gone.Chan():
				return
			case <-changed:
			}

			_, changed = m.Catalog.StreamIDs()

			select {
			case c.changed <- struct{}{}:
			default:
			}
		}
	}()
}

// publish publishes the current set of available event streams, if it differs
// from the published set, and the catalog that serves each.
func (c *CompositeCatalog) publish() {
	streamIDs := uuidcollection.NewSet()
	routes := &uuidcollection.Map[*compositeMember]{}

	for m := range c.members {
		ids, _ := m.Catalog.StreamIDs()

		for streamID := range ids.All() {
			if streamIDs.Has(streamID) {
				continue
			}

			streamIDs.Add(streamID)
			routes.Set(streamID, m)
		}
	}

	c.routes.Store(routes)

	if prev, _ := c.streamIDs.Load(); !prev.IsEqual(streamIDs) {
		c.streamIDs.Store(streamIDs)
	}
}
