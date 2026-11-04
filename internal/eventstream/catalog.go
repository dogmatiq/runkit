package eventstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/cache"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/supervisor"
)

// ErrStreamNotFound is returned by [Catalog.Consume] when the requested stream
// is not in the catalog.
var ErrStreamNotFound = errors.New("event stream not found")

// A Catalog is an index of all event streams known to the engine, regardless of
// whether they belong to this engine or a remote one.
type Catalog struct {
	// CacheSize is the number of each stream's most recent events to cache,
	// per source. It must be positive.
	CacheSize int

	// Logger is the target for log messages produced by the catalog and the
	// sources added to it.
	Logger *slog.Logger

	m         sync.Mutex
	streams   uuidcollection.Map[*catalogStream]
	streamIDs xsync.Observable[*uuidcollection.Set]
}

// catalogStream is an event stream known to a [Catalog].
type catalogStream struct {
	// AvailableFeeds contains the feeds of this stream, one from each source
	// that provides it.
	//
	// It is guarded by [Catalog.m].
	AvailableFeeds map[*feed]struct{}

	// CurrentFeed is the feed that consumers are served from, or nil if there
	// are none. It must be one of the available feeds.
	CurrentFeed xsync.Observable[*feed]
}

// StreamIDs returns an observable of the IDs of the streams in the catalog.
//
// The sets it holds must not be modified.
func (c *Catalog) StreamIDs() xsync.ObservableR[*uuidcollection.Set] {
	return &c.streamIDs
}

// Consume delivers events from a stream to fn, beginning at the given offset.
//
// Only events of the given types are delivered, in contiguous segments, as
// soon as they are available. The offset is typically the consumer's
// checkpoint offset. It blocks until ctx is canceled, fn returns an error, a
// read from the stream's source fails, or the catalog no longer has the
// stream, in which case it returns [ErrStreamNotFound].
//
// A consumer that keeps up with the stream is served from a cache of its most
// recent events, and one that falls behind is served by reading from the
// source until it catches up again.
//
// types must not be empty, and every type must be one that the stream's
// source is watched for.
func (c *Catalog) Consume(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(Segment) error,
) error {
	if types.IsEmpty() {
		panic("message types must not be empty")
	}

	c.m.Lock()
	cs, ok := c.streams.Get(streamID)
	c.m.Unlock()

	if !ok {
		return ErrStreamNotFound
	}

	cp := begin

	for {
		f, _ := cs.CurrentFeed.Load()
		if f == nil {
			return ErrStreamNotFound
		}

		if f.Types != nil {
			if unwatched := types.Diff(f.Types); !unwatched.IsEmpty() {
				panic(fmt.Sprintf(
					"event stream %s is not watched for message types %s",
					streamID,
					unwatched,
				))
			}
		}

		var err error

		cp, err = f.consume(ctx, types, cp, fn)
		if !errors.Is(err, errFeedClosed) {
			return err
		}
	}
}

// Watch watches a source until ctx is canceled.
//
// The streams that the source delivers are added to the catalog, and are
// removed again when Watch returns.
//
// types is the set of message types that the source is watched for, or nil to
// watch it for every type.
func (c *Catalog) Watch(
	ctx context.Context,
	src source.Source,
	types *uuidcollection.Set,
) error {
	if c.CacheSize <= 0 {
		panic("cache size must be positive")
	}

	var (
		m     sync.Mutex
		feeds uuidcollection.Map[*feed]
	)

	// feedFor returns the feed of the given stream from the source, creating
	// it from the given offset if the source has not delivered the stream
	// before.
	feedFor := func(streamID *uuidpb.UUID, begin uint64) *feed {
		m.Lock()
		defer m.Unlock()

		f, ok := feeds.Get(streamID)
		if !ok {
			f = c.add(streamID, src, types, begin)
			feeds.Set(streamID, f)
		}

		return f
	}

	defer func() {
		for f := range feeds.Values() {
			c.remove(f)
		}
	}()

	return supervisor.Run(
		ctx,
		c.Logger,
		func(ctx context.Context) error {
			err := src.Watch(
				ctx,
				types,
				func(seg Segment) error {
					feedFor(seg.StreamID, seg.Begin).Append(seg)
					return nil
				},
			)

			if err == nil && ctx.Err() == nil {
				return errors.New("source stopped watching before the context was canceled")
			}

			return err
		},
	)
}

// add adds a feed of the given stream from the given source, beginning at the
// given offset, and returns it.
func (c *Catalog) add(
	id *uuidpb.UUID,
	src source.Source,
	types *uuidcollection.Set,
	begin uint64,
) *feed {
	c.m.Lock()
	defer c.m.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	f := &feed{
		ID:     id,
		Source: src,
		Types:  types,
		cache:  cache.New(c.CacheSize, begin),
		ctx:    ctx,
		cancel: cancel,
	}

	cs, ok := c.streams.Get(id)
	if !ok {
		cs = &catalogStream{
			AvailableFeeds: map[*feed]struct{}{},
		}
		c.streams.Set(id, cs)
	}

	cs.AvailableFeeds[f] = struct{}{}

	if len(cs.AvailableFeeds) == 1 {
		cs.CurrentFeed.Store(f)
		c.publish(id, true)
	}

	return f
}

// remove removes a feed from the catalog. If consumers were served from it,
// another feed of the same event stream takes over, and if there is none, the
// event stream leaves the catalog.
func (c *Catalog) remove(f *feed) {
	c.m.Lock()
	defer c.m.Unlock()

	cs, _ := c.streams.Get(f.ID)

	delete(cs.AvailableFeeds, f)

	if current, _ := cs.CurrentFeed.Load(); current == f {
		var next *feed
		for next = range cs.AvailableFeeds {
			break
		}

		cs.CurrentFeed.Store(next)

		if next == nil {
			c.publish(f.ID, false)
		}
	}

	// The feed is closed only after the catalog no longer consumes via it, so
	// that a consumer that observes the closure sees the catalog's new state.
	f.Close()
}

// publish publishes the set of stream IDs with the given ID added or removed,
// unless that leaves the set unchanged.
//
// It must be called with the mutex held.
func (c *Catalog) publish(id *uuidpb.UUID, present bool) {
	prev, _ := c.streamIDs.Load()

	if prev.Has(id) == present {
		return
	}

	next := prev.Clone()

	if next == nil {
		next = uuidcollection.NewSet()
	}

	if present {
		next.Add(id)
	} else {
		next.Delete(id)
	}

	c.streamIDs.Store(next)
}
