package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
	"github.com/dogmatiq/runkit/internal/ringbuffer"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"golang.org/x/sync/errgroup"
)

// LocalCatalog is the [Catalog] of event streams recorded by this application.
//
// Streams are never removed, so each set returned by [LocalCatalog.StreamIDs]
// is a superset of the sets returned before it.
//
// Subscriptions are served from a cache of each stream's most recent events. A
// subscription whose checkpoint is behind the cache is served historical events
// until it catches up.
type LocalCatalog struct {
	// DB is the database connection used to discover event streams and read
	// events.
	DB *sql.DB

	// Signals is the bus used to receive signals that event streams have been
	// created and that events have been appended to them.
	Signals *signal.Bus

	// Logger is the target for log messages produced by the catalog and the
	// supervisors of the scanners it starts.
	Logger *slog.Logger

	// ContemporaryCacheSize is the number of recent events cached per stream
	// for delivery to subscriptions that are keeping up. It must be positive.
	ContemporaryCacheSize int

	// HistoricalBatchSize is the maximum number of events read per query when
	// reading historical events. It must be positive.
	HistoricalBatchSize int

	streamIDs xsync.Observable[*uuidcollection.Set]

	once       sync.Once
	sub, unsub chan *scanner.Consumer

	done    xsync.Latch
	group   *supervisor.Group
	synced  chan *scanner.Consumer
	batches chan scanner.Batch
	streams uuidcollection.Map[*localStream]
}

// localStream is the catalog's state for an event stream.
type localStream struct {
	// Cache holds the most recent events, up to the catalog's cache size.
	//
	// It is nil until the stream's contemporary scanner has sent its first
	// batch, at which point the catalog knows the stream's position.
	Cache *ringbuffer.Ring[*envelopepb.Envelope]

	// [CacheBegin, CacheEnd) is a half-open interval representing the range of
	// stream offsets currently held in the cache. It is meaningless while Cache
	// is nil.
	CacheBegin, CacheEnd uint64

	// ContemporaryConsumers is the set of consumers being served from the
	// cache, as opposed to being served by a [scanner.Historical].
	ContemporaryConsumers map[*scanner.Consumer]struct{}
}

// StreamIDs returns the IDs of the known event streams, and a channel that is
// closed when more streams become known.
//
// The returned set must not be modified.
func (c *LocalCatalog) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return c.streamIDs.Load()
}

// Subscribe registers ch to receive events from a specific stream starting
// at the given checkpoint offset. Events are delivered in offset order.
//
// types is the set of message types to deliver. It must not be empty.
//
// lost is closed if the catalog can no longer deliver to the subscription.
func (c *LocalCatalog) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidcollection.Set,
	ch chan<- Batch,
) (lost <-chan struct{}, unsubscribe func()) {
	if types.IsEmpty() {
		panic("message types must not be empty")
	}

	c.init()

	con := &scanner.Consumer{
		ID:           uuidpb.Generate(),
		StreamID:     streamID,
		MessageTypes: types,
		Batches:      ch,
	}
	con.Checkpoint.Store(checkpoint)

	select {
	case <-c.done.Chan():
	case c.sub <- con:
	}

	var once sync.Once

	return c.done.Chan(), func() {
		once.Do(func() {
			con.Gone.Set()

			select {
			case <-c.done.Chan():
			case c.unsub <- con:
			}
		})
	}
}

func (c *LocalCatalog) init() {
	c.once.Do(func() {
		c.sub = make(chan *scanner.Consumer)
		c.unsub = make(chan *scanner.Consumer)
	})
}

// Run discovers event streams and delivers events until ctx is canceled or it
// is unable to continue.
//
// It must be called at most once. Subscriptions are lost when it returns.
func (c *LocalCatalog) Run(ctx context.Context) error {
	if c.ContemporaryCacheSize <= 0 {
		panic("cache size must be positive")
	}

	if c.HistoricalBatchSize <= 0 {
		panic("batch size must be positive")
	}

	c.init()

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return supervisor.Run(ctx, c.Logger, c.discover)
	})

	group.Go(func() error {
		return c.read(ctx)
	})

	return group.Wait()
}

// discover reads the set of event streams, then waits for a stream to be
// created and reads it again.
func (c *LocalCatalog) discover(ctx context.Context) error {
	// Subscribe to signals about new streams _before_ querying, so that a
	// stream created between the query and the subscription is not missed.
	//
	// The signal identifies the stream that was created, but that is not
	// used; each query reads the complete set of streams.
	created := make(chan string, 1)
	unsubscribe := c.Signals.Subscribe("eventstream.create", created)
	defer unsubscribe()

	for {
		streamIDs, err := c.loadStreamIDs(ctx)
		if err != nil {
			return err
		}

		// Streams are never removed, so the set has changed if and only if
		// it contains streams that were not already known.
		known, _ := c.streamIDs.Load()
		discovered := streamIDs.Diff(known)

		if !discovered.IsEmpty() {
			for streamID := range discovered.All() {
				c.Logger.DebugContext(
					ctx,
					"event stream discovered",
					xslog.UUID("stream_id", streamID),
				)
			}

			c.streamIDs.Store(streamIDs)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-created:
		}
	}
}

// loadStreamIDs returns the IDs of all event streams.
func (c *LocalCatalog) loadStreamIDs(ctx context.Context) (*uuidcollection.Set, error) {
	rows, err := c.DB.QueryContext(
		ctx,
		`SELECT id
		FROM eventstream.streams`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query event streams: %w", err)
	}
	defer rows.Close()

	streamIDs := uuidcollection.NewSet()

	for rows.Next() {
		streamID := &uuidpb.UUID{}

		if err := rows.Scan(
			xsql.UUID(streamID),
		); err != nil {
			return nil, fmt.Errorf("unable to scan event stream: %w", err)
		}

		streamIDs.Add(streamID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate event streams: %w", err)
	}

	return streamIDs, nil
}

// read delivers events to subscriptions until ctx is canceled or it is unable to
// continue.
func (c *LocalCatalog) read(ctx context.Context) error {
	c.group, ctx = supervisor.WithContext(ctx)
	defer c.done.Set()

	c.synced = make(chan *scanner.Consumer)
	c.batches = make(chan scanner.Batch)
	c.streams = uuidcollection.Map[*localStream]{}

	for {
		select {
		case <-ctx.Done():
			c.done.Set()

			if err := c.group.Wait(); err != nil {
				return err
			}

			return ctx.Err()
		case con := <-c.sub:
			c.handleSubscribe(ctx, con)
		case con := <-c.unsub:
			c.handleUnsubscribe(ctx, con)
		case con := <-c.synced:
			c.handleSynced(ctx, con)
		case b := <-c.batches:
			c.dispatchBatch(ctx, b)
		}
	}
}

// handleSubscribe handles the subscription of a new consumer.
//
// The consumer is served from the stream's cache, falling back to historical
// events if its checkpoint is not within the cache.
func (c *LocalCatalog) handleSubscribe(ctx context.Context, con *scanner.Consumer) {
	c.Logger.DebugContext(
		ctx,
		"consumer subscribed",
		xslog.UUID("consumer_id", con.ID),
		xslog.UUID("stream_id", con.StreamID),
		slog.Uint64("checkpoint", con.Checkpoint.Load()),
	)

	c.serveContemporary(ctx, con)
}

// handleUnsubscribe handles the unsubscription of a consumer from its stream.
//
// A consumer that is reading historical events is stopped by its Gone latch,
// which has already been set. A consumer that is reading contemporary events
// is simply forgotten.
func (c *LocalCatalog) handleUnsubscribe(ctx context.Context, con *scanner.Consumer) {
	if s, ok := c.streams.Get(con.StreamID); ok {
		delete(s.ContemporaryConsumers, con)
	}

	c.Logger.DebugContext(
		ctx,
		"consumer unsubscribed",
		xslog.UUID("consumer_id", con.ID),
		xslog.UUID("stream_id", con.StreamID),
		slog.Uint64("checkpoint", con.Checkpoint.Load()),
	)
}

// handleSynced handles a consumer that has caught up to the tail of its stream
// by reading historical events, and returns it to being served from the cache.
func (c *LocalCatalog) handleSynced(ctx context.Context, con *scanner.Consumer) {
	// Gone is set before an unsubscription is sent to the catalog. If it is set
	// here, the unsubscription has either already been handled, in which case
	// the consumer must not be re-added, or is still pending, in which case
	// adding it would be immediately undone. If it is not set, the
	// unsubscription is yet to come and will remove the consumer.
	if con.Gone.IsSet() {
		return
	}

	c.Logger.DebugContext(
		ctx,
		"consumer synchronized with stream",
		xslog.UUID("consumer_id", con.ID),
		xslog.UUID("stream_id", con.StreamID),
		slog.Uint64("checkpoint", con.Checkpoint.Load()),
	)

	c.serveContemporary(ctx, con)
}

// handleDesynced returns a consumer that has fallen behind the tail of its
// stream to reading historical events.
func (c *LocalCatalog) handleDesynced(ctx context.Context, s *localStream, con *scanner.Consumer) {
	delete(s.ContemporaryConsumers, con)

	c.Logger.DebugContext(
		ctx,
		"consumer desynchronized from stream",
		xslog.UUID("consumer_id", con.ID),
		xslog.UUID("stream_id", con.StreamID),
		slog.Uint64("checkpoint", con.Checkpoint.Load()),
	)

	c.serveHistorical(ctx, con)
}

// dispatchBatch handles a batch of events read from a stream by its
// contemporary
//
// The events are added to the stream's cache and every contemporary consumer of
// the stream is synchronized with it.
func (c *LocalCatalog) dispatchBatch(ctx context.Context, b scanner.Batch) {
	// The scanner's batches are contiguous, so the offset at which the batch
	// begins is its checkpoint less the number of events it contains.
	offset := b.Checkpoint - uint64(len(b.Envelopes))

	s, ok := c.streams.Get(b.StreamID)
	if !ok {
		// Scanners are only ever started for streams the catalog knows about.
		panic("received events from an unknown stream")
	}

	if s.Cache == nil {
		// This is the first batch from the stream's scanner, so this is where
		// the cache begins.
		s.Cache = ringbuffer.New[*envelopepb.Envelope](c.ContemporaryCacheSize)
		s.CacheBegin = offset
		s.CacheEnd = offset
	} else if offset != s.CacheEnd {
		// The scanner has restarted and begun reading relative to the end of
		// the stream, so the cache no longer leads up to the events it is
		// sending. Rebuild it from here; consumers that fall outside of the
		// new window are returned to reading historical events below.
		c.Logger.DebugContext(
			ctx,
			"contemporary event cache reset",
			xslog.UUID("stream_id", b.StreamID),
			slog.Uint64("offset", offset),
		)

		s.Cache = ringbuffer.New[*envelopepb.Envelope](c.ContemporaryCacheSize)
		s.CacheBegin = offset
		s.CacheEnd = offset
	}

	for _, env := range b.Envelopes {
		s.Cache.Push(env)
	}

	// The scanner reads every event type, so the events are contiguous and the
	// end of the cache is the batch's checkpoint. The beginning is derived from
	// the cache length so that it is correct both before and after the ring
	// buffer wraps.
	s.CacheEnd = b.Checkpoint
	s.CacheBegin = s.CacheEnd - uint64(s.Cache.Len())

	for con := range s.ContemporaryConsumers {
		c.syncConsumer(ctx, s, con)
	}
}

// syncConsumer synchronizes a contemporary consumer with the current state of
// its stream's cache.
//
// All cached events at or after the consumer's checkpoint that are of a type it
// is interested in are delivered in a single batch, after which the consumer's
// checkpoint is the end of the cache.
//
// If the consumer's checkpoint is before the beginning of the cache, or its
// channel is full, it is returned to reading historical events.
//
// If the stream's position is not yet known, nothing is done; the consumer is
// synchronized when the scanner's first batch arrives.
func (c *LocalCatalog) syncConsumer(ctx context.Context, s *localStream, con *scanner.Consumer) {
	if s.Cache == nil {
		return
	}

	checkpoint := con.Checkpoint.Load()

	if checkpoint < s.CacheBegin {
		// The consumer expects an offset that is no longer in the cache.
		c.handleDesynced(ctx, s, con)
		return
	}

	if checkpoint >= s.CacheEnd {
		// The consumer is already caught up with the cache.
		return
	}

	batch := scanner.Batch{
		StreamID:   con.StreamID,
		Checkpoint: s.CacheEnd,
	}

	idx := int(checkpoint - s.CacheBegin)

	for _, env := range s.Cache.Slice(idx, s.Cache.Len()) {
		typeID := env.GetBody().GetMessage().GetTypeId()

		if con.MessageTypes.Has(typeID) {
			batch.Envelopes = append(batch.Envelopes, env)
		}
	}

	// The batch is delivered even if it contains no events, so that the
	// consumer's checkpoint advances past events of irrelevant types.
	select {
	case <-ctx.Done():
	case <-con.Gone.Chan():
		delete(s.ContemporaryConsumers, con)
	case con.Batches <- batch:
		con.Checkpoint.Store(batch.Checkpoint)
	default:
		c.handleDesynced(ctx, s, con)
	}
}

// serveContemporary adds a consumer to the set served from the stream's cache
// and synchronizes it.
func (c *LocalCatalog) serveContemporary(ctx context.Context, con *scanner.Consumer) {
	s := c.streamByID(ctx, con.StreamID)
	s.ContemporaryConsumers[con] = struct{}{}
	c.syncConsumer(ctx, s, con)
}

// serveHistorical starts a supervised historical scanner for the consumer.
//
// The scanner stops of its own accord when the consumer catches up to the tail
// of the stream, or when the consumer is gone.
func (c *LocalCatalog) serveHistorical(ctx context.Context, con *scanner.Consumer) {
	hs := &scanner.Historical{
		DB:        c.DB,
		Consumer:  con,
		Synced:    c.synced,
		BatchSize: c.HistoricalBatchSize,
	}

	logger := c.Logger.With(
		slog.String("scanner", "historical"),
		xslog.UUID("consumer_id", con.ID),
		xslog.UUID("stream_id", con.StreamID),
	)

	c.group.Go(logger, hs.Run)
}

// streamByID returns the catalog's state for the given stream, starting the
// stream's contemporary scanner if necessary.
//
// The scanner runs until the catalog stops, even if the stream has no
// consumers. The number of streams is small and bounded by write concurrency,
// and keeping the scanner running keeps the cache primed for the next consumer.
func (c *LocalCatalog) streamByID(ctx context.Context, streamID *uuidpb.UUID) *localStream {
	if s, ok := c.streams.Get(streamID); ok {
		return s
	}

	s := &localStream{
		ContemporaryConsumers: map[*scanner.Consumer]struct{}{},
	}

	c.streams.Set(streamID, s)

	cs := &scanner.Contemporary{
		DB:        c.DB,
		Signals:   c.Signals,
		StreamID:  streamID,
		Batches:   c.batches,
		BatchSize: c.ContemporaryCacheSize,
	}

	logger := c.Logger.With(
		slog.String("scanner", "contemporary"),
		xslog.UUID("stream_id", streamID),
	)

	c.group.Go(logger, cs.Run)

	return s
}
