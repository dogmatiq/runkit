package eventstream

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/ringbuffer"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// Reader delivers events from event streams to consumers.
//
// Consumers are served from a cache of each stream's most recent events. A
// consumer whose checkpoint is behind the cache reads historical events until
// it catches up.
type Reader struct {
	// DB is the database connection used to read events.
	DB *sql.DB

	// Notifications is the listener used to receive notifications about new
	// events being appended to streams.
	Notifications *notification.Listener

	// Logger is the target for log messages produced by the reader and the
	// supervisors of the scanners it starts.
	Logger *slog.Logger

	// ContemporaryCacheSize is the number of recent events cached per stream
	// for delivery to contemporary consumers. It must be positive.
	ContemporaryCacheSize int

	// HistoricalBatchSize is the maximum number of events read per query when
	// reading historical events. It must be positive.
	HistoricalBatchSize int

	once       sync.Once
	sub, unsub chan *scanner.Consumer

	done    xsync.Latch
	group   supervisor.Group
	synced  chan *scanner.Consumer
	batches chan scanner.Batch
	streams uuidpb.Map[*readerStream]
}

// readerStream is the reader's state for an event stream.
type readerStream struct {
	// Cache holds the most recent events, up to the reader's cache size.
	//
	// It is nil until the stream's contemporary scanner has sent its first
	// batch, at which point the reader knows the stream's position.
	Cache *ringbuffer.Ring[*envelopepb.Envelope]

	// [CacheBegin, CacheEnd) is a half-open interval representing the range of
	// stream offsets currently held in the cache. It is meaningless while Cache
	// is nil.
	CacheBegin, CacheEnd uint64

	// ContemporaryConsumers is the set of consumers being served from the
	// cache, as opposed to being served by a [scanner.Historical].
	ContemporaryConsumers map[*scanner.Consumer]struct{}
}

// Subscribe registers ch to receive batches of events from a stream, starting
// at the given checkpoint offset.
//
// types is the IDs of the message types the consumer wishes to receive. It must
// not be empty.
//
// ch should be buffered. Events are delivered in offset order, and a consumer
// that does not keep up never misses events.
//
// It returns a function that unsubscribes.
func (r *Reader) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidpb.Set,
	ch chan<- Batch,
) func() {
	if types.IsEmpty() {
		panic("message types must not be empty")
	}

	r.init()

	c := &scanner.Consumer{
		ID:           uuidpb.Generate(),
		StreamID:     streamID,
		MessageTypes: types,
		Batches:      ch,
	}
	c.Checkpoint.Store(checkpoint)

	select {
	case <-r.done.Chan():
	case r.sub <- c:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			c.Gone.Set()

			select {
			case <-r.done.Chan():
			case r.unsub <- c:
			}
		})
	}
}

func (r *Reader) init() {
	r.once.Do(func() {
		r.sub = make(chan *scanner.Consumer)
		r.unsub = make(chan *scanner.Consumer)
	})
}

// Run executes the reader until ctx is canceled.
//
// It must be called at most once; after it returns, Subscribe has no effect.
func (r *Reader) Run(ctx context.Context) {
	if r.ContemporaryCacheSize <= 0 {
		panic("cache size must be positive")
	}

	if r.HistoricalBatchSize <= 0 {
		panic("batch size must be positive")
	}

	r.init()
	defer func() {
		r.done.Set()
		r.group.Wait()
	}()

	r.synced = make(chan *scanner.Consumer)
	r.batches = make(chan scanner.Batch)
	r.streams = uuidpb.Map[*readerStream]{}

	for {
		select {
		case <-ctx.Done():
			return
		case c := <-r.sub:
			r.handleSubscribe(ctx, c)
		case c := <-r.unsub:
			r.handleUnsubscribe(ctx, c)
		case c := <-r.synced:
			r.handleSynced(ctx, c)
		case b := <-r.batches:
			r.dispatchBatch(ctx, b)
		}
	}
}

// handleSubscribe handles the subscription of a new consumer.
//
// The consumer is served from the stream's cache, falling back to historical
// events if its checkpoint is not within the cache.
func (r *Reader) handleSubscribe(ctx context.Context, c *scanner.Consumer) {
	r.Logger.DebugContext(
		ctx,
		"consumer subscribed",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)

	r.serveContemporary(ctx, c)
}

// handleUnsubscribe handles the unsubscription of a consumer from its stream.
//
// A consumer that is reading historical events is stopped by its Gone latch,
// which has already been set. A consumer that is reading contemporary events
// is simply forgotten.
func (r *Reader) handleUnsubscribe(ctx context.Context, c *scanner.Consumer) {
	if s, ok := r.streams.Get(c.StreamID); ok {
		delete(s.ContemporaryConsumers, c)
	}

	r.Logger.DebugContext(
		ctx,
		"consumer unsubscribed",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)
}

// handleSynced handles a consumer that has caught up to the tail of its stream
// by reading historical events, and returns it to being served from the cache.
func (r *Reader) handleSynced(ctx context.Context, c *scanner.Consumer) {
	r.Logger.DebugContext(
		ctx,
		"consumer synchronized with stream",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)

	r.serveContemporary(ctx, c)
}

// handleDesynced returns a consumer that has fallen behind the tail of its
// stream to reading historical events.
func (r *Reader) handleDesynced(ctx context.Context, s *readerStream, c *scanner.Consumer) {
	delete(s.ContemporaryConsumers, c)

	r.Logger.DebugContext(
		ctx,
		"consumer desynchronized from stream",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)

	r.serveHistorical(ctx, c)
}

// dispatchBatch handles a batch of events read from a stream by its
// contemporary scanner.
//
// The events are added to the stream's cache and every contemporary consumer of
// the stream is synchronized with it.
func (r *Reader) dispatchBatch(ctx context.Context, b scanner.Batch) {
	// The scanner's batches are contiguous, so the offset at which the batch
	// begins is its checkpoint less the number of events it contains.
	offset := b.Checkpoint - uint64(len(b.Envelopes))

	s, ok := r.streams.Get(b.StreamID)
	if !ok {
		// Scanners are only ever started for streams the reader knows about.
		panic("received events from an unknown stream")
	}

	if s.Cache == nil {
		// This is the first batch from the stream's scanner, so this is where
		// the cache begins.
		s.Cache = ringbuffer.New[*envelopepb.Envelope](r.ContemporaryCacheSize)
		s.CacheBegin = offset
		s.CacheEnd = offset
	} else if offset != s.CacheEnd {
		// The scanner has restarted and begun reading relative to the end of
		// the stream, so the cache no longer leads up to the events it is
		// sending. Rebuild it from here; consumers that fall outside of the
		// new window are returned to reading historical events below.
		r.Logger.DebugContext(
			ctx,
			"contemporary event cache reset",
			xslog.UUID("stream_id", b.StreamID),
			slog.Uint64("offset", offset),
		)

		s.Cache = ringbuffer.New[*envelopepb.Envelope](r.ContemporaryCacheSize)
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

	for c := range s.ContemporaryConsumers {
		r.syncConsumer(ctx, s, c)
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
func (r *Reader) syncConsumer(ctx context.Context, s *readerStream, c *scanner.Consumer) {
	if s.Cache == nil {
		return
	}

	checkpoint := c.Checkpoint.Load()

	if checkpoint < s.CacheBegin {
		// The consumer expects an offset that is no longer in the cache.
		r.handleDesynced(ctx, s, c)
		return
	}

	if checkpoint >= s.CacheEnd {
		// The consumer is already caught up with the cache.
		return
	}

	batch := Batch{
		StreamID:   c.StreamID,
		Checkpoint: s.CacheEnd,
	}

	idx := int(checkpoint - s.CacheBegin)

	for _, env := range s.Cache.Slice(idx, s.Cache.Len()) {
		typeID := env.GetBody().GetMessage().GetTypeId()

		if c.MessageTypes.Has(typeID) {
			batch.Envelopes = append(batch.Envelopes, env)
		}
	}

	// The batch is delivered even if it contains no events, so that the
	// consumer's checkpoint advances past events of irrelevant types.
	select {
	case <-ctx.Done():
	case <-c.Gone.Chan():
		delete(s.ContemporaryConsumers, c)
	case c.Batches <- batch:
		c.Checkpoint.Store(batch.Checkpoint)
	default:
		r.handleDesynced(ctx, s, c)
	}
}

// serveContemporary adds a consumer to the set served from the stream's cache
// and synchronizes it.
func (r *Reader) serveContemporary(ctx context.Context, c *scanner.Consumer) {
	s := r.streamByID(ctx, c.StreamID)
	s.ContemporaryConsumers[c] = struct{}{}
	r.syncConsumer(ctx, s, c)
}

// serveHistorical starts a supervised historical scanner for the consumer.
//
// The scanner stops of its own accord when the consumer catches up to the tail
// of the stream, or when the consumer is gone.
func (r *Reader) serveHistorical(ctx context.Context, c *scanner.Consumer) {
	hs := &scanner.Historical{
		DB:        r.DB,
		Consumer:  c,
		Synced:    r.synced,
		BatchSize: r.HistoricalBatchSize,
	}

	logger := r.Logger.With(
		slog.String("scanner", "historical"),
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
	)

	r.group.Go(ctx, logger, hs.Run)
}

// streamByID returns the reader's state for the given stream, starting the
// stream's contemporary scanner if necessary.
//
// The scanner runs until the reader stops, even if the stream has no consumers.
// The number of streams is small and bounded by write concurrency, and keeping
// the scanner running keeps the cache primed for the next consumer.
func (r *Reader) streamByID(ctx context.Context, streamID *uuidpb.UUID) *readerStream {
	if s, ok := r.streams.Get(streamID); ok {
		return s
	}

	s := &readerStream{
		ContemporaryConsumers: map[*scanner.Consumer]struct{}{},
	}

	r.streams.Set(streamID, s)

	cs := &scanner.Contemporary{
		DB:            r.DB,
		Notifications: r.Notifications,
		StreamID:      streamID,
		Batches:       r.batches,
		BatchSize:     r.ContemporaryCacheSize,
	}

	logger := r.Logger.With(
		slog.String("scanner", "contemporary"),
		xslog.UUID("stream_id", streamID),
	)

	r.group.Go(ctx, logger, cs.Run)

	return s
}
