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
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// Reader delivers events from event streams to consumers.
//
// Each consumer reads historical events until it catches up to the tail of its
// stream, then receives contemporary events as they are appended. A consumer
// that falls behind the tail returns to reading historical events.
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

	done         xsync.Latch
	group        supervisor.Group
	synced       chan *scanner.Consumer
	desynced     chan *scanner.Consumer
	contemporary uuidpb.Map[*scanner.Contemporary]
}

// Subscribe registers ch to receive events of the given types from the given
// stream, starting at the given checkpoint offset.
//
// ch should be buffered. While the consumer is catching up on historical
// events, delivery blocks until ch is read. Once it has caught up, a consumer
// whose channel is full is returned to reading historical events.
//
// It returns a function that unsubscribes.
func (r *Reader) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidpb.Set,
	ch chan<- *envelopepb.Envelope,
) func() {
	if types.IsEmpty() {
		panic("message types must not be empty")
	}

	r.init()

	c := &scanner.Consumer{
		ID:           uuidpb.Generate(),
		StreamID:     streamID,
		MessageTypes: types,
		Envelopes:    ch,
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
	r.desynced = make(chan *scanner.Consumer)
	r.contemporary = uuidpb.Map[*scanner.Contemporary]{}

	for {
		select {
		case <-ctx.Done():
			return
		case c := <-r.sub:
			r.handleSubscribe(ctx, c)
		case c := <-r.unsub:
			r.handleUnsubscribe(ctx, c)
		case c := <-r.desynced:
			r.handleDesynced(ctx, c)
		case c := <-r.synced:
			r.handleSynced(ctx, c)
		}
	}
}

// handleSubscribe handles the subscription of a new consumer.
//
// Every consumer begins by reading historical events, regardless of its
// checkpoint. If it is already at the tail of the stream it is synchronized
// immediately and moves to contemporary events.
func (r *Reader) handleSubscribe(ctx context.Context, c *scanner.Consumer) {
	r.Logger.DebugContext(
		ctx,
		"consumer subscribed",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)

	r.startHistorical(ctx, c)
}

// handleUnsubscribe handles the unsubscription of a consumer from its stream.
//
// A consumer that is reading historical events is stopped by its Gone
// latch, which has already been set, so there's no need to do anything.
//
// A consumer that is reading contemporary events must be removed from its
// stream's [scanner.Contemporary].
func (r *Reader) handleUnsubscribe(ctx context.Context, c *scanner.Consumer) {
	if cs, ok := r.contemporary.Get(c.StreamID); ok {
		r.send(ctx, cs.Unsubscribe, c)
	}

	r.Logger.DebugContext(
		ctx,
		"consumer unsubscribed",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)
}

// handleSynced handles a consumer that has caught up to the tail of its stream.
//
// The consumer is handed to the stream's [scanner.Contemporary], which is started
// if this is the first consumer to synchronize with the stream.
func (r *Reader) handleSynced(ctx context.Context, c *scanner.Consumer) {
	r.Logger.DebugContext(
		ctx,
		"consumer synchronized with stream",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)

	cs := r.contemporaryScanner(ctx, c.StreamID)

	r.send(ctx, cs.Subscribe, c)
}

// handleDesynced handles a consumer that has fallen behind the tail of its
// stream.
//
// The [scanner.Contemporary] has already removed the consumer, so it is simply
// returned to reading historical events from its current checkpoint.
func (r *Reader) handleDesynced(ctx context.Context, c *scanner.Consumer) {
	r.Logger.DebugContext(
		ctx,
		"consumer desynchronized from stream",
		xslog.UUID("consumer_id", c.ID),
		xslog.UUID("stream_id", c.StreamID),
		slog.Uint64("checkpoint", c.Checkpoint.Load()),
	)

	r.startHistorical(ctx, c)
}

// startHistorical starts a supervised historical scanner for the consumer.
//
// The scanner stops of its own accord when the consumer catches up to the tail
// of the stream, or when the consumer is gone.
func (r *Reader) startHistorical(ctx context.Context, c *scanner.Consumer) {
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

// contemporaryScanner returns the contemporary scanner for the given stream,
// starting it if necessary.
func (r *Reader) contemporaryScanner(ctx context.Context, streamID *uuidpb.UUID) *scanner.Contemporary {
	if cs, ok := r.contemporary.Get(streamID); ok {
		return cs
	}

	cs := &scanner.Contemporary{
		DB:            r.DB,
		Notifications: r.Notifications,
		StreamID:      streamID,
		Subscribe:     make(chan *scanner.Consumer),
		Unsubscribe:   make(chan *scanner.Consumer),
		Desynced:      r.desynced,
		CacheSize:     r.ContemporaryCacheSize,
	}

	r.contemporary.Set(streamID, cs)

	logger := r.Logger.With(
		slog.String("scanner", "contemporary"),
		xslog.UUID("stream_id", streamID),
	)

	r.group.Go(ctx, logger, cs.Run)

	return cs
}

// send sends c on ch, which is a [scanner.Contemporary] Subscribe or Unsubscribe
// channel. It returns once the send completes, or when ctx is canceled.
func (r *Reader) send(ctx context.Context, ch chan<- *scanner.Consumer, c *scanner.Consumer) {
	for {
		select {
		case <-ctx.Done():
			return
		case ch <- c:
			return
		case d := <-r.desynced:
			// We must handle desynchronized consumers, as not doing so may
			// block the [scanner.Contemporary] from receiving on ch, causing a
			// deadlock.
			r.startHistorical(ctx, d)
		}
	}
}
