package eventstream

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/ringbuffer"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// ContemporaryReader reads contemporary (near real-time) events from a specific
// event stream and delivers them to consumers.
type ContemporaryReader struct {
	// DB is the database connection used to read events from the stream.
	DB *sql.DB

	// Notifications is the listener used to receive notifications about new
	// events being appended to the stream.
	Notifications *notification.Listener

	// StreamID is the ID of the event stream being read.
	StreamID *uuidpb.UUID

	// Subscribe and Unsubscribe are channels on which consumers are sent to
	// register and unregister their interest in contemporary events.
	Subscribe, Unsubscribe chan *Consumer

	// Desynced is a channel on which consumers are sent when they have
	// desynchronized from the tail of the stream. Such consumers must read
	// historical events until they resynchronize.
	Desynced chan<- *Consumer

	// CacheSize is the maximum number of recent events to keep in the in-memory
	// cache. Any events that don't fit in the cache are considered historical.
	// It must be positive.
	CacheSize int

	// cache holds the most recent events up to the configured CacheSize.
	cache *ringbuffer.Ring[*envelopepb.Envelope]

	// [begin, end) is a half-open interval representing the range of stream
	// offsets currently held in the cache.
	begin, end uint64

	// consumers is the set of currently registered consumers.
	consumers map[*Consumer]struct{}
}

// Run executes the reader until ctx is canceled or an error occurs.
//
// Consumers are retained across subsequent calls. Run must not be called
// concurrently.
func (r *ContemporaryReader) Run(ctx context.Context) error {
	// Subscribe to notifications about new events _before_ reading the current
	// state of the stream. This ensures that no events are missed between the
	// time we read the checkpoint and the time we start catching up.
	appended := make(chan string, 1)
	topic := "eventstream.append." + r.StreamID.AsString()
	unsubscribe := r.Notifications.SubscribeLossy(topic, appended)
	defer unsubscribe()

	// Load the checkpoint from which to start reading events.
	checkpoint, err := r.loadCheckpoint(ctx)
	if err != nil {
		return err
	}

	// Initialize the cache.
	r.cache = ringbuffer.New[*envelopepb.Envelope](r.CacheSize)
	r.begin = checkpoint
	r.end = checkpoint

	if err := r.catchUp(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-appended:
			if err := r.catchUp(ctx); err != nil {
				return err
			}

		case c := <-r.Subscribe:
			if err := r.subscribe(ctx, c); err != nil {
				return err
			}

		case c := <-r.Unsubscribe:
			delete(r.consumers, c)
		}
	}
}

// loadCheckpoint returns the checkpoint at which the reader should start
// reading events, taking into account the cache size.
func (r *ContemporaryReader) loadCheckpoint(ctx context.Context) (uint64, error) {
	row := r.DB.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM eventstream.streams
		WHERE id = $1`,
		xsql.UUID(r.StreamID),
	)

	var checkpoint uint64
	if err := row.Scan(&checkpoint); err != nil {
		return 0, fmt.Errorf("failed to scan next stream offset: %w", err)
	}

	if checkpoint > uint64(r.CacheSize) {
		// We have more events than the cache can hold, so start reading from
		// the point where the cache would be full.
		checkpoint -= uint64(r.CacheSize)
	} else {
		// We have fewer events than the cache can hold, so start reading from
		// the beginning of the stream.
		checkpoint = 0
	}

	return checkpoint, nil
}

// catchUp reads events from the end of the cache until the end of the stream,
// caching each batch and synchronizing consumers as it goes.
func (r *ContemporaryReader) catchUp(ctx context.Context) error {
	for {
		envelopes, err := r.readBatch(ctx)
		if err != nil {
			return err
		}

		if len(envelopes) == 0 {
			return nil
		}

		for _, env := range envelopes {
			r.cache.Push(env)
		}

		// The reader caches every event type, so the batch is contiguous and
		// the end of the cache advances by exactly the number of events read.
		// The beginning is derived from the cache length so that it is correct
		// both before and after the ring buffer wraps.
		r.end += uint64(len(envelopes))
		r.begin = r.end - uint64(r.cache.Len())

		if err := r.syncAll(ctx); err != nil {
			return err
		}

		// The batch does not completely fill the cache, meaning there are no
		// more events on the stream at this time.
		if len(envelopes) < r.CacheSize {
			return nil
		}
	}
}

// readBatch returns up to r.CacheSize events starting at the next event after
// the end of the cache.
func (r *ContemporaryReader) readBatch(ctx context.Context) ([]*envelopepb.Envelope, error) {
	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT
			stream_offset,
			envelope
		FROM eventstream.events
		WHERE stream_id = $1
			AND stream_offset >= $2
		ORDER BY stream_offset
		LIMIT $3`,
		xsql.UUID(r.StreamID),
		r.end,
		r.CacheSize,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	var envelopes []*envelopepb.Envelope

	for rows.Next() {
		var (
			offset uint64
			env    = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			&offset,
			xsql.Envelope(env),
		); err != nil {
			return nil, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(r.StreamID).
				WithOffset(offset).
				Build(),
		)

		envelopes = append(envelopes, env)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate events: %w", err)
	}

	return envelopes, nil
}

// subscribe synchronizes a new consumer with the current state of the cache,
// and registers it to receive future events.
func (r *ContemporaryReader) subscribe(ctx context.Context, c *Consumer) error {
	if r.consumers == nil {
		r.consumers = map[*Consumer]struct{}{}
	}

	r.consumers[c] = struct{}{}

	return r.syncOne(ctx, c)
}

// syncOne synchronizes the given consumer with the current state of the cache.
func (r *ContemporaryReader) syncOne(ctx context.Context, c *Consumer) error {
	if c.Checkpoint < r.begin {
		// The consumer expects an offset that is no longer in the cache.
		return r.desync(ctx, c)
	}

	if c.Checkpoint >= r.end {
		// The consumer is already caught up with the cache.
		//
		// The consumer may be beyond the end of the cache, if it happened to
		// see a new event on the stream before this reader sees that event.
		return nil
	}

	idx := int(c.Checkpoint - r.begin)

	// Deliver all cached events to the consumer starting from its checkpoint.
	for _, env := range r.cache.Slice(idx, r.cache.Len()) {
		typeID := env.GetBody().GetMessage().GetTypeId()

		if c.MessageTypes.Has(typeID) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.Gone.Chan():
				delete(r.consumers, c)
				return nil
			default:
				return r.desync(ctx, c)
			case c.Envelopes <- env:
			}
		}

		c.Checkpoint++
	}

	return nil
}

// syncAll synchronizes all consumers with the current state of the cache.
func (r *ContemporaryReader) syncAll(ctx context.Context) error {
	for c := range r.consumers {
		if err := r.syncOne(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

// desync removes the given consumer from the reader and sends it to the
// r.Desynced channel.
func (r *ContemporaryReader) desync(ctx context.Context, c *Consumer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.Desynced <- c:
		delete(r.consumers, c)
		return nil
	}
}
