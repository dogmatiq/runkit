package eventstream

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/notification"
)

// Reader reads events from event streams and dispatches them to subscribers.
type Reader struct {
	DB            *sql.DB
	Notifications *notification.Listener
	Logger        *slog.Logger

	done xsync.Latch

	once           sync.Once
	sub, unsub     chan *readerSubscriber
	synced, lagged chan *readerSubscriber

	contemporary uuidpb.Map[*contemporaryReader]
	historical   map[*readerSubscriber]*historicalReader
}

// readerSubscriber represents a subscriber registered to receive events from a
// specific event stream.
type readerSubscriber struct {
	StreamID     *uuidpb.UUID
	Offset       uint64
	Types        *uuidpb.Set
	Envelopes    chan<- *envelopepb.Envelope
	Unsubscribed xsync.Latch
}

// event is an event message that has been read from a stream.
type event struct {
	Envelope *envelopepb.Envelope
	Offset   uint64
}

// Run starts the reader, accepting subscription requests and dispatching events
// to subscribers. It blocks until ctx is canceled.
func (r *Reader) Run(ctx context.Context) {
	r.init()
	defer r.done.Set()

	for {
		select {
		case <-ctx.Done():
			return
		case sub := <-r.sub:
			r.handleSubscribe(ctx, sub)
		case sub := <-r.unsub:
			r.handleUnsubscribe(sub)
		case sub := <-r.synced:
			r.handleSync(ctx, sub)
		case sub := <-r.lagged:
			r.handleLagged(ctx, sub)
		}
	}
}

// Subscribe registers ch to receive events from the specified stream starting
// at the given offset.
//
// It returns a function that unsubscribes the channel from the stream.
func (r *Reader) Subscribe(
	streamID *uuidpb.UUID,
	offset uint64,
	types *uuidpb.Set,
	ch chan<- *envelopepb.Envelope,
) func() {
	r.init()

	sub := &readerSubscriber{
		StreamID:  streamID,
		Offset:    offset,
		Types:     types,
		Envelopes: ch,
	}

	select {
	case <-r.done.Chan():
	case r.sub <- sub:
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			sub.Unsubscribed.Set()

			select {
			case r.unsub <- sub:
			case <-r.done.Chan():
			}
		})
	}
}

func (r *Reader) init() {
	r.once.Do(func() {
		r.sub = make(chan *readerSubscriber)
		r.unsub = make(chan *readerSubscriber)
		r.synced = make(chan *readerSubscriber)
		r.lagged = make(chan *readerSubscriber)
		r.historical = map[*readerSubscriber]*historicalReader{}
	})
}

func (r *Reader) handleSubscribe(ctx context.Context, sub *readerSubscriber) {
	ctx, cancel := context.WithCancel(ctx)

	hr := &historicalReader{
		DB:           r.DB,
		Subscription: sub,
		Synced:       r.synced,
		Stop:         cancel,
		Logger:       r.Logger,
	}

	r.historical[sub] = hr

	go hr.Run(ctx)
}

func (r *Reader) handleUnsubscribe(sub *readerSubscriber) {
	if hr, ok := r.historical[sub]; ok {
		hr.Stop()
		delete(r.historical, sub)
	}
}

func (r *Reader) handleSync(ctx context.Context, sub *readerSubscriber) {
	cr, ok := r.contemporary.Get(sub.StreamID)
	if !ok {
		cr = &contemporaryReader{
			DB:            r.DB,
			Notifications: r.Notifications,
			StreamID:      sub.StreamID,
			Subscribe:     make(chan *readerSubscriber),
			Unsubscribe:   make(chan *readerSubscriber),
			Lagged:        r.lagged,
			Logger:        r.Logger,
		}

		r.contemporary.Set(sub.StreamID, cr)

		go cr.Run(ctx)
	}

	select {
	case <-ctx.Done():
	case cr.Subscribe <- sub:
	}
}

func (r *Reader) handleLagged(ctx context.Context, sub *readerSubscriber) {
}
