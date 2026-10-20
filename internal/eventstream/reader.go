package eventstream

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
)

// Reader reads events from event streams and dispatches them to subscribers.
type Reader struct {
	DB *sql.DB

	done xsync.Latch

	once           sync.Once
	sub, unsub     chan *readerSubscriber
	synced, lagged chan *readerSubscriber
	historical     map[*readerSubscriber]*historicalReader
}

type readerSubscriber struct {
	StreamID     *uuidpb.UUID
	Offset       uint64
	Types        *uuidpb.Set
	Envelopes    chan<- *envelopepb.Envelope
	Unsubscribed xsync.Latch
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
			r.subscribe(ctx, sub)
		case sub := <-r.unsub:
			r.unsubscribe(sub)
		case <-r.synced:
		case <-r.lagged:
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

func (r *Reader) subscribe(ctx context.Context, sub *readerSubscriber) {
	ctx, cancel := context.WithCancel(ctx)

	hr := &historicalReader{
		DB:           r.DB,
		Subscription: sub,
		Synced:       r.synced,
		Stop:         cancel,
	}

	r.historical[sub] = hr

	go hr.Run(ctx)
}

func (r *Reader) unsubscribe(sub *readerSubscriber) {
	if hr, ok := r.historical[sub]; ok {
		hr.Stop()
		delete(r.historical, sub)
	}
}
