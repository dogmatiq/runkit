package eventstream

import (
	"context"
	"database/sql"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
)

type Reader struct {
	// 	Notifications *notification.Listener
	DB *sql.DB
	// 	Logger        *log.Logger

	done xsync.Latch
	// contemporary uuidpb.Map[*contemporaryReader]

	once        sync.Once
	sub, unsub  chan *readSubscriber
	subscribers map[*readSubscriber]struct{}
}

type readSubscriber struct {
	StreamID     *uuidpb.UUID
	Types        *uuidpb.Set
	Offset       uint64
	Events    chan<- *envelopepb.Envelope
	Unsubscribed xsync.Latch
}

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
			r.unsubscribe(ctx, sub)
		}
	}
}

func (r *Reader) Read(
	streamID *uuidpb.UUID,
	types *uuidpb.Set,
	offset int64,
	ch chan<- *envelopepb.Envelope,
) func() {
	sub := &readSubscriber{
		StreamID:  streamID,
		Types:     types,
		Offset:    uint64(offset),
		Events: ch,
	}

	select {
	case <-r.done.Chan():
	case r.sub <- sub:
	}

	return func() {
		sub.Unsubscribed.Set()

		select {
		case <-r.done.Chan():
		case r.unsub <- sub:
		}
	}
}

func (r *Reader) init() {
	r.once.Do(func() {
		r.sub = make(chan *readSubscriber)
		r.unsub = make(chan *readSubscriber)
		r.subscribers = make(map[*readSubscriber]struct{})
	})
}

func (r *Reader) subscribe(ctx context.Context, sub *readSubscriber) {
	hr := &historicalReader{
		DB:         r.DB,
		Subscriber: sub,
	}

	go hr.Run(ctx)
}

func (r *Reader) unsubscribe(ctx context.Context, sub *readSubscriber) {
}
