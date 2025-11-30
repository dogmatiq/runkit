package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/eventstreamjournal"
	"golang.org/x/sync/errgroup"
)

// A worker is a service that appends events to a specific stream.
type worker struct {
	ID          int
	StreamID    *uuidpb.UUID
	Journals    journal.BinaryStore
	IdleTimeout time.Duration

	Shutdown       <-chan struct{}
	Terminate      <-chan struct{}
	EventsAppended chan<- EventsAppended
	AppendEvents   chan AppendEvents

	journal eventstreamjournal.Journal
	pos     journal.Position
	offset  uint64
}

func (w *worker) Run(ctx context.Context) error {
	if err := w.init(ctx); err != nil {
		return err
	}

	// The worker was started to service a request, so do that first.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case req := <-w.AppendEvents:
		if err := w.handleAppendEvents(ctx, req); err != nil {
			return err
		}
	}

	// Only after that first request has been handled do we honor the graceful
	// shutdown and idle signals.
	for {
		ok, err := w.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

func (w *worker) init(ctx context.Context) error {
	var err error
	w.journal, err = eventstreamjournal.Open(ctx, w.Journals, w.StreamID)
	if err != nil {
		return err
	}

	return nil
}

func (w *worker) tick(ctx context.Context) (bool, error) {
	idle := time.NewTimer(w.IdleTimeout)
	defer idle.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.Shutdown:
		return false, nil
	case <-idle.C:
		return false, nil
	case req := <-w.AppendEvents:
		return true, w.handleAppendEvents(ctx, req)
	}
}

func (w *worker) handleAppendEvents(ctx context.Context, req AppendEvents) error {
	offsetBefore := w.offset
	offsetAfter := offsetBefore + uint64(len(req.Events))

	if err := w.journal.Append(
		ctx,
		w.pos,
		eventstreamjournal.
			NewRecordBuilder().
			WithOffsetBefore(offsetBefore).
			WithOffsetAfter(offsetAfter).
			WithAppendEvents(
				&eventstreamjournal.AppendEvents{
					Events: req.Events,
				},
			).
			Build(),
	); err != nil {
		return err
	}

	w.pos++
	w.offset = offsetAfter

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case w.EventsAppended <- EventsAppended{
			StreamID: w.StreamID,
			Offset:   offsetBefore,
			Events:   req.Events,
		}:
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req.Reply <- AppendEventsReply{
			BeginOffset: offsetBefore,
			EndOffset:   offsetAfter,
		}:
			return nil
		}
	})

	return g.Wait()
}
