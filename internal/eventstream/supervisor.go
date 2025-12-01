package eventstream

import (
	"context"
	"fmt"

	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/telemetry"
)

// Supervisor accepts and delegates [AppendEvents] requests.
type Supervisor struct {
	// Journals is the journal store used to persist event streams.
	Journals journal.BinaryStore

	// BufferSize is the number of [AppendEvents] requests that can be buffered
	// in memory, per event stream.
	BufferSize int

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Shutdown is a channel that is closed when the supervisor should shut down
	// gracefully.
	Shutdown <-chan struct{}

	// AppendEvents is a channel on which the supervisor receives requests to
	// append events to streams.
	AppendEvents <-chan AppendEvents

	// EventsAppended is a channel on which the supervisor sends notifications
	// that events have been appended to streams.
	EventsAppended chan<- EventsAppended

	telemetry *telemetry.Recorder

	workerID      int
	workers       maps.Proto[*uuidpb.UUID, *worker]
	workerStopped chan workerStopped
}

// Run starts the supervisor's main event loop.
//
// It appends events to streams until a shutdown or terminate signal is
// received.
func (s *Supervisor) Run(ctx context.Context) error {
	s.telemetry = s.Telemetry.Recorder()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		cancel()

		// Wait for all workers to stop.
		//
		// The workers share the same context and shutdown channel as the
		// supervisor. Therefore, if the supervisor is stopping, so too will the
		// workers.
		for s.workers.Len() > 0 {
			s.handleWorkerStopped(ctx, <-s.workerStopped, true)
		}
	}()

	for {
		ok, err := s.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

func (s *Supervisor) tick(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.Shutdown:
		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.shutdown.signal",
			"supervisor received shutdown signal",
			telemetry.Int("supervisor.workers", s.workers.Len()),
		)
		return false, nil
	case x := <-s.workerStopped:
		return true, s.handleWorkerStopped(ctx, x, false)
	case req := <-s.AppendEvents:
		return true, s.handleAppendEvents(ctx, req)
	}
}

func (s *Supervisor) handleAppendEvents(ctx context.Context, req AppendEvents) error {
	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.append.recv",
		"supervisor received request to append events",
		telemetry.UUID("stream.id", req.StreamID),
	)

	// Attempt to deliver to an existing worker.
	for {
		w, ok := s.workers.TryGet(req.StreamID)
		if !ok {
			w = s.startWorker(
				ctx,
				req.StreamID,
				make(chan AppendEvents, s.BufferSize),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case x := <-s.workerStopped:
			if err := s.handleWorkerStopped(ctx, x, false); err != nil {
				return err
			}
		case w.AppendEvents <- req:
			s.telemetry.Info(
				ctx,
				"eventstream.supervisor.append.dispatch",
				"supervisor dispatched append request to worker queue",
				telemetry.UUID("stream.id", req.StreamID),
				telemetry.Int("worker.id", w.ID),
				telemetry.Int("worker.pending_requests", len(w.AppendEvents)),
			)
			return nil
		}
	}
}

func (s *Supervisor) handleWorkerStopped(ctx context.Context, x workerStopped, shutdown bool) error {
	s.workers.Remove(x.Worker.StreamID)
	pending := len(x.Worker.AppendEvents)

	if x.Error != nil {
		s.telemetry.Error(
			ctx,
			"eventstream.supervisor.worker.error",
			fmt.Errorf("supervisor observed worker error: %w", x.Error),
			telemetry.UUID("stream.id", x.Worker.StreamID),
			telemetry.Int("worker.id", x.Worker.ID),
			telemetry.Int("worker.pending_requests", pending),
			telemetry.Int("supervisor.workers", s.workers.Len()),
		)

		return x.Error
	}

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.worker.stop",
		"supervisor observed worker stop gracefully",
		telemetry.UUID("stream.id", x.Worker.StreamID),
		telemetry.Int("worker.id", x.Worker.ID),
		telemetry.Int("worker.pending_requests", pending),
		telemetry.Int("supervisor.workers", s.workers.Len()),
	)

	if shutdown || len(x.Worker.AppendEvents) == 0 {
		return nil
	}

	// We're not shutting down, and the worker had unhandled append requests, so
	// we immediately start a new worker to take over.
	s.startWorker(
		ctx,
		x.Worker.StreamID,
		x.Worker.AppendEvents,
	)

	return nil
}

func (s *Supervisor) startWorker(
	ctx context.Context,
	streamID *uuidpb.UUID,
	appendEvents chan AppendEvents,
) *worker {
	s.workerID++

	w := &worker{
		ID:             s.workerID,
		StreamID:       streamID,
		Journals:       s.Journals,
		Shutdown:       s.Shutdown,
		EventsAppended: s.EventsAppended,
		AppendEvents:   appendEvents,
		Telemetry: s.Telemetry.Recorder(
			telemetry.UUID("stream.id", streamID),
			telemetry.Int("worker.id", s.workerID),
		),
	}

	if s.workerStopped == nil {
		s.workerStopped = make(chan workerStopped)
	}

	go func() {
		// We can send to this channel without blocking (forever), because the
		// supervisor will wait for all workers to stop before it stops
		// receiving from this channel.
		s.workerStopped <- workerStopped{
			w,
			w.Run(ctx),
		}
	}()

	s.workers.Set(streamID, w)

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.worker.start",
		"supervisor started new worker",
		telemetry.UUID("stream.id", w.StreamID),
		telemetry.Int("worker.id", w.ID),
		telemetry.Int("worker.pending_requests", len(w.AppendEvents)),
		telemetry.Int("supervisor.workers", s.workers.Len()),
	)

	return w
}

type workerStopped struct {
	Worker *worker
	Error  error
}
