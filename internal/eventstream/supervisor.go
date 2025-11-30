package eventstream

import (
	"context"

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

	workerID     int
	workers      maps.Proto[*uuidpb.UUID, *worker]
	workerExited chan workerExited
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

		// Wait for all workers to exit.
		//
		// The workers share the same shutdown and terminate channels as the
		// supervisor therefore if the supervisor is exiting, the workers will
		// also be exiting.
		for s.workers.Len() > 0 {
			s.handleWorkerExited(ctx, <-s.workerExited)
		}

		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.done",
			"all workers have exited",
		)
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
			"eventstream.supervisor.shutdown",
			"received shutdown signal",
		)
		return false, nil
	case x := <-s.workerExited:
		return true, s.handleWorkerExited(ctx, x)
	case req := <-s.AppendEvents:
		return true, s.handleAppendEvents(ctx, req)
	}
}

func (s *Supervisor) handleAppendEvents(ctx context.Context, req AppendEvents) error {
	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.append.received",
		"received append request",
		telemetry.UUID("stream_id", req.StreamID),
	)

	// If the request is malformed, we just close the reply channel and return.
	// The sender of the request is misbehaving, and it's up to that subsystem
	// to recover itself.
	if len(req.Events) == 0 {
		close(req.Reply)

		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.append.discarded",
			"discarded append request with zero events",
			telemetry.UUID("stream_id", req.StreamID),
		)

		return nil
	}

	w := s.worker(ctx, req.StreamID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case x := <-s.workerExited:
			if err := s.handleWorkerExited(ctx, x); err != nil {
				return err
			}
		case w.AppendEvents <- req:
			s.telemetry.Info(
				ctx,
				"eventstream.supervisor.append.sent",
				"forwarded append request to worker",
				telemetry.Int("worker_id", w.ID),
				telemetry.UUID("stream_id", req.StreamID),
			)
			return nil
		}
	}
}

func (s *Supervisor) handleWorkerExited(ctx context.Context, x workerExited) error {
	s.workers.Remove(x.Worker.StreamID)

	if x.Error != nil {
		s.telemetry.Error(
			ctx,
			"eventstream.supervisor.worker.failed",
			x.Error,
			telemetry.Int("worker_id", x.Worker.ID),
			telemetry.UUID("stream_id", x.Worker.StreamID),
		)

		return x.Error
	}

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.worker.exited",
		"worker exited normally",
		telemetry.Int("worker_id", x.Worker.ID),
		telemetry.UUID("stream_id", x.Worker.StreamID),
	)

	return nil
}

func (s *Supervisor) worker(ctx context.Context, streamID *uuidpb.UUID) *worker {
	if w, ok := s.workers.TryGet(streamID); ok {
		return w
	}

	s.workerID++

	w := &worker{
		ID:             s.workerID,
		StreamID:       streamID,
		Journals:       s.Journals,
		Shutdown:       s.Shutdown,
		EventsAppended: s.EventsAppended,
		AppendEvents:   make(chan AppendEvents, s.BufferSize),
	}

	if s.workerExited == nil {
		s.workerExited = make(chan workerExited)
	}

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.worker.started",
		"started a new worker",
		telemetry.Int("worker_id", w.ID),
		telemetry.UUID("stream_id", streamID),
	)

	go func() {
		err := w.Run(ctx)

		// We can send to this channel without blocking (forever), because the
		// supervisor will always wait for workers to exit before it exits
		// itself.
		s.workerExited <- workerExited{w, err}
	}()

	s.workers.Set(streamID, w)

	return w
}

type workerExited struct {
	Worker *worker
	Error  error
}
