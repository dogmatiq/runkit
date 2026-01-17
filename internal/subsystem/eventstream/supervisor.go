package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xtelemetry"
)

// Supervisor manages the state of the application's event stream, across all
// partitions.
type Supervisor struct {
	// ID is a unique identifier for this supervisor instance.
	// It is used for telemetry purposes only.
	ID *uuidpb.UUID

	// Journals is the journal store used to persist event stream transactions.
	Journals journal.BinaryStore

	// BufferSize is the number of pending [AppendRequest] values that can be
	// buffered in memory, per partition.
	BufferSize uint

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Shutdown is a channel that is closed when the supervisor should shut down
	// gracefully.
	Shutdown <-chan struct{}

	// AppendRequests is a channel on which the supervisor receives
	// requests to append events to the event stream.
	AppendRequests <-chan AppendRequest

	telemetry *telemetry.Recorder

	workerID      uint
	workers       uuidpb.Map[*worker]
	workerStopped chan workerStopped
}

// Run starts the supervisor's main event loop.
func (s *Supervisor) Run(ctx context.Context) error {
	startedAt := time.Now()

	s.telemetry = s.Telemetry.Recorder(
		xtelemetry.ModulePath,
		telemetry.UUID("supervisor.id", s.ID),
	)

	s.telemetry.Info(
		ctx,
		"event-stream.supervisor.started",
		"event stream supervisor started",
		telemetry.Int("supervisor.buffer_size", s.BufferSize),
	)
	defer func() {
		s.telemetry.Info(
			ctx,
			"event-stream.supervisor.stopped",
			"event stream supervisor stopped",
			telemetry.Duration("supervisor.uptime", time.Since(startedAt)),
		)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		select {
		case <-s.Shutdown:
			// If we're shutting down, let the workers stop gracefully.
		default:
			// Otherwise, terminate them!
			cancel()
		}

		// Wait for all workers to stop.
		//
		// The workers share the same context and shutdown channel as the
		// supervisor. Therefore, if the supervisor is stopping, so too will the
		// workers.
		for s.workers.Len() > 0 {
			s.handleWorkerStopped(ctx, <-s.workerStopped)
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
			"event-stream.supervisor.shutdown",
			"event stream supervisor received a request to shut down",
			telemetry.Int("supervisor.workers", s.workers.Len()),
		)
		return false, nil
	case x := <-s.workerStopped:
		return true, s.handleWorkerStopped(ctx, x)
	case req := <-s.AppendRequests:
		return true, s.handleAppendRequest(ctx, req)
	}
}

func (s *Supervisor) handleAppendRequest(ctx context.Context, req AppendRequest) error {
	if err := validateAppendRequest(req); err != nil {
		return err
	}

	s.telemetry.Info(
		ctx,
		"event-stream.supervisor.append-request.received",
		"event stream supervisor received a request to append events to a partition",
		telemetry.UUID("partition.id", req.PartitionID),
	)

	for {
		w, ok := s.workers.Get(req.PartitionID)
		if !ok {
			w = s.startWorker(
				ctx,
				req.PartitionID,
				make(chan AppendRequest, s.BufferSize),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case x := <-s.workerStopped:
			if err := s.handleWorkerStopped(ctx, x); err != nil {
				return err
			}
		case w.AppendRequests <- req:
			return nil
		}
	}
}

func (s *Supervisor) handleWorkerStopped(ctx context.Context, x workerStopped) error {
	s.workers.Delete(x.Worker.PartitionID)

	if xerrors.IsFatal(x.Error) {
		s.telemetry.Error(
			ctx,
			"event-stream.supervisor.worker.failed",
			"event stream supervisor detected a fatal worker error, supervisor is shutting down",
			x.Error,
			telemetry.Int("supervisor.workers", s.workers.Len()),
			telemetry.UUID("partition.id", x.Worker.PartitionID),
			telemetry.Int("partition.pending_requests", len(x.Worker.AppendRequests)),
			telemetry.Int("worker.id", x.Worker.ID),
		)

		return x.Error
	}

	if x.Error != nil {
		s.telemetry.Error(
			ctx,
			"event-stream.supervisor.worker.failed",
			"event stream supervisor detected a non-fatal worker error",
			x.Error,
			telemetry.Int("supervisor.workers", s.workers.Len()),
			telemetry.UUID("partition.id", x.Worker.PartitionID),
			telemetry.Int("partition.pending_requests", len(x.Worker.AppendRequests)),
			telemetry.Int("worker.id", x.Worker.ID),
		)
	} else {
		s.telemetry.Info(
			ctx,
			"event-stream.supervisor.worker.shutdown",
			"event stream supervisor detected a graceful worker shutdown",
			telemetry.Int("supervisor.workers", s.workers.Len()),
			telemetry.UUID("partition.id", x.Worker.PartitionID),
			telemetry.Int("partition.pending_requests", len(x.Worker.AppendRequests)),
			telemetry.Int("worker.id", x.Worker.ID),
		)
	}

	// If the partition has unhandled requests then start another worker for
	// this partition.
	//
	// Note that we _don't_ start another worker just because the previous one
	// failed. If there are no pending requests, we rely on the sender to resend
	// requests that were outright rejected.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.Shutdown:
		// We're shutting down, so don't start a new worker.
	default:
		if len(x.Worker.AppendRequests) != 0 {
			s.startWorker(
				ctx,
				x.Worker.PartitionID,
				x.Worker.AppendRequests,
			)
		}
	}

	return nil
}

func (s *Supervisor) startWorker(
	ctx context.Context,
	partitionID *uuidpb.UUID,
	requests chan AppendRequest,
) *worker {
	s.workerID++

	w := &worker{
		ID:             s.workerID,
		PartitionID:    partitionID,
		Journals:       s.Journals,
		Shutdown:       s.Shutdown,
		AppendRequests: requests,
		Telemetry: s.Telemetry.Recorder(
			xtelemetry.ModulePath,
			telemetry.UUID("supervisor.id", s.ID),
			telemetry.UUID("partition.id", partitionID),
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

	s.workers.Set(partitionID, w)

	s.telemetry.Info(
		ctx,
		"event-stream.supervisor.worker.started",
		"event stream supervisor started a new worker",
		telemetry.Int("supervisor.workers", s.workers.Len()),
		telemetry.UUID("partition.id", w.PartitionID),
		telemetry.Int("partition.pending_requests", len(w.AppendRequests)),
		telemetry.Int("worker.id", w.ID),
	)

	return w
}

type workerStopped struct {
	Worker *worker
	Error  error
}
