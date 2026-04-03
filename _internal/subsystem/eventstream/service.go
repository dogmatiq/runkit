package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/x/xtelemetry"
)

// Service manages the state of the application's event stream.
type Service struct {
	// Journals is the store used to persist event stream transactions.
	Journals journal.BinaryStore

	// BufferSize is the number of pending [AppendRequest] values that are
	// buffered in memory per partition.
	BufferSize uint

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Stop is a signalling channel that, when closed, initiates a graceful
	// shutdown of the service.
	Stop <-chan struct{}

	// AppendRequests is a messaging channel on which the service receives
	// requests to append events to the event stream.
	AppendRequests <-chan AppendRequest

	telemetry        *telemetry.Recorder
	partitions       uuidpb.Map[*partition]
	partitionStopped chan partitionStopped
}

// Run starts the service. It blocks until it the service is stopped gracefully,
// the context is cancelled, or a fatal error occurs.
func (s *Service) Run(ctx context.Context) error {
	startedAt := time.Now()
	s.telemetry = s.Telemetry.Recorder(xtelemetry.ModulePath)

	s.telemetry.Info(
		ctx,
		"event-stream.service-started",
		"service started",
	)
	defer func() {
		s.telemetry.Info(
			ctx,
			"event-stream.service-stopped",
			"service stopped",
			telemetry.Duration("uptime", time.Since(startedAt)),
		)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		select {
		case <-s.Stop:
			// If we're stopping, let the partitions stop gracefully.
		default:
			// Otherwise, terminate them!
			cancel()
		}

		// Wait for all partitions to stop.
		//
		// The partitions share the same context and stop channel so they do not
		// require any further signalling.
		for s.partitions.Len() > 0 {
			s.handlePartitionStopped(ctx, <-s.partitionStopped)
		}
	}()

	for {
		ok, err := s.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// tick waits for the next request to arrive and processes it.
func (s *Service) tick(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.Stop:
		s.telemetry.Info(
			ctx,
			"event-stream.stop",
			"received a request to stop gracefully",
			telemetry.Int("service.active_partitions", s.partitions.Len()),
		)
		return false, nil
	case x := <-s.partitionStopped:
		return true, s.handlePartitionStopped(ctx, x)
	case req := <-s.AppendRequests:
		return true, s.handleAppendRequest(ctx, req)
	}
}

// handleAppendRequest dispatches an [AppendRequest] to the appropriate
// [partition], starting a new instance if required.
func (s *Service) handleAppendRequest(ctx context.Context, req AppendRequest) error {
	validateAppendRequest(req)

	for {
		p, ok := s.partitions.Get(req.PartitionID)
		if !ok {
			p = s.startPartition(
				ctx,
				req.PartitionID,
				make(chan AppendRequest, s.BufferSize),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case x := <-s.partitionStopped:
			if err := s.handlePartitionStopped(ctx, x); err != nil {
				return err
			}
		case p.AppendRequests <- req:
			return nil
		}
	}
}

func (s *Service) handlePartitionStopped(ctx context.Context, x partitionStopped) error {
	s.partitions.Delete(x.Partition.ID)

	attrs := []telemetry.Attr{
		telemetry.Int("service.active_partitions", s.partitions.Len()),
		telemetry.UUID("partition.id", x.Partition.ID),
		telemetry.Int("partition.pending_requests", len(x.Partition.AppendRequests)),
		telemetry.Duration("uptime", x.Uptime),
	}

	if x.Error != nil {
		s.telemetry.Error(
			ctx,
			"event-stream.partition-stopped",
			"partition stopped due to an error",
			x.Error,
			attrs...,
		)
	} else {
		s.telemetry.Info(
			ctx,
			"event-stream.partition-stopped",
			"partition stopped gracefully",
			attrs...,
		)
	}

	// If the partition has unhandled requests then start another instance for
	// the same partition ID.
	//
	// Note that we _don't_ start another instance just because the previous one
	// failed. If there are no pending requests, we rely on the sender to resend
	// requests that were outright rejected.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.Stop:
		// We're shutting down, so don't start a new instance.
	default:
		if len(x.Partition.AppendRequests) != 0 {
			s.startPartition(
				ctx,
				x.Partition.ID,
				x.Partition.AppendRequests,
			)
		}
	}

	return nil
}

func (s *Service) startPartition(
	ctx context.Context,
	id *uuidpb.UUID,
	requests chan AppendRequest,
) *partition {
	p := &partition{
		ID:             id,
		Journals:       s.Journals,
		Shutdown:       s.Stop,
		AppendRequests: requests,
		Telemetry: s.Telemetry.Recorder(
			xtelemetry.ModulePath,
			telemetry.UUID("partition.id", id),
		),
	}

	s.partitions.Set(id, p)

	if s.partitionStopped == nil {
		s.partitionStopped = make(chan partitionStopped)
	}

	go func() {
		startedAt := time.Now()
		err := p.Run(ctx)

		s.partitionStopped <- partitionStopped{
			p,
			time.Since(startedAt),
			err,
		}
	}()

	message := "partition started"
	if len(requests) > 0 {
		message = "partition restarted"
	}

	s.telemetry.Info(
		ctx,
		"event-stream.partition-started",
		message,
		telemetry.Int("service.active_partitions", s.partitions.Len()),
		telemetry.UUID("partition.id", p.ID),
		telemetry.Int("partition.pending_requests", len(p.AppendRequests)),
		telemetry.Int("partition.buffer_size", s.BufferSize),
	)

	return p
}

// partitionStopped signals to the [service] that a [partition] has stopped
// running.
type partitionStopped struct {
	Partition *partition
	Uptime    time.Duration
	Error     error
}
