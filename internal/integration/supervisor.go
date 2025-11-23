package integration

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/integration/internal/wal"
	"github.com/dogmatiq/runkit/internal/telemetry"
)

// func (s *Supervisor) Run(ctx context.Context) error {
// 	sess := &session{
// 		Handler:          nil,
// 		Identity:         &identitypb.Identity{},
// 		Packer:           &envelopepb.Packer{},
// 		WAL:              nil,
// 		EnqueuedCommands: nil,
// 		EnqueueRequests:  make(<-chan EnqueueRequest),
// 		AppendRequests:   make(chan<- eventstream.AppendRequest),
// 		Shutdown:         r.shutdown,
// 		EventStreamID:    &uuidpb.UUID{},
// 		EventOffsetHint:  0,
// 		Telemetry:        s.Telemetry,
// 	}
// }

// // Supervisor manages the state of the integration subsystem as it pertains to a
// // single [dogma.IntegrationMessageHandler].
// type Supervisor struct {
// 	HandlerIdentity *identitypb.Identity
// 	Handler         dogma.IntegrationMessageHandler
// 	Shutdown        <-chan struct{}
// 	Telemetry       *telemetry.Recorder
// }

// // Run executes the supervisor until ctx is canceled, the shutdown channel is
// // closed, or an error occurs.
// func (s *Supervisor) Run(ctx context.Context) error {
// 	s.journal, err = integrationjournal.Open(ctx, s.Journals, s.HandlerIdentity.Key)
// 	if err != nil {
// 		return err
// 	}
// 	defer s.journal.Close()
// }

// Determine the event stream to which the events should be appended,
// // then use this stream from now on.
// if s.eventStreamID == nil {
// 	s.eventStreamID, s.eventOffsetHint, err = s.Events.SelectEventStream(ctx)
// 	if err != nil {
// 		s.telemetry.Error(ctx, "integration.handle_command.error", err)
// 		return err
// 	}
// }

type EnqueueRequest struct {
	Command *envelopepb.Envelope
	Reply   chan<- EnqueueResponse
}

type EnqueueResponse struct {
	CommandID *uuidpb.UUID
	Ack       bool
}

type Supervisor struct {
	Identity  *identitypb.Identity
	Handler   dogma.IntegrationMessageHandler
	Packer    *envelopepb.Packer
	Workers   int
	WAL       journal.Journal[*wal.Transaction]
	Seen      set.Set[*uuidpb.UUID]
	Telemetry *telemetry.Recorder

	EnqueueRequests <-chan EnqueueRequest
	AppendRequests  chan<- eventstream.AppendRequest
	Shutdown        <-chan struct{}

	bounds   journal.Interval
	truncate chan struct{}

	cancelWorkerContext   context.CancelFunc
	workerRequests        chan<- workerRequest
	workerResponses       <-chan workerResponse
	workerErrors          <-chan error
	pendingWorkers        int
	pendingWorkerRequests int

	pendingAppendRequests int
	appendResponses       chan eventstream.AppendResponse
}

func (s *Supervisor) Run(ctx context.Context) error {
	if err := s.reconcile(ctx); err != nil {
		return err
	}

	s.startWorkers(ctx)
	defer s.stopWorkers()

	s.truncate = make(chan struct{})

	for {
		if s.isIdle() && !s.bounds.IsEmpty() {
			close(s.truncate)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-s.Shutdown:
			return s.doShutdown(ctx)

		case <-s.truncate:
			if err := s.doTruncate(ctx); err != nil {
				return err
			}

		case req := <-s.EnqueueRequests:
			if err := s.onEnqueueRequest(ctx, req); err != nil {
				return err
			}

		case res := <-s.workerResponses:
			if err := s.onWorkerResponse(ctx, res); err != nil {
				return err
			}

		case err := <-s.workerErrors:
			s.pendingWorkers--
			return err

		case res := <-s.appendResponses:
			if err := s.onAppendResponse(ctx, res); err != nil {
				return err
			}

		}
	}
}

func (s *Supervisor) startWorkers(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancelWorkerContext = cancel

	workers := max(s.Workers, 1)

	requests := make(chan workerRequest)
	responses := make(chan workerResponse)
	errors := make(chan error, workers)

	s.workerRequests = requests
	s.workerResponses = responses
	s.workerErrors = errors

	for range workers {
		w := &worker{
			Handler:   s.Handler,
			Identity:  s.Identity,
			Packer:    s.Packer,
			Telemetry: s.Telemetry,

			Inbox:    requests,
			Outbox:   responses,
			Shutdown: s.Shutdown,
		}

		go func() {
			errors <- w.Run(ctx)
		}()

		s.pendingWorkers++
	}
}

func (s *Supervisor) stopWorkers() {
	s.cancelWorkerContext()

	for s.pendingWorkers != 0 {
		<-s.workerErrors
		s.pendingWorkers--
	}
}

func (s *Supervisor) isIdle() bool {
	return s.pendingAppendRequests == 0 && s.pendingWorkerRequests == 0
}

func (s *Supervisor) doShutdown(ctx context.Context) error {
	for !s.isIdle() {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case res := <-s.workerResponses:
			if err := s.onWorkerResponse(ctx, res); err != nil {
				return err
			}

		case err := <-s.workerErrors:
			s.pendingWorkers--
			return err

		case res := <-s.appendResponses:
			if err := s.onAppendResponse(ctx, res); err != nil {
				return err
			}

		}
	}

	return nil
}

func (s *Supervisor) doTruncate(ctx context.Context) error {
	if err := s.WAL.Truncate(ctx, s.bounds.End); err != nil {
		return err
	}

	s.bounds.Begin = s.bounds.End
	s.truncate = make(chan struct{})

	return nil
}

func (s *Supervisor) onEnqueueRequest(ctx context.Context, req EnqueueRequest) (err error) {
	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"integration.enqueue_command",
		telemetry.UUID("command.message_id", req.Command.GetMessageId()),
		telemetry.UUID("command.causation_id", req.Command.GetMessageId()),
		telemetry.UUID("command.correlation_id", req.Command.GetCorrelationId()),
		telemetry.UUID("command.type.id", req.Command.GetTypeId()),
		telemetry.MessageTypeName("command.type.name", req.Command.GetTypeId()),
		telemetry.String("command.description", req.Command.GetDescription()),
	)
	defer func() {
		if err != nil {
			s.Telemetry.Error(ctx, "integration.enqueue_command.error", err)
		}
		span.End()
	}()

	// Do not accept the command if it has already been accepted in the past.
	// This check provides command-level idempotency, even if the journal has
	// been truncated.
	//
	// TODO: there are optimizations to be made here (i.e. in-memory list of
	// recent commands, bloom filter, etc). Perhaps this could / should be done
	// in persistence kit.
	isDup, err := s.Seen.Has(ctx, req.Command.GetMessageId())
	if err != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req.Reply <- EnqueueResponse{req.Command.MessageId, false}:
			return err
		}
	}

	if isDup {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req.Reply <- EnqueueResponse{req.Command.MessageId, true}:
			s.Telemetry.Info(
				ctx,
				"integration.enqueue_command.done",
				"acknowledged request for duplicate command",
			)
			return nil
		}
	}

	op := &wal.HandleCommand{
		Command: req.Command,
	}

	if err := s.commit(
		ctx,
		func(txn *wal.TransactionBuilder) {
			txn.WithHandleCommand(op)
		},
	); err != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req.Reply <- EnqueueResponse{req.Command.MessageId, false}:
			return err
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case req.Reply <- EnqueueResponse{req.Command.MessageId, true}:
		s.Telemetry.Info(
			ctx,
			"integration.enqueue_command.done",
			"acknowledged request for new command",
		)
	}

	return s.doHandleCommand(ctx, op)
}

func (s *Supervisor) onWorkerResponse(
	ctx context.Context,
	res workerResponse,
) error {
	s.pendingWorkerRequests--

	// if len(scope.Events) == 0 {
	// 	s.Telemetry.Info(
	// 		ctx,
	// 		"integration.handle_command.done",
	// 		"command handled successfully",
	// 		telemetry.Int("event_count", 0),
	// 	)
	// 	return nil
	// }

	// appendOp := &wal.AppendEvents{
	// 	CommandId:  op.Command.GetMessageId(),
	// 	Events:     scope.Events,
	// 	StreamId:   s.EventStreamID,
	// 	OffsetHint: uint64(s.EventOffsetHint),
	// }

	// if err := s.commit(
	// 	ctx,
	// 	func(txn *wal.TransactionBuilder) {
	// 		txn.WithAppendEvents(appendOp)
	// 	},
	// ); err != nil {
	// 	return err
	// }

	// s.Telemetry.Info(
	// 	ctx,
	// 	"integration.handle_command.done",
	// 	"command handled successfully",
	// 	telemetry.Int("event_count", 0),
	// )

	// return s.appendEvents(ctx, appendOp)

	return nil
}

func (s *Supervisor) onAppendResponse(
	ctx context.Context,
	res eventstream.AppendResponse,
) error {
	s.pendingAppendRequests--

	return s.commit(
		ctx,
		func(txn *wal.TransactionBuilder) {
			txn.WithMarkEventsAsAppended(
				&wal.MarkEventsAsAppended{
					CommandId: res.CommandID,
				},
			)
		},
	)
}

func (s *Supervisor) doHandleCommand(ctx context.Context, op *wal.HandleCommand) (err error) {
	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"integration.handle_command",
		telemetry.UUID("command.message_id", op.Command.GetMessageId()),
		telemetry.UUID("command.causation_id", op.Command.GetMessageId()),
		telemetry.UUID("command.correlation_id", op.Command.GetCorrelationId()),
		telemetry.UUID("command.type.id", op.Command.GetTypeId()),
		telemetry.MessageTypeName("command.type.name", op.Command.GetTypeId()),
		telemetry.String("command.description", op.Command.GetDescription()),
	)
	defer func() {
		if err != nil {
			s.Telemetry.Error(ctx, "integration.handle_command.error", err)
		}
		span.End()
	}()

	if err := s.Seen.Add(ctx, op.Command.GetMessageId()); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.workerRequests <- workerRequest{op.Command}:
		s.pendingWorkerRequests++
	}

	return nil
}

func (s *Supervisor) doAppendEvents(ctx context.Context, op *wal.AppendEvents) (err error) {
	if len(op.Events) == 0 {
		panic("invalid operation: no events to append")
	}

	ctx, span := s.Telemetry.StartSpan(ctx, "integration.append_events")
	defer func() {
		if err != nil {
			s.Telemetry.Error(ctx, "integration.append_events.error", err)
		}
		span.End()
	}()

	req := eventstream.AppendRequest{
		CommandID:  op.CommandId,
		StreamID:   op.StreamId,
		Events:     op.Events,
		OffsetHint: eventstream.Offset(op.OffsetHint),
		Response:   s.appendResponses,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.AppendRequests <- req:
		s.pendingAppendRequests++
	}

	s.Telemetry.Info(ctx, "integration.append_events.done", "events appended to event stream successfully")

	return nil
}

func (s *Supervisor) commit(
	ctx context.Context,
	fn func(*wal.TransactionBuilder),
) error {
	txn := wal.NewTransactionBuilder()

	fn(txn)

	if err := s.WAL.Append(
		ctx,
		s.bounds.End,
		txn.Build(),
	); err != nil {
		return err
	}

	s.bounds.End++

	return nil
}
