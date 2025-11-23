package integration

import (
	"context"
	"slices"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/integration/internal/wal"
	"github.com/dogmatiq/runkit/internal/telemetry"
)

// reconcile discovers and completes any pending operations in the WAL.
func (s *Supervisor) reconcile(
	ctx context.Context,
) error {
	ctx, span := s.Telemetry.StartSpan(ctx, "integration.reconcile")
	defer span.End()

	s.Telemetry.Info(ctx, "integration.reconcile", "loading pending operations from the write-ahead log")

	unhandled, unappended, err := s.loadPendingOperations(ctx)
	if err != nil {
		return err
	}

	span.SetAttributes(
		telemetry.Int("pending_handle_command_ops", len(unhandled)),
		telemetry.Int("pending_append_events_ops", len(unappended)),
	)

	if len(unhandled) == 0 && len(unappended) == 0 {
		s.Telemetry.Info(
			ctx,
			"integration.reconcile.done",
			"the write-ahead log does not contain any pending operations",
		)

		return nil
	}

	if len(unappended) != 0 {
		events := 0
		for _, op := range unappended {
			events += len(op.GetEvents())
		}

		s.Telemetry.Info(
			ctx,
			"integration.reconcile.found_pending_append_events_ops",
			"integration has produced events that have not yet been appended to an event stream",
			telemetry.Int("op_count", len(unappended)),
			telemetry.Int("event_count", events),
		)

		for _, op := range unappended {
			if err := s.appendEvents(ctx, op); err != nil {
				return err
			}
		}
	}

	if len(unhandled) != 0 {
		s.Telemetry.Info(
			ctx,
			"integration.reconcile.found_pending_handle_command_ops",
			"integration has accepted commands that have not yet been handled",
			telemetry.Int("op_count", len(unhandled)),
		)

		for _, op := range unhandled {
			if err := s.handleCommand(ctx, op); err != nil {
				return err
			}
		}
	}

	s.Telemetry.Info(
		ctx,
		"integration.reconcile.done",
		"all pending operations in the write-ahead log are now complete",
	)

	return nil
}

func (s *Supervisor) loadPendingOperations(ctx context.Context) (
	unhandled []*wal.HandleCommand,
	unappended []*wal.AppendEvents,
	err error,
) {
	s.bounds, err = s.WAL.Bounds(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Range over the journal to build a list of pending operations.
	if !s.bounds.IsEmpty() {
		err = s.WAL.Range(
			ctx,
			s.bounds.Begin,
			func(
				_ context.Context,
				_ journal.Position,
				txn *wal.Transaction,
			) (ok bool, err error) {
				wal.MustSwitch_Transaction_Op(
					txn,
					func(op *wal.HandleCommand) {
						unhandled = append(unhandled, op)
					},
					func(op *wal.AppendEvents) {
						for i, x := range unhandled {
							if x.GetCommand().GetMessageId().Equal(op.GetCommandId()) {
								unhandled = slices.Delete(unhandled, i, i+1)
								break
							}
						}
						unappended = append(unappended, op)
					},
					func(op *wal.MarkEventsAsAppended) {
						for i, x := range unappended {
							if x.GetCommandId().Equal(op.GetCommandId()) {
								unappended = slices.Delete(unappended, i, i+1)
								break
							}
						}
					},
				)

				return true, err
			},
		)
	}

	return unhandled, unappended, err
}
