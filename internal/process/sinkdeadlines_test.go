package process_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestSink_deadlineFailureRollsBackEffects verifies that a failed delivery does
// not commit effects and only the original deadline remains queued for retry.
func TestSink_deadlineFailureRollsBackEffects(t *testing.T) {
	for _, failure := range []string{"handle", "marshal"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()

			db := xtesting.NewDatabase(t)
			sink, handler := newSinkWithDeadlines(t, db)
			sink.BackoffBase = time.Minute
			sink.BackoffCap = 2 * time.Minute
			queueDeadline(t, db, sink, "<instance>", time.Now())

			handler.HandleDeadlineFunc = func(
				_ context.Context,
				_ dogma.ProcessRoot,
				s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
				_ dogma.Deadline,
			) error {
				s.Mutate(func(root dogma.ProcessRoot) {
					r := root.(*stubs.ProcessRootStub)
					r.Value = "<state>"

					if failure == "marshal" {
						r.MarshalBinaryFunc = func() ([]byte, error) {
							return nil, errors.New("<marshal failure>")
						}
					}
				})
				s.ExecuteCommand(stubs.CommandX1)
				s.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(time.Hour))

				if failure == "handle" {
					return errors.New("<failure>")
				}

				return nil
			}

			if err := sink.DeliverDeadline(t.Context(), "<instance>"); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"no committed commands",
				0,
				db,
				`SELECT COUNT(*)
				FROM commandqueue.commands`,
			)
			xtesting.ExpectQueryResult(
				t,
				"original deadline postponed",
				true,
				db,
				`SELECT failures = 1
					AND deliver_at > clock_timestamp()
				FROM process.deadlines`,
			)
			xtesting.ExpectQueryResult(
				t,
				"no committed state change",
				true,
				db,
				`SELECT state IS NULL
				FROM process.instances`,
			)

			handler.HandleDeadlineFunc = func(
				_ context.Context,
				_ dogma.ProcessRoot,
				s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
				_ dogma.Deadline,
			) error {
				s.ExecuteCommand(stubs.CommandX1)

				return nil
			}
			xtesting.ExecOne(
				t,
				db,
				`UPDATE process.deadlines SET
					deliver_at = $1`,
				time.Now(),
			)

			if err := sink.DeliverDeadline(t.Context(), "<instance>"); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"one committed command",
				1,
				db,
				`SELECT COUNT(*)
				FROM commandqueue.commands`,
			)
			xtesting.ExpectQueryResult(
				t,
				"deadline removed after success",
				0,
				db,
				`SELECT COUNT(*)
				FROM process.deadlines`,
			)
		})
	}
}

// TestSink_overlappingDeadlineDeliveries verifies that instance locking prevents
// duplicate committed deliveries from concurrent delivery attempts.
func TestSink_overlappingDeadlineDeliveries(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, handler := newSinkWithDeadlines(t, db)
	queueDeadline(t, db, sink, "<instance>", time.Now())
	var calls atomic.Int32
	handler.HandleDeadlineFunc = func(
		_ context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
		_ dogma.Deadline,
	) error {
		calls.Add(1)
		s.ExecuteCommand(stubs.CommandX1)

		return nil
	}

	var group sync.WaitGroup

	for range 2 {
		group.Go(func() {
			if err := sink.DeliverDeadline(t.Context(), "<instance>"); err != nil {
				t.Error(err)
			}
		})
	}

	group.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("unexpected handler calls: got %d, want 1", got)
	}

	xtesting.ExpectQueryResult(
		t,
		"one committed command",
		1,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
}

// newSinkWithDeadlines returns a process sink configured to handle deadlines.
func newSinkWithDeadlines(
	t testing.TB,
	db *sql.DB,
) (*process.Sink, *stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]) {
	t.Helper()

	sink := newSink(t, db)
	sink.OutboundMessageTypes = map[reflect.Type]struct{}{
		reflect.TypeOf(stubs.CommandX1):  {},
		reflect.TypeOf(stubs.DeadlineA1): {},
	}
	sink.DeadlineTypes = uuidcollection.NewSet(stubs.MessageTypeUUID[*stubs.DeadlineStub[stubs.TypeA]]())
	sink.BackoffBase = time.Millisecond
	sink.BackoffCap = time.Second

	handler := sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])

	return sink, handler
}

// queueDeadline schedules a deadline through the process event scope.
func queueDeadline(
	t testing.TB,
	db *sql.DB,
	events *process.Sink,
	instanceID string,
	at time.Time,
) {
	t.Helper()

	handler := events.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])
	handler.RouteEventToInstanceFunc = func(context.Context, dogma.Event) (string, bool, error) {
		return instanceID, true, nil
	}
	handler.HandleEventFunc = func(
		_ context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessEventScope[dogma.ProcessRoot],
		_ dogma.Event,
	) error {
		s.ScheduleDeadline(stubs.DeadlineA1, at)

		return nil
	}
	id, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)

	if _, ok, err := events.ConsumeSegment(
		t.Context(),
		0,
		eventstream.Segment{
			StreamID:  id,
			End:       1,
			Envelopes: envs,
		},
	); err != nil || !ok {
		t.Fatalf("unable to schedule deadline: %t, %v", ok, err)
	}
}
