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

// TestDeadlineSink_failureRollsBackEffects verifies that a failed delivery does
// not commit effects and only the original deadline remains queued for retry.
func TestDeadlineSink_failureRollsBackEffects(t *testing.T) {
	for _, failure := range []string{"handle", "marshal"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()

			db := xtesting.NewDatabase(t)
			events, sink, handler := newDeadlineTestSink(t, db)
			sink.BackoffBase = time.Minute
			sink.BackoffCap = 2 * time.Minute
			queueDeadline(t, db, events, "<instance>", time.Now())

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
			if err := sink.Deliver(t.Context(), "<instance>"); err != nil {
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
			if err := sink.Deliver(t.Context(), "<instance>"); err != nil {
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

// TestDeadlineSink_overlappingOwners verifies that instance locking prevents
// duplicate committed deliveries while cluster views disagree during handoff.
func TestDeadlineSink_overlappingOwners(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	events, sink, handler := newDeadlineTestSink(t, db)
	queueDeadline(t, db, events, "<instance>", time.Now())
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
			if err := sink.Deliver(t.Context(), "<instance>"); err != nil {
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

// newDeadlineTestSink returns event and deadline sinks for one process handler.
func newDeadlineTestSink(
	t testing.TB,
	db *sql.DB,
) (*process.Sink, *process.DeadlineSink, *stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]) {
	t.Helper()

	events := newSink(t, db)
	events.OutboundMessageTypes = map[reflect.Type]struct{}{
		reflect.TypeOf(stubs.CommandX1):  {},
		reflect.TypeOf(stubs.DeadlineA1): {},
	}
	handler := events.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])
	sink := &process.DeadlineSink{
		DB:                   db,
		Handler:              handler,
		Identity:             events.Identity,
		Packer:               events.Packer,
		DeadlineTypeIDs:      uuidcollection.NewSet(stubs.MessageTypeUUID[*stubs.DeadlineStub[stubs.TypeA]]()),
		OutboundMessageTypes: events.OutboundMessageTypes,
		HandleTimeout:        time.Second,
		Concurrency:          2,
		BackoffBase:          time.Millisecond,
		BackoffCap:           time.Second,
		Logger:               events.Logger,
	}
	return events, sink, handler
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
	handler.HandleEventFunc = func(_ context.Context, _ dogma.ProcessRoot, s dogma.ProcessEventScope[dogma.ProcessRoot], _ dogma.Event) error {
		s.ScheduleDeadline(stubs.DeadlineA1, at)
		return nil
	}
	id, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
	if _, ok, err := events.ConsumeSegment(t.Context(), 0, eventstream.Segment{StreamID: id, End: 1, Envelopes: envs}); err != nil || !ok {
		t.Fatalf("unable to schedule deadline: %t, %v", ok, err)
	}
}
