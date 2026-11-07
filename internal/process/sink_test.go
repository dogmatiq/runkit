package process_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

// TestSink_filteredEventsAdvanceCheckpoint verifies that ranges without owned
// events are durably consumed, including streams absent from the local database.
func TestSink_filteredEventsAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	s := newSink(t, xtesting.NewDatabase(t))
	streamID := uuidpb.Generate()
	cp, err := s.CheckpointOffset(t.Context(), streamID)
	if err != nil || cp != 0 {
		t.Fatalf("unexpected initial checkpoint: %d, %v", cp, err)
	}

	next, ok, err := s.ConsumeSegment(t.Context(), cp, eventstream.Segment{
		StreamID: streamID,
		End:      5,
	})
	if err != nil || !ok || next != 5 {
		t.Fatalf("unexpected consumption result: %d, %t, %v", next, ok, err)
	}

	cp, err = s.CheckpointOffset(t.Context(), streamID)
	if err != nil || cp != 5 {
		t.Fatalf("unexpected durable checkpoint: %d, %v", cp, err)
	}
}

// TestSink_overlappingConsumers verifies that competing consumers cannot commit
// the effects of the same event twice, even when no checkpoint exists yet.
func TestSink_overlappingConsumers(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	s := newSink(t, db)
	streamID, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
	var calls int
	s.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]).HandleEventFunc = func(
		context.Context,
		dogma.ProcessRoot,
		dogma.ProcessEventScope[dogma.ProcessRoot],
		dogma.Event,
	) error {
		calls++
		return nil
	}

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			next, ok, err := s.ConsumeSegment(t.Context(), 0, eventstream.Segment{
				StreamID:  streamID,
				End:       1,
				Envelopes: envs,
			})
			if err != nil || (ok && next != 1) {
				t.Errorf("unexpected consumption result: %d, %t, %v", next, ok, err)
			}
		})
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("unexpected number of handler calls: got %d, want 1", calls)
	}
}

// TestSink_failureRollsBack verifies that a failed attempt leaves no instance
// state or checkpoint progress, and can be retried from the same segment.
func TestSink_failureRollsBack(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			s := newSink(t, db)
			streamID, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
			seg := eventstream.Segment{StreamID: streamID, End: 1, Envelopes: envs}
			handler := s.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])
			handler.HandleEventFunc = func(
				context.Context,
				dogma.ProcessRoot,
				dogma.ProcessEventScope[dogma.ProcessRoot],
				dogma.Event,
			) error {
				if failure == "panic" {
					panic("<failure>")
				}
				return errors.New("<failure>")
			}

			if _, _, err := s.ConsumeSegment(t.Context(), 0, seg); err == nil {
				t.Fatal("expected a consumption error")
			}

			xtesting.ExpectQueryResult(
				t,
				"rolled back instances",
				0,
				db,
				`SELECT COUNT(*)
				FROM process.instances`,
			)
			xtesting.ExpectQueryResult(
				t,
				"rolled back checkpoints",
				0,
				db,
				`SELECT COUNT(*)
				FROM eventstream.handler_checkpoints`,
			)

			handler.HandleEventFunc = nil
			next, ok, err := s.ConsumeSegment(t.Context(), 0, seg)
			if err != nil || !ok || next != 1 {
				t.Fatalf("unexpected retry result: %d, %t, %v", next, ok, err)
			}
		})
	}
}

// newSink returns a sink whose handler routes every event to the same instance.
func newSink(t testing.TB, db *sql.DB) *Sink {
	t.Helper()

	return &Sink{
		DB: db,
		Handler: &stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]{
			NewFunc: func() dogma.ProcessRoot { return &stubs.ProcessRootStub{} },
			RouteEventToInstanceFunc: func(context.Context, dogma.Event) (string, bool, error) {
				return "<instance>", true, nil
			},
		},
		Identity: identitypb.New("<handler>", uuidpb.Generate()),
		Packer: &envelope.Packer{
			Application: identitypb.New("<app>", uuidpb.Generate()),
		},
		HandleTimeout: time.Minute,
		RouteTimeout:  10 * time.Second,
		Logger:        spruce.NewTestLogger(t),
	}
}

// TestSink_routingRejectionsAdvancePastTrailingEvents verifies that routing
// rejections and filtered events at the end of a segment both advance progress.
func TestSink_routingRejectionsAdvancePastTrailingEvents(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	s := newSink(t, db)
	s.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]).RouteEventToInstanceFunc = func(context.Context, dogma.Event) (string, bool, error) {
		return "", false, nil
	}
	streamID, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
	next, ok, err := s.ConsumeSegment(t.Context(), 0, eventstream.Segment{
		StreamID:  streamID,
		End:       8,
		Envelopes: envs,
	})
	if err != nil || !ok || next != 8 {
		t.Fatalf("unexpected consumption result: %d, %t, %v", next, ok, err)
	}

	cp, err := s.CheckpointOffset(t.Context(), streamID)
	if err != nil || cp != 8 {
		t.Fatalf("unexpected durable checkpoint: %d, %v", cp, err)
	}
	xtesting.ExpectQueryResult(
		t,
		"no instance created",
		0,
		db,
		`SELECT COUNT(*)
		FROM process.instances`,
	)
}

// TestSink_checkpointFailureRollsBackEffects verifies that commands, deadlines,
// instance state, and checkpoint progress commit together.
func TestSink_checkpointFailureRollsBackEffects(t *testing.T) {
	t.Parallel()

	var fail atomic.Bool
	db := xtesting.NewDatabaseWithFailingQueries(t, func(query string, _ []any) bool {
		return fail.Load() && strings.Contains(query, "UPDATE eventstream.handler_checkpoints")
	})
	s := newSink(t, db)
	s.OutboundMessageTypes = map[reflect.Type]struct{}{
		reflect.TypeOf(stubs.CommandX1):  {},
		reflect.TypeOf(stubs.DeadlineA1): {},
	}
	s.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]).HandleEventFunc = func(
		_ context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessEventScope[dogma.ProcessRoot],
		_ dogma.Event,
	) error {
		s.Mutate(func(root dogma.ProcessRoot) {
			root.(*stubs.ProcessRootStub).Value = "<state>"
		})
		s.ExecuteCommand(stubs.CommandX1)
		s.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(time.Hour))
		return nil
	}
	streamID, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
	seg := eventstream.Segment{StreamID: streamID, End: 1, Envelopes: envs}
	fail.Store(true)
	if _, _, err := s.ConsumeSegment(t.Context(), 0, seg); err == nil {
		t.Fatal("expected checkpoint update to fail")
	}

	for _, table := range []string{"process.instances", "process.deadlines", "eventstream.handler_checkpoints", "commandqueue.commands"} {
		xtesting.ExpectQueryResult(
			t,
			"rolled back "+table,
			0,
			db,
			`SELECT COUNT(*)
            FROM `+table,
		)
	}

	fail.Store(false)
	next, ok, err := s.ConsumeSegment(t.Context(), 0, seg)
	if err != nil || !ok || next != 1 {
		t.Fatalf("unexpected retry result: %d, %t, %v", next, ok, err)
	}
	for _, table := range []string{"process.instances", "process.deadlines", "eventstream.handler_checkpoints", "commandqueue.commands"} {
		xtesting.ExpectQueryResult(
			t,
			"committed "+table,
			1,
			db,
			`SELECT COUNT(*)
            FROM `+table,
		)
	}
}

// TestSink_handlerDeadlines verifies that the process sink applies the engine's
// routing and handling time limits when it invokes application code.
func TestSink_handlerDeadlines(t *testing.T) {
	for _, operation := range []string{"route", "handle"} {
		t.Run(operation, func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			s := newSink(t, db)
			handler := s.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])

			if operation == "route" {
				s.RouteTimeout = time.Nanosecond
				handler.RouteEventToInstanceFunc = func(ctx context.Context, _ dogma.Event) (string, bool, error) {
					<-ctx.Done()
					return "", false, ctx.Err()
				}
			} else {
				s.HandleTimeout = time.Nanosecond
				handler.HandleEventFunc = func(
					ctx context.Context,
					_ dogma.ProcessRoot,
					_ dogma.ProcessEventScope[dogma.ProcessRoot],
					_ dogma.Event,
				) error {
					<-ctx.Done()
					return ctx.Err()
				}
			}

			streamID, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
			_, _, err := s.ConsumeSegment(t.Context(), 0, eventstream.Segment{
				StreamID:  streamID,
				End:       1,
				Envelopes: envs,
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected a deadline error, got %v", err)
			}

			cp, err := s.CheckpointOffset(t.Context(), streamID)
			if err != nil || cp != 0 {
				t.Fatalf("unexpected checkpoint after timeout: %d, %v", cp, err)
			}
		})
	}
}
