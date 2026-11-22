package commandexecutor_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/commandexecutor"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestExecutor(t *testing.T) {
	// setup returns an executor for *stubs.CommandStub[stubs.TypeA] commands
	// over a new database, and the database.
	setup := func(t *testing.T) (*Executor, *sql.DB) {
		db := xtesting.NewDatabase(t)
		testLogger := spruce.NewTestLogger(t)

		node := &cluster.Node{
			DB:                  db,
			ID:                  uuid.New(),
			HeartbeatInterval:   20 * time.Millisecond,
			HeartbeatTimeout:    100 * time.Millisecond,
			EngineFailurePolicy: xtesting.FailurePolicy(),
			EngineLogger:        testLogger,
		}

		xtesting.GoE(t, node.Run)

		executor := &Executor{
			DB:   db,
			Node: node,
			Packer: &envelope.Packer{
				Application: identitypb.New("<app>", uuidpb.Generate()),
			},
			QueueRouter: &messagequeue.Router{
				NodeID: node.ID,
				CommandHandlers: map[uuid.UUID]messagequeue.CommandHandler{
					stubs.MessageTypeID[*stubs.CommandStub[stubs.TypeA]](): {
						Key: uuid.New(),
					},
				},
			},
			CommandTypes: map[reflect.Type]struct{}{
				reflect.TypeFor[*stubs.CommandStub[stubs.TypeA]](): {},
			},
			EngineLogger: testLogger,
			AppLogger:    testLogger,
		}

		return executor, db
	}

	t.Run("func ExecuteCommand()", func(t *testing.T) {
		t.Run("when an event observer is given", func(t *testing.T) {
			t.Run("it passes each event with the command's correlation ID to the observer once", func(t *testing.T) {
				executor, db := setup(t)

				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()

				// Execute the command once without an observer, to learn its
				// correlation ID.
				var correlationID uuid.UUID

				if err := executor.ExecuteCommand(
					contexthook.With(
						ctx,
						func(h contexthook.ExecuteCommand) {
							correlationID = h.CommandEnvelope.GetHeader().GetCorrelationId().AsStd()
						},
					),
					stubs.CommandA1,
					dogma.WithIdempotencyKey("<key>"),
				); err != nil {
					t.Fatal(err)
				}

				// Record more events than one read returns, spread across
				// several streams and mixed with events that have other
				// correlation IDs.
				const want = 250
				streamIDs := xtesting.CreateEventStreams(t, db, 3)

				for i := range want {
					streamID := streamIDs[i%len(streamIDs)]

					xtesting.AppendCorrelatedEvents(
						t,
						db,
						streamID,
						correlationID,
						&stubs.EventStub[stubs.TypeE]{Content: stubs.TypeE(fmt.Sprint(i))},
					)

					xtesting.AppendToEventStream(t, db, streamID, stubs.EventE1)
				}

				// Execute the command again, so that it's deduplicated and the
				// observer sees the events recorded above.
				seen := map[stubs.TypeE]int{}

				if err := executor.ExecuteCommand(
					ctx,
					stubs.CommandA1,
					dogma.WithIdempotencyKey("<key>"),
					dogma.WithEventObserver(
						func(_ context.Context, e *stubs.EventStub[stubs.TypeE]) (bool, error) {
							seen[e.Content]++
							return len(seen) == want, nil
						},
					),
				); err != nil {
					t.Fatal(err)
				}

				for i := range want {
					content := stubs.TypeE(fmt.Sprint(i))

					if n := seen[content]; n != 1 {
						t.Fatalf("event %q was observed %d times, want 1", content, n)
					}
				}
			})

			t.Run("it passes events recorded while it waits", func(t *testing.T) {
				executor, db := setup(t)
				streamIDs := xtesting.CreateEventStreams(t, db, 2)

				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()

				correlationIDs := make(chan uuid.UUID, 1)
				firstObserved := make(chan struct{})
				result := make(chan error, 1)

				go func() {
					result <- executor.ExecuteCommand(
						contexthook.With(
							ctx,
							func(h contexthook.ExecuteCommand) {
								correlationIDs <- h.CommandEnvelope.GetHeader().GetCorrelationId().AsStd()
							},
						),
						stubs.CommandA1,
						dogma.WithEventObserver(
							func(_ context.Context, e *stubs.EventStub[stubs.TypeE]) (bool, error) {
								if e.Content == "first" {
									close(firstObserved)
									return false, nil
								}

								return e.Content == "second", nil
							},
						),
					)
				}()

				correlationID := <-correlationIDs

				xtesting.AppendCorrelatedEvents(
					t,
					db,
					streamIDs[0],
					correlationID,
					&stubs.EventStub[stubs.TypeE]{Content: "first"},
				)

				select {
				case <-firstObserved:
				case err := <-result:
					t.Fatalf("ExecuteCommand returned before observing the first event: %v", err)
				}

				xtesting.AppendCorrelatedEvents(
					t,
					db,
					streamIDs[1],
					correlationID,
					&stubs.EventStub[stubs.TypeE]{Content: "second"},
				)

				if err := <-result; err != nil {
					t.Fatal(err)
				}
			})
		})
	})
}
