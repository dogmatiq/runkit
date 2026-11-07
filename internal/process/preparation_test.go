package process_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// A preparation error must leave only the original deadline and its retry;
// commands, successor deadlines and state are committed only on success.
func TestPreparation_deadlineSerializationFailureDoesNotCommitEffects(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			var fail atomic.Bool
			fail.Store(true)
			const handlerKey = "ba44b051-1665-4f38-a37a-a45a0b964172"
			xtesting.RunEngines(t, func(t testing.TB, _ *xtesting.Cluster, db *sql.DB) {
				xtesting.PopulateEventStreams(t, db, func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				}, 1)
				xtesting.WaitForHandlerToConsumeAllEvents(t, db, handlerKey)
				xtesting.WaitForQueryResult(t, "failed deadline postponed", true, db,
					`SELECT EXISTS (SELECT 1 FROM process.deadlines WHERE failures > 0)`)
				xtesting.ExpectQueryResult(t, "no partial commands", 0, db,
					`SELECT COUNT(*) FROM commandqueue.commands`)
				xtesting.ExpectQueryResult(t, "only original deadline remains", 1, db,
					`SELECT COUNT(*) FROM process.deadlines`)
				xtesting.ExpectQueryResult(t, "state unchanged", "initial", db,
					`SELECT convert_from(state, 'UTF8')::jsonb ->> 'value' FROM process.instances`)

				fail.Store(false)
				xtesting.WaitForQueryResult(t, "successful retry committed command", 1, db,
					`SELECT COUNT(*) FROM commandqueue.commands`)
				xtesting.ExpectQueryResult(t, "successor committed once", 1, db,
					`SELECT COUNT(*) FROM process.deadlines WHERE failures = 0`)
				xtesting.ExpectQueryResult(t, "original consumed", 0, db,
					`SELECT COUNT(*) FROM process.deadlines WHERE failures > 0`)
				xtesting.ExpectQueryResult(t, "state committed", "changed", db,
					`SELECT convert_from(state, 'UTF8')::jsonb ->> 'value' FROM process.instances`)
			}, dogma.ViaProcess(&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("process", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeB]](),
					)
				},
				NewFunc: func() *stubs.ProcessRootStub {
					root := &stubs.ProcessRootStub{}
					root.MarshalBinaryFunc = func() ([]byte, error) {
						if root.Value == "changed" && fail.Load() {
							if failure == "panic" {
								panic("serialization failed")
							}
							return nil, errors.New("serialization failed")
						}
						return json.Marshal(root)
					}
					return root
				},
				RouteEventToInstanceFunc: func(context.Context, dogma.Event) (string, bool, error) {
					return "instance", true, nil
				},
				HandleEventFunc: func(_ context.Context, _ *stubs.ProcessRootStub, scope dogma.ProcessEventScope[*stubs.ProcessRootStub], _ dogma.Event) error {
					scope.Mutate(func(root *stubs.ProcessRootStub) { root.Value = "initial" })
					scope.ScheduleDeadline(stubs.DeadlineA1, time.Now())
					return nil
				},
				HandleDeadlineFunc: func(_ context.Context, _ *stubs.ProcessRootStub, scope dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], _ dogma.Deadline) error {
					scope.Mutate(func(root *stubs.ProcessRootStub) { root.Value = "changed" })
					scope.ExecuteCommand(stubs.CommandA1)
					scope.ScheduleDeadline(stubs.DeadlineB1, time.Now().Add(time.Hour))
					return nil
				},
			}))
		})
	}
}
