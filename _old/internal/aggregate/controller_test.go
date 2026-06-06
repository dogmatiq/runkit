package aggregate_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	. "github.com/dogmatiq/reference-engine/internal/aggregate"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestController(t *testing.T) {
	db, _ := databasetest.New(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	controller := &Controller{
		Config: runtimeconfig.FromAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(c dogma.Command) string {
					switch c := c.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						return string(c.Content)
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					_ dogma.Command,
				) {
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
						Content: stubs.TypeA(
							fmt.Sprintf(
								"event #%d",
								len(r.AppliedEvents)+1,
							),
						),
					})
				},
			},
		),
		DB:     db,
		Packer: packer,
		Logger: spruce.NewTestLogger(t),
	}

	go func() {
		if err := controller.Run(t.Context()); err != nil {
			if err != t.Context().Err() {
				t.Error(err)
			}
		}
	}()

	t.Run("it handles commands that are routed to the handler", func(t *testing.T) {
		instanceID := "handle-test"

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				return commandqueue.Enqueue(
					ctx,
					tx,
					packer.PackCommand(
						&stubs.CommandStub[stubs.TypeA]{
							Content: stubs.TypeA(instanceID),
						},
					),
				)
			},
		)

		xtesting.WaitQuery(
			t,
			db,
			"empty command queue",
			`SELECT COUNT(*) = 0
			FROM pending_commands`,
		)

		xtesting.ExpectEvents(
			t,
			eventstream.ReadByAggregateInstance(
				t.Context(),
				db,
				xtesting.EventStreamByAggregateInstance(
					t,
					db,
					controller.Config.Identity().GetKey(),
					instanceID,
				),
				0,
				controller.Config.Identity().GetKey(),
				instanceID,
			),
			&stubs.EventStub[stubs.TypeA]{
				Content: "event #1",
			},
		)
	})

	t.Run("it reroutes a command when RouteCommandToInstance() returns a different instance", func(t *testing.T) {
		correctInstanceID := "reroute-correct"
		incorrectInstanceID := "reroute-incorrect"

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				commandEnvelope := packer.PackCommand(
					&stubs.CommandStub[stubs.TypeA]{
						Content: stubs.TypeA(correctInstanceID),
					},
				)

				if err := commandqueue.Enqueue(ctx, tx, commandEnvelope); err != nil {
					return err
				}

				eventStreamID, err := eventstream.Acquire(ctx, tx)
				if err != nil {
					return err
				}

				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO aggregate_instances (
						handler_key,
						instance_id,
						event_stream_id
					) VALUES ($1, $2, $3)`,
					database.MarshalUUID(controller.Config.Identity().GetKey()),
					incorrectInstanceID,
					database.MarshalUUID(eventStreamID),
				); err != nil {
					return err
				}

				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO aggregate_command_routes (
						message_id,
						handler_key,
						instance_id
					) VALUES ($1, $2, $3)`,
					database.MarshalUUID(commandEnvelope.GetBody().GetMessageId()),
					database.MarshalUUID(controller.Config.Identity().GetKey()),
					incorrectInstanceID,
				); err != nil {
					return err
				}

				return nil
			},
		)

		xtesting.WaitQuery(
			t,
			db,
			"empty command queue",
			`SELECT COUNT(*) = 0
			FROM pending_commands`,
		)

		xtesting.ExpectEvents(
			t,
			eventstream.ReadByAggregateInstance(
				t.Context(),
				db,
				xtesting.EventStreamByAggregateInstance(
					t,
					db,
					controller.Config.Identity().GetKey(),
					correctInstanceID,
				),
				0,
				controller.Config.Identity().GetKey(),
				correctInstanceID,
			),
			&stubs.EventStub[stubs.TypeA]{
				Content: "event #1",
			},
		)
	})

	t.Run("it nacks commands that cannot be unpacked", func(t *testing.T) {
		// Enqueue a command with corrupted message data so it cannot be unpacked.
		commandEnvelope := packer.PackCommand(stubs.CommandA1)
		commandEnvelope.GetBody().GetMessage().SetData([]byte("corrupted"))

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				return commandqueue.Enqueue(ctx, tx, commandEnvelope)
			},
		)

		xtesting.WaitQuery(
			t,
			db,
			"command nack'd",
			`SELECT
				COUNT(*) != 0
			FROM pending_commands
			WHERE message_id = $1
				AND next_attempt_at > now()`,
			database.MarshalUUID(commandEnvelope.GetBody().GetMessageId()),
		)

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				// Remove the corrupted command from the queue so it doesn't
				// interfere with other tests.
				return commandqueue.Dequeue(
					ctx,
					tx,
					commandEnvelope.GetBody().GetMessageId(),
				)
			},
		)
	})

	t.Run("it uses snapshots", func(t *testing.T) {
		instanceID := "snapshot-test"

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				return commandqueue.Enqueue(
					ctx,
					tx,
					packer.PackCommand(
						&stubs.CommandStub[stubs.TypeA]{
							Content: stubs.TypeA(instanceID),
						},
					),
				)
			},
		)

		xtesting.WaitQuery(
			t,
			db,
			"snapshot saved",
			`SELECT snapshot IS NOT NULL
			FROM aggregate_instances
			WHERE handler_key = $1
				AND instance_id = $2`,
			database.MarshalUUID(controller.Config.Identity().GetKey()),
			instanceID,
		)

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(
					ctx,
					`DELETE FROM events
					WHERE aggregate_handler_key = $1
						AND aggregate_instance_id = $2`,
					database.MarshalUUID(controller.Config.Identity().GetKey()),
					instanceID,
				)
				return err
			},
		)

		databasetest.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				return commandqueue.Enqueue(
					ctx,
					tx,
					packer.PackCommand(
						&stubs.CommandStub[stubs.TypeA]{
							Content: stubs.TypeA(instanceID),
						},
					),
				)
			},
		)

		xtesting.WaitQuery(
			t,
			db,
			"empty command queue",
			`SELECT COUNT(*) = 0
			FROM pending_commands`,
		)

		xtesting.ExpectEvents(
			t,
			eventstream.ReadByAggregateInstance(
				t.Context(),
				db,
				xtesting.EventStreamByAggregateInstance(
					t,
					db,
					controller.Config.Identity().GetKey(),
					instanceID,
				),
				0,
				controller.Config.Identity().GetKey(),
				instanceID,
			),
			&stubs.EventStub[stubs.TypeA]{
				Content: "event #2",
			},
		)
	})
}
