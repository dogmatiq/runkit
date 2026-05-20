package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	. "github.com/dogmatiq/reference-engine/internal/integration"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestController(t *testing.T) {
	db, _ := database.NewTestDB(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	controller := &Controller{
		Config: runtimeconfig.FromIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<integration>", "a1b2c3d4-e5f6-4890-abcd-ef1234567890")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					c dogma.Command,
				) error {
					if c, ok := c.(*stubs.CommandStub[stubs.TypeA]); ok {
						if c.Content == "fail" {
							return errors.New("<error>")
						}
					}
					s.RecordEvent(stubs.EventA1)
					return nil
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
		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				return commandqueue.Enqueue(
					ctx,
					tx,
					packer.PackCommand(stubs.CommandA1),
				)
			},
		)

		xtesting.WaitQuery(
			t,
			db,
			"empty command queue",
			`SELECT COUNT(*) = 0
			FROM command_queue`,
		)

		xtesting.ExpectEvents(
			t,
			eventstream.Read(
				t.Context(),
				db,
				0,
			),
			stubs.EventA1,
		)
	})

	t.Run("it nacks commands that cannot be unpacked", func(t *testing.T) {
		// Enqueue a command with corrupted message data so it cannot be unpacked.
		commandEnvelope := packer.PackCommand(stubs.CommandA1)
		commandEnvelope.GetBody().GetMessage().SetData([]byte("corrupted"))

		database.Transact(
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
			FROM command_queue
			WHERE message_id = $1
				AND next_attempt_at > now()`,
			database.MarshalUUID(commandEnvelope.GetBody().GetMessageId()),
		)

		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				// Remove the corrupted command from the queue so it doesn't
				// interfere with other tests.
				return commandqueue.Ack(
					ctx,
					tx,
					commandEnvelope.GetBody().GetMessageId(),
				)
			},
		)
	})

	t.Run("it nacks commands when the handler returns an error", func(t *testing.T) {
		commandEnvelope := packer.PackCommand(
			&stubs.CommandStub[stubs.TypeA]{Content: "fail"},
		)

		database.Transact(
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
			FROM command_queue
			WHERE message_id = $1
				AND next_attempt_at > now()`,
			database.MarshalUUID(commandEnvelope.GetBody().GetMessageId()),
		)

		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				// Remove the failed command from the queue so it doesn't
				// interfere with other tests.
				return commandqueue.Ack(
					ctx,
					tx,
					commandEnvelope.GetBody().GetMessageId(),
				)
			},
		)
	})
}
