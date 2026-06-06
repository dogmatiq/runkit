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
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	. "github.com/dogmatiq/reference-engine/internal/integration"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestController(t *testing.T) {
	db, _ := databasetest.New(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	handlerKey := uuidpb.MustParse("a1b2c3d4-e5f6-4890-abcd-ef1234567890")

	controller := &Controller{
		Config: runtimeconfig.FromIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<integration>", handlerKey.AsString())
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
		databasetest.Transact(t, db, func(tx *sql.Tx) {
			if err := commandqueue.Enqueue(
				t.Context(),
				tx,
				packer.PackCommand(stubs.CommandA1),
				handlerKey,
				nil,
			); err != nil {
				t.Fatal(err)
			}
		})

		databasetest.WaitUntil(
			t,
			db,
			"empty command queue",
			`SELECT COUNT(*) = 0
			FROM pending_commands`,
		)

		offsets, err := eventstream.Offsets(t.Context(), db)
		if err != nil {
			t.Fatal(err)
		}

		var eventStreamID *uuidpb.UUID
		for id := range offsets.Keys() {
			if eventStreamID != nil {
				t.Fatal("expected exactly one event stream")
			}
			eventStreamID = id
		}

		xtesting.ExpectEvents(
			t,
			eventstream.Read(
				t.Context(),
				db,
				eventStreamID,
				0,
				nil,
			),
			stubs.EventA1,
		)
	})

	t.Run("it backs off when a command cannot be handled", func(t *testing.T) {
		envelopeWithInvalidMessageData := packer.PackCommand(&stubs.CommandStub[stubs.TypeA]{})
		envelopeWithInvalidMessageData.GetBody().GetMessage().SetData([]byte("invalid"))

		cases := []struct {
			Name     string
			Envelope *envelopepb.Envelope
		}{
			{
				Name:     "command cannot be unpacked",
				Envelope: envelopeWithInvalidMessageData,
			},
			{
				Name: "no route for command type",
				Envelope: packer.PackCommand(
					// The handler has no route for [stubs.TypeB] commands.
					&stubs.CommandStub[stubs.TypeB]{},
				),
			},
			{
				Name: "handler returns an error",
				Envelope: packer.PackCommand(
					&stubs.CommandStub[stubs.TypeA]{
						// The "fail" value causes the handler stub to return an
						// error.
						Content: "fail",
					},
				),
			},
		}

		for _, c := range cases {
			t.Run(c.Name, func(t *testing.T) {
				commandMessageID := c.Envelope.GetBody().GetMessageId()

				databasetest.Transact(t, db, func(tx *sql.Tx) {
					if err := commandqueue.Enqueue(
						t.Context(),
						tx,
						c.Envelope,
						handlerKey,
						nil,
					); err != nil {
						t.Fatal(err)
					}
				})

				// Remove the unhandled command from the queue so it doesn't
				// interfere with other tests.
				defer func() {
					databasetest.Transact(t, db, func(tx *sql.Tx) {
						if err := commandqueue.Dequeue(
							t.Context(),
							tx,
							commandMessageID,
						); err != nil {
							t.Fatal(err)
						}
					})
				}()

				databasetest.WaitUntil(
					t,
					db,
					"back-off applied",
					`SELECT
						COUNT(*) != 0
					FROM pending_commands
					WHERE message_id = $1
						AND next_attempt_at > now()`,
					database.MarshalUUID(commandMessageID),
				)
			})
		}
	})
}
