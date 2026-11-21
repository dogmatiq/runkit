package integration_test

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCommandSink(t *testing.T) {
	t.Run("when the handler handles a command", func(t *testing.T) {
		t.Run("it passes the ID of the delivered message to the handler", func(t *testing.T) {
			t.Parallel()

			var got uuid.UUID

			db := xtesting.NewDatabase(t)
			sink := &CommandSink{
				Packer: &envelope.Packer{
					Application: identitypb.New("<app>", uuidpb.Generate()),
				},
				Handler: &stubs.IntegrationMessageHandlerStub{
					HandleCommandFunc: func(
						_ context.Context,
						s dogma.IntegrationCommandScope,
						_ dogma.Command,
					) error {
						got = s.MessageID()
						return nil
					},
				},
				Identity:      identitypb.New("<handler>", uuidpb.Generate()),
				HandleTimeout: time.Minute,
			}

			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			env := xtesting.PackCommand(t, stubs.CommandA1)
			logger := spruce.NewTestLogger(t)

			if err := sink.Deliver(
				t.Context(),
				tx,
				messagequeue.Delivery{},
				env,
				logger,
				logger,
			); err != nil {
				t.Fatal(err)
			}

			if want := env.GetBody().GetMessageId().AsStd(); got != want {
				t.Fatalf("unexpected message ID: got %s, want %s", got, want)
			}
		})
	})
}
