package aggregate_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/aggregate"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCommandSink(t *testing.T) {
	t.Run("when the handler routes the command to an empty instance ID", func(t *testing.T) {
		t.Run("it returns a deterministic error", func(t *testing.T) {
			t.Parallel()

			db := xtesting.NewDatabase(t)
			sink := &CommandSink{
				Packer: &envelope.Packer{
					Application: identitypb.New("<app>", uuidpb.Generate()),
				},
				Handler: &stubs.AggregateMessageHandlerStub[dogma.AggregateRoot]{
					RouteCommandToInstanceFunc: func(dogma.Command) string {
						return ""
					},
				},
				Identity: identitypb.New("<handler>", uuidpb.Generate()),
			}

			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			err = sink.Deliver(
				t.Context(),
				tx,
				messagequeue.Delivery{},
				xtesting.PackCommand(t, stubs.CommandA1),
				spruce.NewTestLogger(t),
				spruce.NewTestLogger(t),
			)

			if !xerrors.IsDeterministic(err) {
				t.Fatalf("expected a deterministic error, got %v", err)
			}
		})
	})
}
