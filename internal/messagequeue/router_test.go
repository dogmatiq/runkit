package messagequeue_test

import (
	"database/sql"
	"testing"
	"uuid"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestRouter(t *testing.T) {
	aggregateKey := uuid.New()
	integrationKey := uuid.New()

	// commandTypeID returns the ID of the given command's type.
	commandTypeID := func(c dogma.Command) uuid.UUID {
		mt, _ := dogma.RegisteredMessageTypeOf(c)
		return mt.ID()
	}

	// setup returns a router for a node with three handlers:
	//
	//   - an aggregate that handles commands of type A, and routes each one to
	//     the instance that route returns
	//   - an integration that minimizes concurrency, and handles commands of
	//     type B
	//   - an integration that maximizes concurrency, and handles commands of
	//     type C
	setup := func(t *testing.T, route func(dogma.Command) string) (*sql.DB, *Router) {
		db := xtesting.NewDatabase(t)

		handlers := map[uuid.UUID]CommandHandler{
			commandTypeID(stubs.CommandA1): {
				Key: aggregateKey,
				Aggregate: &stubs.AggregateMessageHandlerStub[dogma.AggregateRoot]{
					RouteCommandToInstanceFunc: route,
				},
			},
			commandTypeID(stubs.CommandB1): {
				Key:                   integrationKey,
				ConcurrencyPreference: dogma.MinimizeConcurrency,
			},
			commandTypeID(stubs.CommandC1): {
				Key:                   uuid.New(),
				ConcurrencyPreference: dogma.MaximizeConcurrency,
			},
		}

		router := &Router{
			NodeID:          uuid.New(),
			CommandHandlers: handlers,
		}

		return db, router
	}

	// insertInstance inserts an aggregate instance for the handler with the
	// given key. If bound is true, it binds the instance to a new event stream
	// and returns the stream's ID.
	insertInstance := func(
		t *testing.T,
		db *sql.DB,
		handlerKey uuid.UUID,
		instanceID string,
		bound bool,
	) uuid.UUID {
		t.Helper()

		var streamID uuid.UUID

		if bound {
			if err := db.QueryRowContext(
				t.Context(),
				`SELECT runkit.acquire_event_stream()`,
			).Scan(xsql.UUID(&streamID)); err != nil {
				t.Fatal(err)
			}
		}

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO runkit.aggregate_instances (
				handler_key,
				instance_id,
				stream_id
			) VALUES ($1, $2, $3)`,
			xsql.UUID(handlerKey),
			instanceID,
			xsql.UUID(streamID),
		); err != nil {
			t.Fatal(err)
		}

		return streamID
	}

	packer := &envelope.Packer{
		Application: identitypb.New("<app>", uuidpb.Generate()),
	}

	routeToInstance := func(dogma.Command) string { return "<instance>" }

	t.Run("RouteCommandToQueue", func(t *testing.T) {
		t.Run("it returns the ID of the event stream that the target instance is bound to", func(t *testing.T) {
			t.Parallel()

			db, router := setup(t, routeToInstance)
			streamID := insertInstance(t, db, aggregateKey, "<instance>", true)

			got, err := router.RouteCommandToQueue(t.Context(), db, packer.PackCommand(stubs.CommandA1))
			if err != nil {
				t.Fatal(err)
			}

			if got != streamID {
				t.Fatalf("unexpected queue ID: got %s, want %s", got, streamID)
			}
		})

		t.Run("it returns the integration's identity key if an integration that minimizes concurrency handles the command", func(t *testing.T) {
			t.Parallel()

			db, router := setup(t, routeToInstance)

			got, err := router.RouteCommandToQueue(t.Context(), db, packer.PackCommand(stubs.CommandB1))
			if err != nil {
				t.Fatal(err)
			}

			if got != integrationKey {
				t.Fatalf("unexpected queue ID: got %s, want %s", got, integrationKey)
			}
		})

		cases := []struct {
			Name    string
			Route   func(dogma.Command) string
			Setup   func(*testing.T, *sql.DB)
			Command dogma.Command
		}{
			{
				Name:    "it returns the node ID when the target instance isn't bound to an event stream",
				Route:   routeToInstance,
				Setup:   func(t *testing.T, db *sql.DB) { insertInstance(t, db, aggregateKey, "<instance>", false) },
				Command: stubs.CommandA1,
			},
			{
				Name:    "it returns the node ID when the target instance doesn't exist",
				Route:   routeToInstance,
				Setup:   func(*testing.T, *sql.DB) {},
				Command: stubs.CommandA1,
			},
			{
				Name:    "it returns the node ID when only another aggregate has an instance with the target ID",
				Route:   routeToInstance,
				Setup:   func(t *testing.T, db *sql.DB) { insertInstance(t, db, uuid.New(), "<instance>", true) },
				Command: stubs.CommandA1,
			},
			{
				Name:    "it returns the node ID if an integration that maximizes concurrency handles the command",
				Route:   routeToInstance,
				Setup:   func(*testing.T, *sql.DB) {},
				Command: stubs.CommandC1,
			},
			{
				Name:    "it returns the node ID for a command that no handler handles",
				Route:   routeToInstance,
				Setup:   func(*testing.T, *sql.DB) {},
				Command: stubs.CommandX1,
			},
			{
				Name:    "it returns the node ID when the aggregate routes the command to an empty instance ID",
				Route:   func(dogma.Command) string { return "" },
				Setup:   func(*testing.T, *sql.DB) {},
				Command: stubs.CommandA1,
			},
			{
				Name:    "it returns the node ID when routing the command panics",
				Route:   func(dogma.Command) string { panic("<panic>") },
				Setup:   func(*testing.T, *sql.DB) {},
				Command: stubs.CommandA1,
			},
		}

		for _, c := range cases {
			t.Run(c.Name, func(t *testing.T) {
				t.Parallel()

				db, router := setup(t, c.Route)
				c.Setup(t, db)

				got, err := router.RouteCommandToQueue(t.Context(), db, packer.PackCommand(c.Command))
				if err != nil {
					t.Fatal(err)
				}

				if got != router.NodeID {
					t.Fatalf("unexpected queue ID: got %s, want %s", got, router.NodeID)
				}
			})
		}
	})

	t.Run("SerialQueueIDs", func(t *testing.T) {
		t.Run("it contains the queue for the commands of an integration that minimizes concurrency", func(t *testing.T) {
			t.Parallel()

			db, router := setup(t, routeToInstance)

			queueID, err := router.RouteCommandToQueue(t.Context(), db, packer.PackCommand(stubs.CommandB1))
			if err != nil {
				t.Fatal(err)
			}

			if !router.SerialQueueIDs().Has(queueID) {
				t.Fatalf("expected %s to be a serial queue", queueID)
			}
		})

		t.Run("it does not contain the queue for the commands of an integration that maximizes concurrency", func(t *testing.T) {
			t.Parallel()

			db, router := setup(t, routeToInstance)

			queueID, err := router.RouteCommandToQueue(t.Context(), db, packer.PackCommand(stubs.CommandC1))
			if err != nil {
				t.Fatal(err)
			}

			if router.SerialQueueIDs().Has(queueID) {
				t.Fatalf("expected %s not to be a serial queue", queueID)
			}
		})
	})

	t.Run("RouteDeadlinesToQueue", func(t *testing.T) {
		t.Run("it returns the node ID", func(t *testing.T) {
			t.Parallel()

			db, router := setup(t, routeToInstance)

			got, err := router.RouteDeadlinesToQueue(t.Context(), db, uuid.New(), "<instance>")
			if err != nil {
				t.Fatal(err)
			}

			if got != router.NodeID {
				t.Fatalf("unexpected queue ID: got %s, want %s", got, router.NodeID)
			}
		})
	})
}
