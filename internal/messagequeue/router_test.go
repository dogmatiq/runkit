package messagequeue_test

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestRouter(t *testing.T) {
	aggregateKey := uuidpb.Generate()

	// setup returns a router for a node with an aggregate that handles commands
	// of type A, and routes each one to the instance that route returns.
	setup := func(t *testing.T, route func(dogma.Command) string) (*sql.DB, *Router) {
		db := xtesting.NewDatabase(t)

		aggregates := &uuidcollection.Map[AggregateRoute]{}

		mt, _ := dogma.RegisteredMessageTypeOf(stubs.CommandA1)
		aggregates.Set(
			uuidpb.MustParse(mt.ID()),
			AggregateRoute{
				Key: aggregateKey,
				Handler: &stubs.AggregateMessageHandlerStub[dogma.AggregateRoot]{
					RouteCommandToInstanceFunc: route,
				},
			},
		)

		router := &Router{
			NodeID:     uuidpb.Generate(),
			Aggregates: aggregates,
		}

		return db, router
	}

	// insertInstance inserts an aggregate instance for the handler with the
	// given key. If bound is true, it binds the instance to a new event stream
	// and returns the stream's ID.
	insertInstance := func(
		t *testing.T,
		db *sql.DB,
		handlerKey *uuidpb.UUID,
		instanceID string,
		bound bool,
	) *uuidpb.UUID {
		t.Helper()

		var streamID *uuidpb.UUID

		if bound {
			streamID = &uuidpb.UUID{}

			if err := db.QueryRowContext(
				t.Context(),
				`SELECT eventstream.acquire_for_write()`,
			).Scan(xsql.UUID(streamID)); err != nil {
				t.Fatal(err)
			}
		}

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO aggregate.instances (
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

			if !got.Equal(streamID) {
				t.Fatalf("unexpected queue ID: got %s, want %s", got, streamID)
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
				Setup:   func(t *testing.T, db *sql.DB) { insertInstance(t, db, uuidpb.Generate(), "<instance>", true) },
				Command: stubs.CommandA1,
			},
			{
				Name:    "it returns the node ID for a command that no aggregate handles",
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

				if !got.Equal(router.NodeID) {
					t.Fatalf("unexpected queue ID: got %s, want %s", got, router.NodeID)
				}
			})
		}
	})

	t.Run("RouteDeadlinesToQueue", func(t *testing.T) {
		t.Run("it returns the node ID", func(t *testing.T) {
			t.Parallel()

			db, router := setup(t, routeToInstance)

			got, err := router.RouteDeadlinesToQueue(t.Context(), db, uuidpb.Generate(), "<instance>")
			if err != nil {
				t.Fatal(err)
			}

			if !got.Equal(router.NodeID) {
				t.Fatalf("unexpected queue ID: got %s, want %s", got, router.NodeID)
			}
		})
	})
}
