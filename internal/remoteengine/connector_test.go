package remoteengine_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/remoteengine"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConnector(t *testing.T) {
	// setup returns a running composite catalog, and a connection to the given
	// engine.
	setup := func(
		t testing.TB,
		engine *runkit.Engine,
	) (*eventstream.CompositeCatalog, grpc.ClientConnInterface) {
		composite := &eventstream.CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		addr, err := engine.ListenAddr(ctx)
		if err != nil {
			t.Fatal(err)
		}

		conn, err := grpc.NewClient(
			addr.String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })

		return composite, conn
	}

	t.Run("it offers the event streams of each added remote engine", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				composite, conn := setup(t, engine)
				streamID := xtesting.CreateEventStream(t, db)

				connector := &Connector{
					Discover: func(
						ctx context.Context,
						add func(grpc.ClientConnInterface) func(),
					) error {
						add(conn)
						<-ctx.Done()
						return ctx.Err()
					},
					EventStreams: composite,
					Logger:       spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, connector.Run)

				expectStreamIDs(t, composite, streamID)
			},
			app(),
		)
	})

	t.Run("it removes the event streams of a remote engine that is removed", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				composite, conn := setup(t, engine)
				streamID := xtesting.CreateEventStream(t, db)

				remove := make(chan func(), 1)

				connector := &Connector{
					Discover: func(
						ctx context.Context,
						add func(grpc.ClientConnInterface) func(),
					) error {
						remove <- add(conn)
						<-ctx.Done()
						return ctx.Err()
					},
					EventStreams: composite,
					Logger:       spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, connector.Run)

				expectStreamIDs(t, composite, streamID)

				select {
				case <-time.After(xtesting.WaitTimeout):
					t.Fatal("timeout waiting for the remote engine to be added")
				case fn := <-remove:
					fn()
				}

				expectStreamIDs(t, composite)
			},
			app(),
		)
	})

	t.Run("it removes the event streams of every remote engine when the discoverer returns", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				composite, conn := setup(t, engine)
				streamID := xtesting.CreateEventStream(t, db)

				stop := make(chan struct{})

				connector := &Connector{
					Discover: func(
						ctx context.Context,
						add func(grpc.ClientConnInterface) func(),
					) error {
						add(conn)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-stop:
							return nil
						}
					},
					EventStreams: composite,
					Logger:       spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, connector.Run)

				expectStreamIDs(t, composite, streamID)

				close(stop)

				expectStreamIDs(t, composite)
			},
			app(),
		)
	})

	t.Run("it calls the discoverer again if it fails", func(t *testing.T) {
		var calls atomic.Int32

		connector := &Connector{
			Discover: func(
				ctx context.Context,
				_ func(grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					return errors.New("<error>")
				}

				<-ctx.Done()
				return ctx.Err()
			},
			EventStreams: &eventstream.CompositeCatalog{},
			Logger:       spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, connector.Run)

		deadline := time.After(xtesting.WaitTimeout)
		for calls.Load() < 2 {
			select {
			case <-deadline:
				t.Fatal("timeout waiting for the discoverer to be called again")
			case <-time.After(10 * time.Millisecond):
			}
		}
	})
}

// app returns a handler route for an application that records events, so
// that the engine running it serves its event streams.
func app() dogma.HandlerRoute {
	return dogma.ViaIntegration(
		&stubs.IntegrationMessageHandlerStub{
			ConfigureFunc: func(c dogma.IntegrationConfigurer) {
				c.Identity("<handler>", "6a8b5f0e-4c1d-4f2a-9b7e-3d5c8a1f2e40")
				c.Routes(
					dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
					dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
				)
			},
		},
	)
}

// expectStreamIDs waits for the catalog to offer exactly the given streams.
func expectStreamIDs(
	t testing.TB,
	catalog *eventstream.CompositeCatalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidcollection.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := catalog.StreamIDs()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected stream IDs: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}
