package runkit_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEngine_reusesIdleDatabaseConnections(t *testing.T) {
	t.Parallel()
	db := xtesting.NewDatabase(t)
	xtesting.StartCluster(t, db, testApp(), 1, runkit.WithMessageQueueConcurrency(1))

	closedBefore := db.Stats().MaxIdleClosed
	for range 20 {
		var value int
		if err := db.QueryRowContext(t.Context(), "SELECT 1").Scan(&value); err != nil {
			t.Fatal(err)
		}
	}
	if closed := db.Stats().MaxIdleClosed - closedBefore; closed != 0 {
		t.Fatalf("pool closed %d connections that it could have reused", closed)
	}
}

func TestEngine(t *testing.T) {
	t.Run("it does not listen for gRPC requests by default", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- engine.Run(ctx)
		}()

		select {
		case <-engine.Ready():
		case err := <-done:
			t.Fatalf("engine stopped before it was ready: %v", err)
		}

		if addr, err := engine.ListenAddr(ctx); err == nil {
			t.Fatalf("expected an error, got address %s", addr)
		}

		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("it creates the gRPC server with the given options", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithListenAddress(
				"127.0.0.1:0",
				grpc.ChainStreamInterceptor(
					func(
						any,
						grpc.ServerStream,
						*grpc.StreamServerInfo,
						grpc.StreamHandler,
					) error {
						return status.Error(codes.PermissionDenied, "<denied>")
					},
				),
			),
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- engine.Run(ctx)
		}()
		defer func() {
			cancel()
			<-done
		}()

		conn := xtesting.DialGRPC(t, engine)

		stream, err := messaginggrpc.
			NewEventStreamConsumerAPIClient(conn).
			WatchEvents(ctx, &messaginggrpc.WatchEventsRequest{})
		if err == nil {
			_, err = stream.Recv()
		}

		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected the request to be denied by the interceptor, got %v", err)
		}
	})

	t.Run("it panics if the database does not use the pgx driver", func(t *testing.T) {
		db := sql.OpenDB(nonPGXConnector{})
		defer db.Close()

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected a panic")
			}

			if msg, _ := r.(string); !strings.Contains(msg, "pgx") {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()

		runkit.New(testApp(), db)
	})

	t.Run("it panics if the listen address is empty", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithListenAddress("")
	})

	t.Run("it returns an error if it can not listen for gRPC requests", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		// Occupy a port so that the engine can not listen on it.
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer occupied.Close()

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithListenAddress(occupied.Addr().String()),
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		err = engine.Run(ctx)
		if err == nil || errors.Is(err, ctx.Err()) {
			t.Fatalf("expected a listen error, got %v", err)
		}

		var opErr *net.OpError
		if !errors.As(err, &opErr) || opErr.Op != "listen" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("it returns an error if the database has a cluster that runs a different application", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		other := identitypb.New("<other>", uuidpb.Generate())
		if _, err := cluster.Join(
			t.Context(),
			db,
			other,
		); err != nil {
			t.Fatal(err)
		}

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		err := engine.Run(ctx)
		if err == nil || errors.Is(err, ctx.Err()) {
			t.Fatalf("expected an error for a cluster that runs a different application, got %v", err)
		}

		if !strings.Contains(err.Error(), other.GetKey().AsStd().String()) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// nonPGXConnector is a [driver.Connector] for a driver other than pgx.
type nonPGXConnector struct{}

func (nonPGXConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("<not implemented>")
}

func (nonPGXConnector) Driver() driver.Driver {
	return nonPGXDriver{}
}

// nonPGXDriver is a [driver.Driver] other than pgx.
type nonPGXDriver struct{}

func (nonPGXDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("<not implemented>")
}

// testApp returns a minimal application for testing the engine.
func testApp() dogma.Application {
	return &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<app>", "a2b5f0ae-4b1a-4c6e-9d26-0c5a2e5a6b3d")
			c.Routes(
				dogma.ViaIntegration(
					&stubs.IntegrationMessageHandlerStub{
						ConfigureFunc: func(c dogma.IntegrationConfigurer) {
							c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
					},
				),
			)
		},
	}
}

// TestEngine_schema verifies how Run applies the engine's database schema
// before it starts.
//
// Some cases change [schema.TargetVersion], so these tests must not call
// t.Parallel(). Go runs the parallel tests only after every test that doesn't
// call it.
func TestEngine_schema(t *testing.T) {
	t.Run("it creates the schema on a database that doesn't have one yet", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)

		runUntilReady(t, db)

		xtesting.ExpectQueryResult(
			t,
			"the database is at the target schema version",
			schema.TargetVersion.String(),
			db,
			`SELECT version
			FROM runkit.schema_version`,
		)
	})

	t.Run("it upgrades a schema that is behind in its minor version", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)
		raiseTargetVersion(t, 1)

		runUntilReady(t, db)

		xtesting.ExpectQueryResult(
			t,
			"the database is at the new target schema version",
			schema.TargetVersion.String(),
			db,
			`SELECT version
			FROM runkit.schema_version`,
		)
	})

	t.Run("it returns an error if the schema is behind in its major version", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)
		raiseTargetVersion(t, 0)

		err := runUntilStopped(t, db)
		if _, ok := errors.AsType[schema.OfflineUpgradeRequiredError](err); !ok {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("it returns an error if the schema is ahead in its major version", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)

		xtesting.ExecOne(
			t,
			db,
			`UPDATE runkit.schema_version
			SET version = $1`,
			fmt.Sprintf("v%d.0.0", schema.TargetVersion.Major()+1),
		)

		err := runUntilStopped(t, db)
		if _, ok := errors.AsType[schema.IncompatibleSchemaError](err); !ok {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("it starts if an upgrade fails but it can run on the schema as it is", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)
		raiseTargetVersion(t, 2)

		// An invalid index stops the upgrade. A unique index built concurrently
		// over duplicate values fails, and leaves the index invalid.
		if _, err := db.ExecContext(
			t.Context(),
			`CREATE TABLE runkit.duplicates (value int);
			INSERT INTO runkit.duplicates VALUES (1), (1)`,
		); err != nil {
			t.Fatal(err)
		}

		if _, err := db.ExecContext(
			t.Context(),
			`CREATE UNIQUE INDEX CONCURRENTLY duplicates_by_value
			ON runkit.duplicates (value)`,
		); err == nil {
			t.Fatal("expected the index build to fail")
		}

		previous := schema.TargetVersion
		previous[2]--

		runUntilReady(t, db)

		xtesting.ExpectQueryResult(
			t,
			"the database is still at its previous schema version",
			previous.String(),
			db,
			`SELECT version
			FROM runkit.schema_version`,
		)
	})

	t.Run("it tries again after an upgrade step times out waiting for a lock", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)
		raiseTargetVersion(t, 1)

		// Applying the current schema changes this table, which needs a lock
		// that conflicts with the one this transaction holds.
		holder, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Rollback()

		if _, err := holder.ExecContext(
			t.Context(),
			`LOCK TABLE runkit.event_streams IN ACCESS EXCLUSIVE MODE`,
		); err != nil {
			t.Fatal(err)
		}

		engine, done := run(t, db)

		// Hold the lock until the engine has waited for it and given up at
		// least once.
		xtesting.WaitForQueryResult(
			t,
			"the engine waits for the lock",
			true,
			db,
			`SELECT EXISTS (
				SELECT
				FROM pg_stat_activity
				WHERE datname = current_database()
				AND wait_event_type = 'Lock'
			)`,
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		for {
			var waiting bool
			if err := db.QueryRowContext(
				ctx,
				`SELECT EXISTS (
					SELECT
					FROM pg_stat_activity
					WHERE datname = current_database()
					AND wait_event_type = 'Lock'
				)`,
			).Scan(&waiting); err != nil {
				t.Fatal(err)
			}

			if !waiting {
				break
			}

			select {
			case <-ctx.Done():
				t.Fatal("the engine's upgrade step never timed out")
			case <-time.After(50 * time.Millisecond):
			}
		}

		if err := holder.Rollback(); err != nil {
			t.Fatal(err)
		}

		select {
		case <-engine.Ready():
		case err := <-done:
			done <- err
			t.Fatalf("engine stopped before it was ready: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for the engine to be ready")
		}

		xtesting.ExpectQueryResult(
			t,
			"the database is at the new target schema version",
			schema.TargetVersion.String(),
			db,
			`SELECT version
			FROM runkit.schema_version`,
		)
	})
}

// raiseTargetVersion adds one to the given part of [schema.TargetVersion],
// where 0 is the major number, 1 the minor number and 2 the patch number, and
// restores it when the test ends.
func raiseTargetVersion(t *testing.T, part int) {
	t.Helper()

	previous := schema.TargetVersion
	t.Cleanup(func() { schema.TargetVersion = previous })

	schema.TargetVersion[part]++
	for i := part + 1; i < len(schema.TargetVersion); i++ {
		schema.TargetVersion[i] = 0
	}
}

// run runs an engine on the given database until the test ends. It returns
// the engine, and a channel that receives the error that Run returns. A caller
// that receives from the channel before the test ends must send the error back,
// so that the cleanup can wait for Run to return.
func run(t *testing.T, db *sql.DB) (*runkit.Engine, chan error) {
	t.Helper()

	engine := runkit.New(
		testApp(),
		db,
		runkit.WithLogger(spruce.NewTestLogger(t)),
	)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- engine.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	return engine, done
}

// runUntilReady runs an engine on the given database, and fails the test if
// the engine doesn't become ready. The engine runs until the test ends.
func runUntilReady(t *testing.T, db *sql.DB) {
	t.Helper()

	engine, done := run(t, db)

	select {
	case <-engine.Ready():
	case err := <-done:
		done <- err
		t.Fatalf("engine stopped before it was ready: %v", err)
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timed out waiting for the engine to be ready")
	}
}

// runUntilStopped runs an engine on the given database, and returns the error
// that Run returns. It fails the test if the engine becomes ready.
func runUntilStopped(t *testing.T, db *sql.DB) error {
	t.Helper()

	engine, done := run(t, db)

	select {
	case <-engine.Ready():
		t.Fatal("expected the engine to stop, but it became ready")
	case err := <-done:
		done <- err
		return err
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timed out waiting for the engine to stop")
	}

	return nil
}
