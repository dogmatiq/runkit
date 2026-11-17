package migrations_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	. "github.com/dogmatiq/runkit/internal/schema/internal/migrations"
	"github.com/dogmatiq/runkit/internal/schema/internal/version"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// newRunner returns a runner for the given migrations, which uses a connection
// to a database that has the engine's schema.
func newRunner(
	t *testing.T,
	db *sql.DB,
	offline bool,
	migrations ...Migration,
) *Runner {
	t.Helper()

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return &Runner{
		Conn:             conn,
		Migrations:       migrations,
		Offline:          offline,
		LockTimeout:      5 * time.Second,
		StatementTimeout: 100 * time.Millisecond,
	}
}

func TestRunner(t *testing.T) {
	t.Run("func Run()", func(t *testing.T) {
		t.Run("it applies steps of both kinds in order", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			runner := newRunner(
				t,
				db,
				false,
				Migration{
					Version: version.Version{1, 0, 0},
					Steps: []Step{
						{Name: "create", Kind: Transaction, SQL: `
							CREATE TABLE public.steps (id serial, step int);
							INSERT INTO public.steps (step) VALUES (10)`},
						{Name: "insert", Kind: Statement, SQL: `INSERT INTO public.steps (step) VALUES (20)`},
						// A concurrent index build can't run in a transaction.
						{Name: "index", Kind: Statement, SQL: `CREATE INDEX CONCURRENTLY steps_by_step ON public.steps (step)`},
						{Name: "insert again", Kind: Transaction, SQL: `INSERT INTO public.steps (step) VALUES (40)`},
					},
				},
			)

			if err := runner.Run(t.Context()); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"the steps ran in order",
				"10,20,40",
				db,
				`SELECT string_agg(step::text, ',' ORDER BY id)
				FROM public.steps`,
			)

			xtesting.ExpectQueryResult(
				t,
				"the index is valid",
				true,
				db,
				`SELECT indisvalid
				FROM pg_index
				WHERE indexrelid = 'public.steps_by_step'::regclass`,
			)

			xtesting.ExpectQueryResult(
				t,
				"every step is recorded",
				true,
				db,
				`SELECT migrating_to = 'v1.0.0'
					AND migration_steps_completed = 4
				FROM runkit.schema_version`,
			)
		})

		t.Run("it rejects a statement step with more than one statement", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			runner := newRunner(
				t,
				db,
				false,
				Migration{
					Version: version.Version{1, 0, 0},
					Steps: []Step{
						{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.steps (step int)`},
						{Name: "insert", Kind: Statement, SQL: `INSERT INTO public.steps VALUES (20); INSERT INTO public.steps VALUES (21)`},
					},
				},
			)

			if err := runner.Run(t.Context()); err == nil {
				t.Fatal("expected an error, got nil")
			}

			xtesting.ExpectQueryResult(
				t,
				"neither statement ran",
				0,
				db,
				`SELECT COUNT(*)
				FROM public.steps`,
			)

			xtesting.ExpectQueryResult(
				t,
				"only the earlier step is recorded",
				1,
				db,
				`SELECT migration_steps_completed
				FROM runkit.schema_version`,
			)
		})

		t.Run("it resumes after the last step that completed", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			broken := Migration{
				Version: version.Version{1, 0, 0},
				Steps: []Step{
					// Running this step again would fail, because the table exists.
					{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.steps (step int)`},
					{Name: "insert", Kind: Transaction, SQL: `INSERT INTO public.missing VALUES (20)`},
				},
			}

			if err := newRunner(t, db, false, broken).Run(t.Context()); err == nil {
				t.Fatal("expected an error, got nil")
			}

			xtesting.ExpectQueryResult(
				t,
				"the first step is recorded",
				1,
				db,
				`SELECT migration_steps_completed
				FROM runkit.schema_version`,
			)

			fixed := Migration{
				Version: version.Version{1, 0, 0},
				Steps: []Step{
					{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.steps (step int)`},
					{Name: "insert", Kind: Transaction, SQL: `INSERT INTO public.steps VALUES (20)`},
				},
			}

			if err := newRunner(t, db, false, fixed).Run(t.Context()); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"the second step ran once",
				1,
				db,
				`SELECT COUNT(*)
				FROM public.steps`,
			)

			xtesting.ExpectQueryResult(
				t,
				"both steps are recorded",
				2,
				db,
				`SELECT migration_steps_completed
				FROM runkit.schema_version`,
			)
		})

		t.Run("it resumes in the migration of the last step that completed", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			first := Migration{
				Version: version.Version{1, 0, 0},
				Steps: []Step{
					{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.first (step int)`},
				},
			}

			secondBroken := Migration{
				Version: version.Version{2, 0, 0},
				Steps: []Step{
					{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.second (step int) WITH (no_such_option = 1)`},
				},
			}

			if err := newRunner(t, db, false, first, secondBroken).Run(t.Context()); err == nil {
				t.Fatal("expected an error, got nil")
			}

			secondFixed := Migration{
				Version: version.Version{2, 0, 0},
				Steps: []Step{
					{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.second (step int)`},
				},
			}

			if err := newRunner(t, db, false, first, secondFixed).Run(t.Context()); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"both migrations ran",
				2,
				db,
				`SELECT COUNT(*)
				FROM pg_class
				WHERE relnamespace = 'public'::regnamespace
				AND relname IN ('first', 'second')`,
			)

			xtesting.ExpectQueryResult(
				t,
				"the second migration's step is recorded",
				true,
				db,
				`SELECT migrating_to = 'v2.0.0'
					AND migration_steps_completed = 1
				FROM runkit.schema_version`,
			)
		})

		t.Run("it fails if the last step that completed belongs to an unknown migration", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			if _, err := db.ExecContext(
				t.Context(),
				`UPDATE runkit.schema_version
				SET migrating_to = 'v9.0.0',
					migration_steps_completed = 1`,
			); err != nil {
				t.Fatal(err)
			}

			runner := newRunner(
				t,
				db,
				false,
				Migration{
					Version: version.Version{1, 0, 0},
					Steps: []Step{
						{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.steps (step int)`},
					},
				},
			)

			if err := runner.Run(t.Context()); err == nil {
				t.Fatal("expected an error, got nil")
			}

			xtesting.ExpectQueryResult(
				t,
				"no step ran",
				true,
				db,
				`SELECT to_regclass('public.steps') IS NULL`,
			)
		})

		t.Run("it fails if more steps completed than the migration has", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			if _, err := db.ExecContext(
				t.Context(),
				`UPDATE runkit.schema_version
				SET migrating_to = 'v1.0.0',
					migration_steps_completed = 2`,
			); err != nil {
				t.Fatal(err)
			}

			runner := newRunner(
				t,
				db,
				false,
				Migration{
					Version: version.Version{1, 0, 0},
					Steps: []Step{
						{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.steps (step int)`},
					},
				},
				Migration{
					Version: version.Version{2, 0, 0},
					Steps: []Step{
						{Name: "create", Kind: Transaction, SQL: `CREATE TABLE public.later (step int)`},
					},
				},
			)

			if err := runner.Run(t.Context()); err == nil {
				t.Fatal("expected an error, got nil")
			}

			xtesting.ExpectQueryResult(
				t,
				"no step ran",
				true,
				db,
				`SELECT to_regclass('public.later') IS NULL`,
			)
		})

		t.Run("it times out a transaction step unless offline", func(t *testing.T) {
			slow := Migration{
				Version: version.Version{1, 0, 0},
				Steps: []Step{
					{Name: "sleep", Kind: Transaction, SQL: `SELECT pg_sleep(0.5)`},
				},
			}

			db := xtesting.NewDatabase(t)

			err := newRunner(t, db, false, slow).Run(t.Context())
			if err == nil {
				t.Fatal("expected an error, got nil")
			}

			if !strings.Contains(err.Error(), "statement timeout") {
				t.Fatalf("unexpected error: %s", err)
			}

			if err := newRunner(t, db, true, slow).Run(t.Context()); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("it doesn't time out a statement step", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			runner := newRunner(
				t,
				db,
				false,
				Migration{
					Version: version.Version{1, 0, 0},
					Steps: []Step{
						{Name: "sleep", Kind: Statement, SQL: `SELECT pg_sleep(0.5)`},
					},
				},
			)

			if err := runner.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("func Run() with a transaction timeout", func(t *testing.T) {
		// setTransactionTimeout sets a transaction timeout on the runner's
		// connection, as a default of the database or the role would.
		setTransactionTimeout := func(t *testing.T, runner *Runner) {
			t.Helper()

			if _, err := runner.Conn.ExecContext(
				t.Context(),
				`SET transaction_timeout = '100ms'`,
			); err != nil {
				t.Fatal(err)
			}
		}

		t.Run("it doesn't apply the transaction timeout to a transaction step when offline", func(t *testing.T) {
			db := xtesting.NewDatabase(t)

			runner := newRunner(
				t,
				db,
				true,
				Migration{
					Version: version.Version{1, 0, 0},
					Steps: []Step{
						{Name: "sleep", Kind: Transaction, SQL: `SELECT pg_sleep(0.5)`},
					},
				},
			)
			setTransactionTimeout(t, runner)

			if err := runner.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("it doesn't apply the transaction timeout to a statement step", func(t *testing.T) {
			for _, offline := range []bool{false, true} {
				db := xtesting.NewDatabase(t)

				runner := newRunner(
					t,
					db,
					offline,
					Migration{
						Version: version.Version{1, 0, 0},
						Steps: []Step{
							{Name: "sleep", Kind: Statement, SQL: `SELECT pg_sleep(0.5)`},
						},
					},
				)
				setTransactionTimeout(t, runner)

				if err := runner.Run(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
	})

	t.Run("func Transact()", func(t *testing.T) {
		t.Run("it commits the transaction if the function succeeds", func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			runner := newRunner(t, db, false)

			if err := runner.Transact(
				t.Context(),
				func(ctx context.Context, tx *sql.Tx) error {
					return xsql.ExecOne(
						ctx,
						tx,
						`UPDATE runkit.schema_version
						SET migrating_to = 'v1.0.0',
							migration_steps_completed = 1`,
					)
				},
			); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"the change is committed",
				1,
				db,
				`SELECT migration_steps_completed
				FROM runkit.schema_version`,
			)
		})
	})
}
