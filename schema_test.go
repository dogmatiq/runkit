package runkit_test

import (
	"errors"
	"testing"

	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestApplySchemaOnline verifies how ApplySchemaOnline creates and upgrades the
// engine's database schema.
//
// Some cases change [schema.TargetVersion], so these tests must not call
// t.Parallel().
func TestApplySchemaOnline(t *testing.T) {
	t.Run("it creates the schema on a database that doesn't have one yet", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)

		if err := runkit.ApplySchemaOnline(t.Context(), db); err != nil {
			t.Fatal(err)
		}

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

		if err := runkit.ApplySchemaOnline(t.Context(), db); err != nil {
			t.Fatal(err)
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

	t.Run("it returns an error and changes nothing if the schema is behind in its major version", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)
		previous := schema.TargetVersion.String()
		raiseTargetVersion(t, 0)

		err := runkit.ApplySchemaOnline(t.Context(), db)
		if _, ok := errors.AsType[schema.OfflineUpgradeRequiredError](err); !ok {
			t.Fatalf("unexpected error: %v", err)
		}

		xtesting.ExpectQueryResult(
			t,
			"the schema version is unchanged",
			previous,
			db,
			`SELECT version
			FROM runkit.schema_version`,
		)
	})
}

// TestApplySchemaOffline verifies that ApplySchemaOffline applies the upgrades
// that ApplySchemaOnline refuses.
//
// It changes [schema.TargetVersion], so it must not call t.Parallel().
func TestApplySchemaOffline(t *testing.T) {
	t.Run("it upgrades a schema that is behind in its major version", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)
		raiseTargetVersion(t, 0)

		if err := runkit.ApplySchemaOffline(t.Context(), db); err != nil {
			t.Fatal(err)
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

func TestDropSchema(t *testing.T) {
	t.Run("it drops the engine's schema", func(t *testing.T) {
		db := xtesting.NewDatabaseAsOwner(t)

		if err := runkit.DropSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"the database has no schema named runkit",
			false,
			db,
			`SELECT EXISTS (
				SELECT
				FROM pg_namespace
				WHERE nspname = 'runkit'
			)`,
		)
	})
}
