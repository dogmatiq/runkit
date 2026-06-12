package schema_test

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/schema"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestCreateAndDrop(t *testing.T) {
	db := xtesting.NewDatabaseWithoutSchema(t)

	var baseline int
	if err := db.QueryRow(
		`SELECT COUNT(*)
		FROM information_schema.schemata`,
	).Scan(&baseline); err != nil {
		t.Fatal(err)
	}

	if err := schema.Create(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema count increased after Create()",
		true,
		db,
		`SELECT COUNT(*) > $1
		FROM information_schema.schemata`,
		baseline,
	)

	xtesting.ExpectQueryResult(
		t,
		"no tables created in public schema",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"no functions created in public schema",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.routines
		WHERE routine_schema = 'public'`,
	)

	// Create() again to verify idempotency.
	if err := schema.Create(t.Context(), db); err != nil {
		t.Fatalf("schema creation is not idempotent: %s", err)
	}

	if err := schema.Drop(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema count restored after Drop()",
		baseline,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata`,
	)
}
