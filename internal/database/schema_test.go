package database_test

import (
	"testing"

	. "github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestCreateAndDrop(t *testing.T) {
	db := xtesting.NewDatabase(t)

	if err := CreateSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema exists",
		1,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata
		WHERE schema_name = 'dogma'`,
	)

	if err := CreateSchema(t.Context(), db); err != nil {
		t.Fatalf("schema creation is not idempotent: %s", err)
	}

	if err := DropSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema does not exist",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata
		WHERE schema_name = 'dogma'`,
	)
}
