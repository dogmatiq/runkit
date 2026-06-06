package schema_test

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	. "github.com/dogmatiq/reference-engine/internal/database/schema"
)

func TestCreateAndDrop(t *testing.T) {
	db := databasetest.New(t)

	if err := Create(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	databasetest.Expect(
		t,
		"schema exists",
		1,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata
		WHERE schema_name = 'dogma'`,
	)

	if err := Create(t.Context(), db); err != nil {
		t.Fatalf("schema creation is not idempotent: %s", err)
	}

	if err := Drop(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	databasetest.Expect(
		t,
		"schema does not exist",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata
		WHERE schema_name = 'dogma'`,
	)
}
