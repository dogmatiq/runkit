package schema_test

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	. "github.com/dogmatiq/reference-engine/internal/database/schema"
)

func TestCreate(t *testing.T) {
	db := databasetest.New(t)

	if err := Create(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	databasetest.Expect(
		t,
		"schema exists",
		db,
		`SELECT 1
		FROM information_schema.schemata
		WHERE schema_name = 'dogma'`,
	)
}
