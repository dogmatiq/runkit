package database_test

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
)

func TestApplySchema_isIdempotent(t *testing.T) {
	db, _ := databasetest.New(t)

	// [databasetest.New] already applied the schema; an additional application
	// must be a no-op.
	if err := database.ApplySchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}
