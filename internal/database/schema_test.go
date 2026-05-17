package database_test

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/database"
)

func TestApplySchema_isIdempotent(t *testing.T) {
	db, _ := database.NewTestDB(t)

	// [NewTestDB] already applied the schema; a second call must be a no-op.
	if err := database.ApplySchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}
