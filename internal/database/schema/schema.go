package schema

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schemaSQL string

// Create applies the PostgreSQL schema to the given database.
func Create(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("unable to create schema: %w", err)
	}

	return nil
}

// Drop removes the engine's schema from the given database.
func Drop(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS dogma CASCADE`); err != nil {
		return fmt.Errorf("unable to drop schema: %w", err)
	}

	return nil
}
