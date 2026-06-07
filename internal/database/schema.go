package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schemaSQL string

// CreateSchema applies the PostgreSQL schema to the given database.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Lock the schema to prevent concurrent creation attempts from interfering
	// with each other. This is necessary because CREATE SCHEMA IF NOT EXISTS
	// suffers a long-standing race condition.
	//
	// https://stackoverflow.com/questions/29900845/create-schema-if-not-exists-raises-duplicate-key-error
	if _, err := tx.ExecContext(
		ctx,
		`LOCK TABLE pg_catalog.pg_namespace IN SHARE ROW EXCLUSIVE MODE`,
	); err != nil {
		return fmt.Errorf("unable to lock schema: %w", err)
	}

	if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("unable to execute DDL: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// DropSchema removes the engine's schema from the given database.
func DropSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS dogma CASCADE`); err != nil {
		return fmt.Errorf("unable to drop schema: %w", err)
	}

	return nil
}
